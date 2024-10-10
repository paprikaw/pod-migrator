package migrator

import (
	"context"
	"fmt"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/api/policy/v1beta1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
	throttles "k8s.io/client-go/util/flowcontrol"
	retry "k8s.io/client-go/util/retry"
	"k8s.io/klog/v2"
)

type Migrator struct {
	clientset *kubernetes.Clientset
}

func NewMigrator(kubeconfig string) (*Migrator, error) {

	config, err := clientcmd.BuildConfigFromFlags("", kubeconfig)
	if err != nil {
		return nil, fmt.Errorf("failed to load kubeconfig: %v", err)
	}
	config.RateLimiter = throttles.NewTokenBucketRateLimiter(100, 300)
	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		return nil, fmt.Errorf("failed to create kubernetes client: %v", err)
	}
	return &Migrator{
		clientset: clientset,
	}, nil
}
func (m *Migrator) waitForPodAnnotationReady(ctx context.Context, namespace string, podName string, annotationKey string, expectedValue string) error {
	logger := klog.FromContext(ctx)
	logger.V(2).Info("等待 Pod 的 annotation 更新", "podName", podName, "annotationKey", annotationKey, "expectedValue", expectedValue)

	// 定义轮询机制，等待 Pod annotation 更新
	for {
		pod, err := m.clientset.CoreV1().Pods(namespace).Get(ctx, podName, metav1.GetOptions{})
		if err != nil {
			logger.Error(err, "无法获取 Pod")
			return err
		}

		// 检查 Pod 的 annotation 是否更新为期望的值
		if value, exists := pod.Annotations[annotationKey]; exists && value == expectedValue {
			logger.V(1).Info("Pod 的 annotation 已更新为期望的值", "podName", pod.Name, "annotationKey", annotationKey, "value", value)
			return nil
		}

		time.Sleep(50 * time.Millisecond) // 等待一段时间后重试
	}
}

// evictPod 驱逐指定 Pod
func (m *Migrator) evictPod(ctx context.Context, namespace string, podName string) error {
	logger := klog.FromContext(ctx)
	logger.V(1).Info("驱逐 Pod", "podName", podName)

	eviction := &v1beta1.Eviction{
		ObjectMeta: metav1.ObjectMeta{
			Name:      podName,
			Namespace: namespace,
		},
	}

	// 调用驱逐 API
	err := m.clientset.CoreV1().Pods(namespace).Evict(ctx, eviction)
	if err != nil {
		logger.Error(err, "驱逐 Pod 失败")
		return err
	}

	logger.V(1).Info("成功驱逐 Pod", "podName", podName)
	return nil
}

func (m *Migrator) MigratePod(ctx context.Context, namespace string, podName string, appLabel string, targetNode string, replicaCnt int32) error {
	deploymentName := podName[:strings.LastIndex(podName[:strings.LastIndex(podName, "-")], "-")]
	logger := klog.FromContext(ctx)
	// Get all pods of the same deployment
	pods, err := m.clientset.CoreV1().Pods(namespace).List(ctx, metav1.ListOptions{
		LabelSelector: appLabel,
	})
	if err != nil {
		return fmt.Errorf("failed to get Pod list: %v", err)
	}
	podMap := make(map[string]corev1.Pod)
	// Check if the target pod is in the list
	for _, pod := range pods.Items {
		podMap[pod.Name] = pod
	}

	if _, ok := podMap[podName]; !ok {
		return fmt.Errorf("target Pod does not exist: %s", podName)
	}
	// Step 2: 设置节点不可调度
	node_cnt := 0
	err = retry.RetryOnConflict(retry.DefaultRetry, func() error {
		nodes, err := m.clientset.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
		node_cnt = len(nodes.Items)
		if err != nil {
			return fmt.Errorf("failed to get node list: %v", err)
		}
		currentNode := podMap[podName].Spec.NodeName
		logger.V(0).Info("Migration...", "podName", podName, "currentNode", currentNode, "targetNode", targetNode)
		logger.V(2).Info("开始设置节点为不可调度", "targetNode", targetNode)
		// Iterate through all nodes, cordon all except the target node
		for _, node := range nodes.Items {
			if node.Name != targetNode {
				node.Spec.Unschedulable = true
				_, err := m.clientset.CoreV1().Nodes().Update(ctx, &node, metav1.UpdateOptions{})
				if err != nil {
					logger.Error(err, "failed to set node as unschedulable", "node", node.Name)
					return err
				} else {
					logger.V(3).Info("successfully set node as unschedulable", "node", node.Name)
				}
			}
		}
		return nil
	})
	if err != nil {
		logger.Error(err, "设置节点为不可调度失败")
		return err
	}
	logger.V(2).Info("检查cordon nodes状态")
	for {
		ok, err := m.checkCordonNode(ctx, node_cnt-1)
		if err != nil {
			logger.Error(err, "检查uncordon的节点数量失败")
			return err
		}
		if ok {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	logger.V(2).Info("cordon nodes状态达到预期")

	// Step 3: 增加 replicas 数量
	err = retry.RetryOnConflict(retry.DefaultRetry, func() error {
		logger.V(3).Info("修改replicaset", "podName", podName)
		deployment, err := m.clientset.AppsV1().Deployments(namespace).Get(ctx, deploymentName, metav1.GetOptions{})
		if err != nil {
			logger.Error(err, "获取 Deployment 失败")
			return err
		}
		originalReplicas := *deployment.Spec.Replicas
		if originalReplicas != replicaCnt {
			err = fmt.Errorf("原始 replicas 数量 %d 不等于预期 replicas 数量 %d", originalReplicas, replicaCnt)
			logger.Error(err, "原始 replicas 数量不等于预期 replicas 数量")
			return err
		}
		newReplicas := replicaCnt + 1
		deployment.Spec.Replicas = &newReplicas
		logger.V(3).Info("修改replica", "newReplicas", newReplicas)
		_, err = m.clientset.AppsV1().Deployments(namespace).Update(ctx, deployment, metav1.UpdateOptions{})
		if err != nil {
			logger.Error(err, "增加replica失败")
			return err
		}
		return nil
	})
	if err != nil {
		logger.Error(err, "增加replica失败")
		return err
	}
	// Wait for new replica to be at running state
	// Step 4: 更改targetPod的优先级
	logger.V(2).Info("更改Pod优先级:更新pod")
	err = retry.RetryOnConflict(retry.DefaultRetry, func() error {
		pod, err := m.clientset.CoreV1().Pods(namespace).Get(ctx, podName, metav1.GetOptions{})
		if err != nil {
			logger.Error(err, "更改Pod优先级:获取pod失败")
			return err
		}
		if pod.Annotations == nil {
			pod.Annotations = make(map[string]string)
		}
		pod.Annotations["controller.kubernetes.io/pod-deletion-cost"] = "0"
		_, err = m.clientset.CoreV1().Pods(namespace).Update(ctx, pod, metav1.UpdateOptions{})
		return err
	})
	if err != nil {
		logger.Error(err, "更改Pod优先级:更新pod失败")
		return err
	}
	logger.V(2).Info("更改Pod优先级:更新pod成功")
	err = m.waitForPodAnnotationReady(ctx, namespace, podName, "controller.kubernetes.io/pod-deletion-cost", "0")
	if err != nil {
		logger.Error(err, "等待annotation更新失败")
		return err
	}

	// Step 5: 将replica set改回去
	err = retry.RetryOnConflict(retry.DefaultRetry, func() error {
		deployment, err := m.clientset.AppsV1().Deployments(namespace).Get(ctx, deploymentName, metav1.GetOptions{})
		if err != nil {
			logger.Error(err, "获取deployment失败")
			return err
		}
		deployment.Spec.Replicas = &replicaCnt
		_, err = m.clientset.AppsV1().Deployments(namespace).Update(ctx, deployment, metav1.UpdateOptions{})
		if err != nil {
			logger.Error(err, "减少replica失败")
			return err
		}
		return err
	})
	if err != nil {
		logger.Error(err, "减少replica失败")
		return err
	}
	// Final Check: 检查新的replicaset的状况
	// 1: 新的replicaSet 数量和原来一样
	// 2: 新的replicaSet中的pod处于正常状态
	// 3: targetPod不在replicaSet中
	// 4: 出现一个新的pod
	for {
		time.Sleep(1 * time.Second)
		podList, err := m.clientset.CoreV1().Pods(namespace).List(ctx, metav1.ListOptions{
			LabelSelector: fmt.Sprintf("app=%s", deploymentName),
		})
		if err != nil {
			logger.Error(err, "无法获取 Pods 列表")
			return err
		}
		if len(podList.Items) != int(replicaCnt) {
			err = fmt.Errorf("新的replicaSet数量不等于预期, 预期数量: %d, 实际数量: %d", replicaCnt, len(podList.Items))
			logger.Error(err, "新的replicaSet数量不等于预期")
			continue
		}
		if _, ok := podMap[podName]; ok {
			err = fmt.Errorf("旧的pod %s 仍然存在", podName)
			logger.Error(err, "旧的pod仍然存在")
			continue
		}
		isNewPodRunning := false
		for _, pod := range podList.Items {
			// 检查 Pod 是否处于 Running 且 Ready 状态
			if _, ok := podMap[pod.Name]; !ok && pod.Status.Phase == corev1.PodRunning {
				allReady := true
				if pod.Spec.NodeName != targetNode {
					err = fmt.Errorf("新的pod %s 不在目标节点上", pod.Name)
					logger.Error(err, "新的pod不在目标节点上")
					return err
				}
				for _, condition := range pod.Status.Conditions {
					if condition.Type == corev1.PodReady && condition.Status != corev1.ConditionTrue {
						allReady = false
						break
					}
				}
				if allReady {
					logger.V(2).Info("新的 Pod 已经处于 Running 且 Ready 状态", "podName", pod.Name)
					isNewPodRunning = true
					break
				}
			}
		}
		if isNewPodRunning {
			break
		}
	}
	// Step 6: 恢复节点为可调度
	logger.V(2).Info("开始恢复节点为可调度")
	err = retry.RetryOnConflict(retry.DefaultRetry, func() error {
		nodes, err := m.clientset.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
		if err != nil {
			return fmt.Errorf("failed to get node list: %v", err)
		}
		for _, node := range nodes.Items {
			if node.Spec.Unschedulable {
				logger.V(3).Info("restoring node to schedulable state", "node", node.Name)
				node.Spec.Unschedulable = false
				_, err := m.clientset.CoreV1().Nodes().Update(ctx, &node, metav1.UpdateOptions{})
				if err != nil {
					return fmt.Errorf("failed to restore node to schedulable state: %v", err)
				} else {
					logger.V(3).Info("successfully restored node to schedulable state", "node", node.Name)
				}
			}
		}
		return nil
	})
	if err != nil {
		logger.Error(err, "恢复节点为可调度失败")
		return err
	}
	logger.V(2).Info("检查uncordon nodes状态")
	for {
		ok, err := m.checkCordonNode(ctx, 0)
		if err != nil {
			logger.Error(err, "检查uncordon的节点数量失败")
			return err
		}
		if ok {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	logger.V(2).Info("恢复节点为可调度成功")
	// Restore all nodes to schedulable state
	return nil
}

func (m *Migrator) checkCordonNode(ctx context.Context, cordonNodeCnt int) (bool, error) {
	logger := klog.FromContext(ctx)
	logger.V(2).Info("检查uncordon的节点数量是否符合预期", "cordonNodeCnt", cordonNodeCnt)

	nodes, err := m.clientset.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
	if err != nil {
		return false, fmt.Errorf("failed to get node list: %v", err)
	}

	cordonNodes := 0
	for _, node := range nodes.Items {
		if node.Spec.Unschedulable {
			cordonNodes++
		}
	}
	if cordonNodes != cordonNodeCnt {
		return false, nil
	}
	return true, nil
}
