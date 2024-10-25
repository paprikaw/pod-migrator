package migrator

import (
	"context"
	"fmt"
	"strings"
	"time"

	de "github.com/paprikaw/rscheduler/pkg/dataexporter"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/api/policy/v1beta1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
	throttles "k8s.io/client-go/util/flowcontrol"
	retry "k8s.io/client-go/util/retry"
	"k8s.io/klog/v2"
)

// go run . -v 2 --mode=nodefailed --output=nodefailed.csv >> RL_Migrator.log 2>&1
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

func (m *Migrator) GetK8sClient() *kubernetes.Clientset {
	return m.clientset
}

func (m *Migrator) waitForPodAnnotationReady(ctx context.Context, pollingPeriod time.Duration, namespace string, podName string, annotationKey string, expectedValue string, maxRetry int) error {
	logger := klog.FromContext(ctx)
	logger.V(2).Info("等待 Pod 的 annotation 更新", "podName", podName, "annotationKey", annotationKey, "expectedValue", expectedValue)
	// 定义轮询机制，等待 Pod annotation 更新
	for i := 0; i < maxRetry; i++ {
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

		time.Sleep(pollingPeriod) // 等待一段时间后重试
	}
	return fmt.Errorf("等待Pod的annotation更新失败")
}

func (m *Migrator) EvictAllPods(ctx context.Context, namespace string, appLabel string, targetNode string) error {
	logger := klog.FromContext(ctx)
	logger.V(1).Info("驱逐 Deployment 下的所有 Pod", "appLabel", appLabel)
	// 得到所有的pod
	podList, err := m.clientset.CoreV1().Pods(namespace).List(ctx, metav1.ListOptions{
		FieldSelector: fmt.Sprintf("spec.nodeName=%s", targetNode),
		LabelSelector: appLabel,
	})
	if err != nil {
		logger.Error(err, "无法获取 Pods 列表")
		return err
	}
	for _, pod := range podList.Items {
		eviction := &v1beta1.Eviction{
			ObjectMeta: metav1.ObjectMeta{
				Name:      pod.Name,
				Namespace: namespace,
			},
		}

		// 调用驱逐 API
		err := m.clientset.CoreV1().Pods(namespace).Evict(ctx, eviction)
		if err != nil {
			logger.Error(err, "驱逐 Pod 失败")
			return err
		}
	}

	logger.V(1).Info("成功驱逐 Deployment 下的所有 Pod", "appLabel", appLabel)
	return nil
}

// waitForPodReady 等待新的 Pod 处于 Running 状态
func (m *Migrator) waitForPodReady(ctx context.Context, pollingPeriod time.Duration, namespace string, deployment string, oldPods map[string]corev1.Pod, maxRetry int) error {
	logger := klog.FromContext(ctx)
	logger.V(1).Info("等待新的 Pod 处于 Running 状态", "appLabel", deployment)

	// 直接for循环请求，遍历podList查看是否处于running状态
	for i := 0; i < maxRetry; i++ {
		podList, err := m.clientset.CoreV1().Pods(namespace).List(ctx, metav1.ListOptions{
			LabelSelector: fmt.Sprintf("app=%s", deployment),
		})
		if err != nil {
			logger.Error(err, "无法获取 Pods 列表")
			return err
		}

		for _, pod := range podList.Items {
			// 检查 Pod 是否处于 Running 且 Ready 状态
			if _, ok := oldPods[pod.Name]; !ok && pod.Status.Phase == corev1.PodRunning {
				allReady := true
				for _, condition := range pod.Status.Conditions {
					if condition.Type == corev1.PodReady && condition.Status != corev1.ConditionTrue {
						allReady = false
						break
					}
				}
				if allReady {
					logger.V(1).Info("新的 Pod 已经处于 Running 且 Ready 状态", "podName", pod.Name)
					return nil
				}
			}
		}
		time.Sleep(pollingPeriod)
	}
	return fmt.Errorf("等待新的Pod处于Running状态失败")
}

func (m *Migrator) MigratePod(ctx context.Context, pollingPeriod time.Duration, namespace string, podName string, appLabel string, nodeName string, dataExporter *de.DataExporter, curLatency float64, isTS bool) (err error) {
	var backoff = wait.Backoff{
		Steps:    100,
		Duration: 50 * time.Millisecond,
		Factor:   1.0,
		Jitter:   0.1,
	}
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
	currentNode := podMap[podName].Spec.NodeName
	if _, ok := podMap[podName]; !ok {
		return fmt.Errorf("target Pod does not exist: %s", podName)
	}
	if isTS {
		dataExporter.WriteTS(time.Now().Unix(), float64(curLatency), currentNode, nodeName, podName, "", de.RESCHEDULING)
	}
	// Step 2: 设置节点不可调度
	node_cnt := 0
	err = retry.RetryOnConflict(backoff, func() error {
		nodes, err := m.clientset.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
		node_cnt = len(nodes.Items)
		if err != nil {
			return fmt.Errorf("failed to get node list: %v", err)
		}
		currentNode := podMap[podName].Spec.NodeName
		logger.V(1).Info("Migration...", "podName", podName, "currentNode", currentNode, "targetNode", nodeName)
		logger.V(2).Info("开始设置节点为不可调度", "targetNode", nodeName)
		// Iterate through all nodes, cordon all except the target node
		for _, node := range nodes.Items {
			if node.Name != nodeName {
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

	okhere := false
	for i := 0; i < backoff.Steps; i++ {
		ok, err := m.CheckCordonNode(ctx, node_cnt-1)
		okhere = ok
		if err != nil {
			logger.Error(err, "检查uncordon的节点数量失败")
			return err
		}
		if ok {
			break
		}
		logger.V(1).Info("检查cordon nodes状态失败，等待下一轮检查")
		time.Sleep(pollingPeriod)
	}
	if !okhere {
		logger.Error(err, "cordon nodes状态未达到预期")
		return fmt.Errorf("cordon nodes状态未达到预期")
	}
	logger.V(2).Info("cordon nodes状态达到预期")

	var originalReplicas int32 = 0
	// Step 3: 增加 replicas 数量
	err = retry.RetryOnConflict(backoff, func() error {
		logger.V(3).Info("修改replicaset", "podName", podName)
		deployment, err := m.clientset.AppsV1().Deployments(namespace).Get(ctx, deploymentName, metav1.GetOptions{})
		originalReplicas = *deployment.Spec.Replicas
		if err != nil {
			logger.Error(err, "获取 Deployment 失败")
			return err
		}
		newReplicas := originalReplicas + 1
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
	err = m.waitForPodReady(ctx, pollingPeriod, namespace, deploymentName, podMap, backoff.Steps)
	if err != nil {
		logger.Error(err, "等待新的pod处于running状态失败")
		return err
	}
	// Step 4: 更改targetPod的优先级
	logger.V(2).Info("更改Pod优先级:更新pod")
	err = retry.RetryOnConflict(backoff, func() error {
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
	err = m.waitForPodAnnotationReady(ctx, pollingPeriod, namespace, podName, "controller.kubernetes.io/pod-deletion-cost", "0", backoff.Steps)
	if err != nil {
		logger.Error(err, "等待annotation更新失败")
		return err
	}

	// Step 5: 将replica set改回去
	err = retry.RetryOnConflict(backoff, func() error {
		deployment, err := m.clientset.AppsV1().Deployments(namespace).Get(ctx, deploymentName, metav1.GetOptions{})
		if err != nil {
			logger.Error(err, "获取deployment失败")
			return err
		}
		deployment.Spec.Replicas = &originalReplicas
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
	// 5: 新的pod在目标节点上
	for i := 0; i < backoff.Steps; i++ {
		time.Sleep(pollingPeriod)
		podList, err := m.clientset.CoreV1().Pods(namespace).List(ctx, metav1.ListOptions{
			LabelSelector: fmt.Sprintf("app=%s", deploymentName),
		})
		if err != nil {
			logger.Error(err, "无法获取 Pods 列表")
			return err
		}
		if len(podList.Items) != int(originalReplicas) {
			logger.V(2).Info("新的replicaSet数量不等于预期", "expected", originalReplicas, "actual", len(podList.Items))
			continue
		}
		for _, pod := range podList.Items {
			if pod.Name == podName {
				logger.V(2).Info("旧的pod仍然存在", "podName", pod.Name)
				continue
			}
		}
		isNewPodRunning := false
		for _, pod := range podList.Items {
			// 检查 Pod 是否处于 Running 且 Ready 状态
			if _, ok := podMap[pod.Name]; !ok && pod.Status.Phase == corev1.PodRunning {
				allReady := true
				if pod.Spec.NodeName != nodeName {
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
					logger.V(2).Info("新的 Pod 已经处于 Running 且 Ready 状态", "podName", pod.Name, "node", pod.Spec.NodeName)
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
	err = retry.RetryOnConflict(backoff, func() error {
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
	okhere = false
	for i := 0; i < backoff.Steps; i++ {
		ok, err := m.CheckCordonNode(ctx, 0)
		okhere = ok
		if err != nil {
			logger.Error(err, "检查uncordon的节点数量失败")
			return err
		}
		if ok {
			break
		}
		time.Sleep(pollingPeriod)
	}
	if !okhere {
		return fmt.Errorf("恢复节点为可调度失败")
	}
	logger.V(2).Info("恢复节点为可调度成功")
	// Restore all nodes to schedulable state
	return nil
}

func (m *Migrator) CheckCordonNode(ctx context.Context, cordonNodeCnt int) (bool, error) {
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
