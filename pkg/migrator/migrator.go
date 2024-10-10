package migrator

import (
	"context"
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/api/policy/v1beta1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
	throttles "k8s.io/client-go/util/flowcontrol"
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
	watch, err := m.clientset.CoreV1().Pods(namespace).Watch(ctx, metav1.ListOptions{
		FieldSelector: fmt.Sprintf("metadata.name=%s", podName),
	})
	if err != nil {
		logger.Error(err, "无法监视 Pod")
		return err
	}
	defer watch.Stop()

	for event := range watch.ResultChan() {
		pod, ok := event.Object.(*corev1.Pod)
		if !ok {
			continue
		}

		// 检查 Pod 的 annotation 是否更新为期望的值
		if value, exists := pod.Annotations[annotationKey]; exists && value == expectedValue {
			logger.V(1).Info("Pod 的 annotation 已更新为期望的值", "podName", pod.Name, "annotationKey", annotationKey, "value", value)
			return nil
		}
	}

	return fmt.Errorf("未找到符合条件的 Pod annotation")
}

// waitForPodReady 等待新的 Pod 处于 Running 状态
func (m *Migrator) waitForPodReady(ctx context.Context, namespace string, deployment string, oldPods map[string]corev1.Pod) error {
	logger := klog.FromContext(ctx)
	logger.V(1).Info("等待新的 Pod 处于 Running 状态", "appLabel", deployment)

	// 直接for循环请求，遍历podList查看是否处于running状态
	for {
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
		time.Sleep(50 * time.Millisecond)
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

func (m *Migrator) MigratePod(ctx context.Context, namespace string, podName string, appLabel string, targetNode string) error {
	logger := klog.FromContext(ctx)
	logger.V(1).Info("开始迁移Pod", "podName", podName, "targetNode", targetNode)
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
	logger.V(2).Info("开始设置节点为不可调度", "targetNode", targetNode)
	// Get all nodes
	nodes, err := m.clientset.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
	if err != nil {
		return fmt.Errorf("failed to get node list: %v", err)
	}
	// Iterate through all nodes, cordon all except the target node
	for _, node := range nodes.Items {
		if node.Name != targetNode {
			node.Spec.Unschedulable = true
			_, err := m.clientset.CoreV1().Nodes().Update(ctx, &node, metav1.UpdateOptions{})
			if err != nil {
				logger.Error(err, "failed to set node as unschedulable", "node", node.Name)
			} else {
				logger.V(2).Info("successfully set node as unschedulable", "node", node.Name)
			}
		}
	}
	logger.V(2).Info("开始驱逐Pod", "podName", podName)
	// Evict the current pod
	err = m.clientset.CoreV1().Pods(namespace).Evict(ctx, &v1beta1.Eviction{
		ObjectMeta: metav1.ObjectMeta{
			Name:      podName,
			Namespace: namespace,
		},
	})
	if err != nil {
		return fmt.Errorf("failed to evict Pod: %v", err)
	}
	// Get the status of the new pod and wait for it to initialize successfully
	for {
		pods, err := m.clientset.CoreV1().Pods(namespace).List(ctx, metav1.ListOptions{
			LabelSelector: appLabel,
		})
		if err != nil {
			return fmt.Errorf("failed to get Pod list: %v", err)
		}

		// The new node hasn't appeared in the previous map
		var newPod *corev1.Pod
		for _, pod := range pods.Items {
			if _, ok := podMap[pod.Name]; !ok && pod.Status.Phase == corev1.PodRunning {
				newPod = &pod
				break
			}
		}

		if newPod == nil {
			logger.V(2).Info("new Pod is not ready yet, waiting...")
		} else {
			// check if newPod is in the target Node
			if newPod.Spec.NodeName != targetNode {
				logger.V(0).Info("new pods is incorrectly scheduled", "podName", newPod.Name, "nodeName", newPod.Spec.NodeName)
			}
			// 获取Pod的deployment，并且得到deployment，确定其replicas
			deployment, err := m.clientset.AppsV1().Deployments(namespace).Get(ctx, newPod.Labels["app"], metav1.GetOptions{})

			if err != nil {
				logger.Error(err, "获取deployment失败")
				return err
			}
			if deployment.Status.ReadyReplicas != 2 {
				logger.V(1).Info("old Pod is removed yet, waiting...", "podName", newPod.Name)
			} else {
				logger.V(1).Info("new Pod has been successfully initialized", "podName", newPod.Name)
				break
			}
		}
		time.Sleep(1 * time.Second)
	}

	// Restore all nodes to schedulable state
	logger.V(1).Info("开始恢复节点为可调度", "targetNode", targetNode)
	nodes, err = m.clientset.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
	if err != nil {
		return fmt.Errorf("failed to get node list: %v", err)
	}
	for _, node := range nodes.Items {
		if node.Spec.Unschedulable {
			logger.V(2).Info("restoring node to schedulable state", "node", node.Name)
			node.Spec.Unschedulable = false
			_, err := m.clientset.CoreV1().Nodes().Update(ctx, &node, metav1.UpdateOptions{})
			if err != nil {
				return fmt.Errorf("failed to restore node to schedulable state: %v", err)
			} else {
				logger.V(2).Info("successfully restored node to schedulable state", "node", node.Name)
			}
		}
	}
	return nil
}

func (m *Migrator) waitForReplicaDecrease(ctx context.Context, namespace string, deploymentName string, replicaCnt int32, podName string) error {
	logger := klog.FromContext(ctx)
	logger.V(2).Info("等待replica减少", "deploymentName", deploymentName)
	for {
		deployment, err := m.clientset.AppsV1().Deployments(namespace).Get(ctx, deploymentName, metav1.GetOptions{})
		if err != nil {
			logger.Error(err, "获取deployment失败")
			return err
		}

		if deployment.Status.ReadyReplicas == replicaCnt {
			logger.V(2).Info(fmt.Sprintf("replica已经减少为%d", replicaCnt))
			// check whether the pod is deleted
			podList, err := m.clientset.CoreV1().Pods(namespace).List(ctx, metav1.ListOptions{
				LabelSelector: fmt.Sprintf("app=%s", deploymentName),
			})
			if err != nil {
				logger.Error(err, "无法获取 Pods 列表")
				return err
			}

			for _, pod := range podList.Items {
				// 检查 Pod 是否处于 Running 且 Ready 状态
				if pod.Name == podName {
					return fmt.Errorf("pod %s is still running", podName)
				}
			}
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
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
