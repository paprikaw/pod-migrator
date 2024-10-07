package migrator

import (
	"context"
	"fmt"
	"strings"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/api/policy/v1beta1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
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
	logger.V(1).Info("等待 Pod 的 annotation 更新", "podName", podName, "annotationKey", annotationKey, "expectedValue", expectedValue)

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

	// 定义轮询机制, 等待deployment中的所有pod处于running状态，
	watch, err := m.clientset.CoreV1().Pods(namespace).Watch(ctx, metav1.ListOptions{
		LabelSelector: fmt.Sprintf("app=%s", deployment),
	})
	if err != nil {
		logger.Error(err, "无法监视 Pods")
		return err
	}
	defer watch.Stop()

	for event := range watch.ResultChan() {
		pod, ok := event.Object.(*corev1.Pod)
		if !ok {
			continue
		}
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

	return fmt.Errorf("未找到符合条件的 Running 状态 Pod")
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
	deploymentName := podName[:strings.LastIndex(podName[:strings.LastIndex(podName, "-")], "-")]
	logger := klog.FromContext(ctx)
	logger.V(1).Info("开始迁移Pod", "podName", podName, "targetNode", targetNode)
	// Get all nodes
	nodes, err := m.clientset.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
	if err != nil {
		return fmt.Errorf("failed to get node list: %v", err)
	}
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
			} else {
				logger.V(2).Info("successfully set node as unschedulable", "node", node.Name)
			}
		}
	}
	logger.V(2).Info("修改replicaset", "podName", podName)
	deployment, err := m.clientset.AppsV1().Deployments(namespace).Get(ctx, deploymentName, metav1.GetOptions{})
	logger.V(2).Info("deployment version", "version", deployment.Spec.Template.Labels["version"])
	if err != nil {
		logger.Error(err, "获取 Deployment 失败")
		return err
	}

	// Step 3: 增加 replicas 数量
	originalReplicas := *deployment.Spec.Replicas
	logger.V(2).Info("原始 replicas 数量", "originalReplicas", originalReplicas)
	newReplicas := originalReplicas + 1
	deployment.Spec.Replicas = &newReplicas
	logger.V(2).Info("修改replica", "newReplicas", newReplicas)
	deployment, err = m.clientset.AppsV1().Deployments(namespace).Update(ctx, deployment, metav1.UpdateOptions{})
	if err != nil {
		logger.Error(err, "增加replica失败")
		return err
	}

	// Step 4: 等待新 Pod 运行
	err = m.waitForPodReady(ctx, namespace, deploymentName, podMap)
	if err != nil {
		logger.Error(err, "等待新的 Pod 运行失败")
		return err
	}

	// Step 5: 恢复节点为可调度
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

	// Step 5: 将replica set改回去
	// 获取 Pod
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

	deployment, err = m.clientset.AppsV1().Deployments(namespace).Get(ctx, deploymentName, metav1.GetOptions{})
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

	// Restore all nodes to schedulable state
	return nil
}
