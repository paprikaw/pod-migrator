package main

import (
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/klog/v2"
)

func checkDeploymentsPodsReady(ctx context.Context, clientset *kubernetes.Clientset, namespace string, labelSelector string) (bool, error) {
	logger := klog.FromContext(ctx)

	// 获取所有匹配 label 的 Deployments
	deploymentList, err := clientset.AppsV1().Deployments(namespace).List(ctx, metav1.ListOptions{
		LabelSelector: labelSelector,
	})
	if err != nil {
		logger.Error(err, "无法获取 Deployment 列表")
		return false, err
	}

	// 遍历每个 Deployment，检查其 Pods 状态
	for _, deployment := range deploymentList.Items {
		// 获取 Deployment 的 Pods 列表
		podList, err := clientset.CoreV1().Pods(namespace).List(ctx, metav1.ListOptions{
			LabelSelector: fmt.Sprintf("app=%s", deployment.Spec.Template.Labels["app"]),
		})
		if err != nil {
			logger.Error(err, "无法获取 Pod 列表", "deployment", deployment.Name)
			return false, err
		}

		// 检查每个 Pod 是否处于 Running 且 Ready 状态
		for _, pod := range podList.Items {
			if pod.Status.Phase != corev1.PodRunning {
				logger.V(2).Info("Pod is not in Running phase", "pod", pod.Name)
				return false, nil
			}
			for _, condition := range pod.Status.Conditions {
				if condition.Type == corev1.PodReady && condition.Status != corev1.ConditionTrue {
					logger.V(2).Info("Pod is not Ready", "pod", pod.Name)
					return false, nil
				}
			}
		}
	}

	// 如果所有 Deployment 的 Pods 都处于 Running 且 Ready 状态
	return true, nil
}
