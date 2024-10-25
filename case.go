package main

import (
	"context"
	"fmt"
	"time"

	de "github.com/paprikaw/rscheduler/pkg/dataexporter"
	m "github.com/paprikaw/rscheduler/pkg/migrator"
	q "github.com/paprikaw/rscheduler/pkg/queryclient"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	retry "k8s.io/client-go/util/retry"
	"k8s.io/klog/v2"
)

func nodeFailedCase(ctx context.Context, config *Config, migrator *m.Migrator, queryClient *q.QueryClient, dataExporter *de.DataExporter, reschedulingDoneCh chan bool, namespace string, appLabel string, waitingTime int, afterWaitTime int, nodeNames []string) error {
	logger := klog.FromContext(ctx)
	var backoff = wait.Backoff{
		Steps:    1000,
		Duration: 50 * time.Millisecond,
		Factor:   1.0,
		Jitter:   0.1,
	}
	time.Sleep(time.Duration(waitingTime) * time.Second)
	logger.V(2).Info("waiting time", "waitingTime", waitingTime)
	schedulingMutex.Lock()
	for i, nodeName := range nodeNames {
		logger.V(2).Info("rescheduling done, start to cordon the node")
		dataExporter.WriteTS(time.Now().Unix(), estimatedRT.Get(), "", "", "", nodeName, de.NODE_FAILED)
		err := cordonNode(ctx, migrator, nodeName, backoff, i+1)
		if err != nil {
			logger.Error(err, "cordon node failed")
			return err
		}
		failedNodes.Add(nodeName)
		err = migrator.EvictAllPods(ctx, namespace, appLabel, nodeName)
		if err != nil {
			logger.Error(err, "驱逐 Deployment 下的所有 Pod 失败")
			return err
		}
	}
	err := waitForPodReady(ctx, config, queryClient)
	if err != nil {
		logger.Error(err, "等待pod ready失败")
		return err
	}
	for i, nodeName := range nodeNames {
		err = uncordonNode(ctx, config, migrator, dataExporter, nodeName, backoff, 1-i)
		if err != nil {
			logger.Error(err, "uncordon node failed")
			return err
		}
	}
	schedulingMutex.Unlock()
	time.Sleep(time.Duration(afterWaitTime) * time.Second) // 等待rescheduling 完成
	dataExporter.WriteTS(time.Now().Unix(), estimatedRT.Get(), "", "", "", "Finish", de.NODE_RECOVERED)
	return nil
}

func autoscalingCase(ctx context.Context, config *Config, migrator *m.Migrator, queryClient *q.QueryClient, dataExporter *de.DataExporter, reschedulingDoneCh chan bool, namespace string, targetReplica int32) error {
	var backoff = wait.Backoff{
		Steps:    100,
		Duration: 50 * time.Millisecond,
		Factor:   1.0,
		Jitter:   0.1,
	}
	logger := klog.FromContext(ctx)
	k8s_client := migrator.GetK8sClient()
	waitForPodReady(ctx, config, queryClient)
	if <-reschedulingDoneCh {
		waitForPodReady(ctx, config, queryClient)
		// Starts to increase the replica number
		dataExporter.WriteTS(time.Now().Unix(), 0, "", "", "", "", de.AUTOSCALING)
		logger.V(2).Info("开始增加replica")
		err := retry.RetryOnConflict(backoff, func() error {
			deployment, err := k8s_client.AppsV1().Deployments(namespace).Get(ctx, "aggregator", metav1.GetOptions{})
			if err != nil {
				logger.Error(err, "获取 Deployment 失败")
				return err
			}
			deployment.Spec.Replicas = &targetReplica
			logger.V(3).Info("修改replica", "newReplicas", targetReplica)
			dataExporter.WriteTS(time.Now().Unix(), 0, "", "", "", "", de.AUTOSCALING)
			_, err = k8s_client.AppsV1().Deployments(namespace).Update(ctx, deployment, metav1.UpdateOptions{})
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
	}
	return nil
}

func uncordonNode(ctx context.Context, config *Config, migrator *m.Migrator, dataExporter *de.DataExporter, targetNode string, backoff wait.Backoff, leftCordenedNodeCnt int) error {
	k8s_client := migrator.GetK8sClient()
	logger := klog.FromContext(ctx)
	logger.V(2).Info("开始恢复节点为可调度")
	err := retry.RetryOnConflict(backoff, func() error {
		nodes, err := k8s_client.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
		if err != nil {
			return fmt.Errorf("failed to get node list: %v", err)
		}
		for _, node := range nodes.Items {
			if node.Name == targetNode {
				node.Spec.Unschedulable = false
				_, err := k8s_client.CoreV1().Nodes().Update(ctx, &node, metav1.UpdateOptions{})
				if err != nil {
					return fmt.Errorf("failed to restore node to schedulable state: %v", err)
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
	for i := 0; i < backoff.Steps; i++ {
		ok, err := migrator.CheckCordonNode(ctx, leftCordenedNodeCnt)
		if err != nil {
			logger.Error(err, "检查uncordon的节点数量失败")
			return err
		}
		if ok {
			break
		}
		time.Sleep(backoff.Duration)
	}
	logger.V(2).Info("uncordon nodes状态达到预期")
	return nil
}

func cordonNode(ctx context.Context, migrator *m.Migrator, targetNode string, backoff wait.Backoff, cordonNodeCnt int) error {
	logger := klog.FromContext(ctx)
	k8s_client := migrator.GetK8sClient()
	err := retry.RetryOnConflict(backoff, func() error {
		node, err := k8s_client.CoreV1().Nodes().Get(ctx, targetNode, metav1.GetOptions{})
		if err != nil {
			return err
		}
		node.Spec.Unschedulable = true
		_, err = k8s_client.CoreV1().Nodes().Update(ctx, node, metav1.UpdateOptions{})
		if err != nil {
			logger.Error(err, "failed to set node as unschedulable", "node", node.Name)
			return err
		} else {
			logger.V(2).Info("successfully set node as unschedulable", "node", node.Name)
		}
		return nil
	})
	if err != nil {
		logger.Error(err, "failed to set node as unschedulable", "node", targetNode)
		return err
	}
	// wait for the node to be cordoned
	for i := 0; i < backoff.Steps; i++ {
		ok, err := migrator.CheckCordonNode(ctx, cordonNodeCnt)
		if err != nil {
			logger.Error(err, "检查uncordon的节点数量失败")
			return err
		}
		if ok {
			break
		}
		logger.V(2).Info("检查cordon nodes状态失败，等待下一轮检查")
		time.Sleep(backoff.Duration)
	}
	logger.V(1).Info("cordon nodes状态达到预期")
	return nil
}
