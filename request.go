package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"

	de "github.com/paprikaw/rscheduler/pkg/dataexporter"
	m "github.com/paprikaw/rscheduler/pkg/migrator"
	d "github.com/paprikaw/rscheduler/pkg/model"
	q "github.com/paprikaw/rscheduler/pkg/queryclient"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/klog/v2"
)

func GetRequest(ctx context.Context, config *Config, queryClient *q.QueryClient, migrator *m.Migrator) (*d.Request, error) {
	clusterState, err := GetClusterState(ctx, config, queryClient)
	if err != nil {
		return nil, err
	}
	podDeployable, err := queryClient.GetPodsAvailableNodes(ctx, config.Namespace, config.AppLabel)
	if err != nil {
		return nil, err
	}
	request := d.Request{
		ClusterState:  *clusterState,
		PodDeployable: podDeployable,
	}
	return &request, nil
}

func GetPodDistribution(ctx context.Context, config *Config, migrator *m.Migrator) (aggregator de.PodDistribution, detection de.PodDistribution, ml de.PodDistribution, db de.PodDistribution, err error) {
	pods, err := migrator.GetK8sClient().CoreV1().Pods(config.Namespace).List(ctx, metav1.ListOptions{
		LabelSelector: config.AppLabel,
	})
	for _, pod := range pods.Items {
		nodeName := pod.Spec.NodeName
		switch {
		case strings.HasPrefix(pod.Name, "aggregator"):
			switch {
			case strings.Contains(nodeName, "cloud-vm-8-1"):
				aggregator.Cloud_8_1++
			case strings.Contains(nodeName, "cloud-vm-8-2"):
				aggregator.Cloud_8_2++
			case strings.Contains(nodeName, "edge-vm-4-1"):
				aggregator.Edge_4_1++
			case strings.Contains(nodeName, "edge-vm-4-2"):
				aggregator.Edge_4_2++
			case strings.Contains(nodeName, "edge-vm-2-1"):
				aggregator.Edge_2_1++
			case strings.Contains(nodeName, "edge-vm-2-2"):
				aggregator.Edge_2_2++
			}
		case strings.HasPrefix(pod.Name, "detection"):
			switch {
			case strings.Contains(nodeName, "cloud-vm-8-1"):
				detection.Cloud_8_1++
			case strings.Contains(nodeName, "cloud-vm-8-2"):
				detection.Cloud_8_2++
			case strings.Contains(nodeName, "edge-vm-4-1"):
				detection.Edge_4_1++
			case strings.Contains(nodeName, "edge-vm-4-2"):
				detection.Edge_4_2++
			case strings.Contains(nodeName, "edge-vm-2-1"):
				detection.Edge_2_1++
			case strings.Contains(nodeName, "edge-vm-2-2"):
				detection.Edge_2_2++
			}
		case strings.HasPrefix(pod.Name, "machine-learning"):
			switch {
			case strings.Contains(nodeName, "cloud-vm-8-1"):
				ml.Cloud_8_1++
			case strings.Contains(nodeName, "cloud-vm-8-2"):
				ml.Cloud_8_2++
			case strings.Contains(nodeName, "edge-vm-4-1"):
				ml.Edge_4_1++
			case strings.Contains(nodeName, "edge-vm-4-2"):
				ml.Edge_4_2++
			case strings.Contains(nodeName, "edge-vm-2-1"):
				ml.Edge_2_1++
			case strings.Contains(nodeName, "edge-vm-2-2"):
				ml.Edge_2_2++
			}
		case strings.HasPrefix(pod.Name, "db"):
			switch {
			case strings.Contains(nodeName, "cloud-vm-8-1"):
				db.Cloud_8_1++
			case strings.Contains(nodeName, "cloud-vm-8-2"):
				db.Cloud_8_2++
			case strings.Contains(nodeName, "edge-vm-4-1"):
				db.Edge_4_1++
			case strings.Contains(nodeName, "edge-vm-4-2"):
				db.Edge_4_2++
			case strings.Contains(nodeName, "edge-vm-2-1"):
				db.Edge_2_1++
			case strings.Contains(nodeName, "edge-vm-2-2"):
				db.Edge_2_2++
			}
		}
	}
	if err != nil {
		return de.PodDistribution{}, de.PodDistribution{}, de.PodDistribution{}, de.PodDistribution{}, err
	}
	return aggregator, detection, ml, db, nil
}
func GetMigrationResult(ctx context.Context, config *Config, queryClient *q.QueryClient, httpClient *http.Client, migrator *m.Migrator, failedNodes []string) (*d.Response, error) {
	request, err := GetRequest(ctx, config, queryClient, migrator)
	logger := klog.FromContext(ctx)
	for _, node := range failedNodes {
		nodeData := request.ClusterState.Nodes[node]
		nodeData.CPUAvailability = 0
		nodeData.MemoryAvailability = 0
		request.ClusterState.Nodes[node] = nodeData
		for podName, deployableNodes := range request.PodDeployable {
			for i, deployableNode := range deployableNodes {
				if deployableNode == node {
					deployableNodes = append(deployableNodes[:i], deployableNodes[i+1:]...)
					break
				}
			}
			request.PodDeployable[podName] = deployableNodes
		}
	}
	logger.V(3).Info("当前集群状态", "state", request.ClusterState)
	logger.V(3).Info("当前pod部署情况", "state", request.PodDeployable)
	if err != nil {
		return nil, err
	}
	jsonData, err := json.Marshal(request)
	if err != nil {
		return nil, err
	}

	// 发送POST请求
	resp, err := httpClient.Post("http://localhost:5000/get_action", "application/json", bytes.NewBuffer(jsonData))
	if err != nil {
		return nil, err
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("HTTP请求失败，状态码：%d", resp.StatusCode)
	}

	defer resp.Body.Close()

	// 读取响应体
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	// 解析JSON响应
	var response d.Response
	err = json.Unmarshal(body, &response)
	if err != nil {
		return nil, err
	}
	return &response, nil
}

func GetClusterState(ctx context.Context, config *Config, queryClient *q.QueryClient) (*d.ClusterState, error) {
	logger := klog.FromContext(ctx)
	// 获取所有节点
	nodes, err := queryClient.GetNodes(ctx)
	if err != nil {
		return nil, err
	}
	Nodes := make(map[string]d.Node, len(nodes.Items))
	for _, node := range nodes.Items {
		cpuReqs, memoryReqs, err := queryClient.GetNodeTotalRequests(ctx, &node)
		if err != nil {
			return nil, err
		}
		Nodes[node.Name] = d.Node{
			NodeName:           node.Name,
			CPUAvailability:    float64(node.Status.Allocatable.Cpu().MilliValue() - cpuReqs.MilliValue()),
			MemoryAvailability: float64(node.Status.Allocatable.Memory().Value()-memoryReqs.Value()) / (1024 * 1024 * 1024),
		}
	}

	pods, err := queryClient.GetPods(ctx, config.Namespace, config.AppLabel)
	if err != nil {
		return nil, err
	}

	Services := make(map[string]d.Service, len(pods.Items))
	for _, pod := range pods.Items {
		// 从pod.Name中提取serviceName
		serviceName := pod.Name
		if parts := strings.Split(pod.Name, "-"); len(parts) > 2 {
			serviceName = strings.Join(parts[:len(parts)-2], "-")
		}
		if _, ok := Services[serviceName]; !ok {
			Services[serviceName] = d.Service{ServiceName: serviceName, Pods: []d.Pod{}}
		}
		service := Services[serviceName]
		service.Pods = append(service.Pods, d.Pod{
			NodeName: pod.Spec.NodeName,
			PodName:  pod.Name,
		})
		Services[serviceName] = service
	}
	clusterState := &d.ClusterState{
		Nodes:    Nodes,
		Services: Services,
	}
	logger.V(5).Info("当前集群状态", "state", clusterState)
	return clusterState, nil
}
