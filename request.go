package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"

	m "github.com/paprikaw/rscheduler/pkg/migrator"
	d "github.com/paprikaw/rscheduler/pkg/model"
	q "github.com/paprikaw/rscheduler/pkg/queryclient"

	"k8s.io/klog/v2"
)

func GetRequest(ctx context.Context, config *Config, queryClient *q.QueryClient, migrator *m.Migrator, latency float64) (*d.Request, error) {
	clusterState, err := GetClusterState(ctx, config, queryClient)
	if err != nil {
		return nil, err
	}
	clusterState.Latency = latency
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

func GetMigrationResult(ctx context.Context, config *Config, queryClient *q.QueryClient, httpClient *http.Client, migrator *m.Migrator, latency float64) (*d.Response, error) {
	request, err := GetRequest(ctx, config, queryClient, migrator, latency)
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
		address := node.Status.Addresses[0].Address + ":9100"
		bandwidth, err := queryClient.QueryNodeBandwidthByMBytes(ctx, address)
		if err != nil {
			return nil, err
		}
		cpuReqs, memoryReqs, err := queryClient.GetNodeTotalRequests(ctx, &node)
		if err != nil {
			return nil, err
		}
		Nodes[node.Name] = d.Node{
			NodeName:           node.Name,
			CPUAvailability:    float64(node.Status.Allocatable.Cpu().MilliValue() - cpuReqs.MilliValue()),
			MemoryAvailability: float64(node.Status.Allocatable.Memory().Value()-memoryReqs.Value()) / (1024 * 1024 * 1024),
			BandwidthUsage:     float64(bandwidth),
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
