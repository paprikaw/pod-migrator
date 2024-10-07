package proclient

import (
	"context"
	"fmt"
	"time"

	d "github.com/paprikaw/rscheduler/pkg/model"
	resourcehelper "github.com/paprikaw/rscheduler/pkg/utils"
	promapi "github.com/prometheus/client_golang/api"
	v1prom "github.com/prometheus/client_golang/api/prometheus/v1"
	"github.com/prometheus/common/model"
	v1core "k8s.io/api/core/v1"
	resource "k8s.io/apimachinery/pkg/api/resource"
	typev1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/scheduler/framework"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/noderesources"
)

type QueryClient struct {
	proClient v1prom.API
	clientset *kubernetes.Clientset
}

func NewQueryClient(PromeAddr string, kubeConfigPath string) (*QueryClient, error) {
	client, err := promapi.NewClient(promapi.Config{
		Address: PromeAddr,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create Prometheus client: %v", err)
	}

	config, err := clientcmd.BuildConfigFromFlags("", kubeConfigPath)
	if err != nil {
		return nil, fmt.Errorf("failed to load kubeconfig: %v", err)
	}
	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		return nil, fmt.Errorf("failed to create kubernetes client: %v", err)
	}
	prometheusClient := v1prom.NewAPI(client)

	return &QueryClient{proClient: prometheusClient, clientset: clientset}, nil
}

func (c *QueryClient) QeuryAppLatencyByMilli(ctx context.Context, destination_service string) (int64, error) {
	logger := klog.FromContext(ctx)

	// Construct query to get average end-to-end latency for a specific service over the past 2 minute
	query := fmt.Sprintf(`sum by (app_name) (increase(mub_request_processing_latency_milliseconds_sum{app_name="%s"}[1m])) / sum by (app_name) (increase(mub_request_processing_latency_milliseconds_count{app_name="%s"}[1m]))`, destination_service, destination_service)

	logger.V(3).Info("query statement", "query", query)
	// Execute query
	result, warnings, err := c.proClient.Query(ctx, query, time.Now())
	if err != nil {
		return 0, fmt.Errorf("failed to query Prometheus: %v", err)
	}
	if len(warnings) > 0 {
		fmt.Printf("warnings: %v", warnings)
	}

	// Parse query results
	vector, ok := result.(model.Vector)
	if !ok || vector.Len() == 0 {
		return 0, fmt.Errorf("no latency data found")
	}
	// Get latency value (in milliseconds)
	latency := int64(vector[0].Value)

	logger.V(3).Info("95th percentile end-to-end latency for service %s is %.2f milliseconds", destination_service, latency)

	return latency, nil
}

func (c *QueryClient) QueryNodeBandwidthByMBytes(ctx context.Context, nodeAddress string) (int64, error) {
	logger := klog.FromContext(ctx)

	// 构造查询语句，获取指定节点eth0接口的带宽使用量（以字节/秒为单位）
	query := fmt.Sprintf(`sum(rate(node_network_receive_bytes_total{device="eth0",instance="%s"}[1m]) + rate(node_network_transmit_bytes_total{device="eth0",instance="%s"}[1m]))`, nodeAddress, nodeAddress)

	logger.V(3).Info("查询语句", "query", query)

	// 执行查询
	result, warnings, err := c.proClient.Query(ctx, query, time.Now())
	if err != nil {
		return 0, fmt.Errorf("查询Prometheus失败: %v", err)
	}
	if len(warnings) > 0 {
		logger.V(1).Info("查询警告", "warnings", warnings)
	}

	// 解析查询结果
	vector, ok := result.(model.Vector)
	if !ok || vector.Len() == 0 {
		return 0, fmt.Errorf("未找到带宽数据")
	}

	// 获取带宽值（字节/秒）
	bandwidthBytesPerSecond := int64(vector[0].Value)

	// 将字节/秒转换为Mbps
	bandwidthMbps := bandwidthBytesPerSecond
	return bandwidthMbps, nil
}

func (c *QueryClient) GetPodsAvailableNodes(ctx context.Context, namespace string, appLabel string) (d.PodDeployable, error) {
	podList, err := c.GetPods(ctx, namespace, appLabel)
	if err != nil {
		return nil, err
	}
	podDeployable := make(d.PodDeployable)
	for _, pod := range podList.Items {
		availableNodes, err := c.getAvailableNodesForPod(ctx, &pod)
		if err != nil {
			return nil, err
		}
		podDeployable[pod.Name] = availableNodes
	}
	return podDeployable, nil
}

func (c *QueryClient) GetPods(ctx context.Context, namespace string, appLabel string) (*v1core.PodList, error) {
	return c.clientset.CoreV1().Pods(namespace).List(ctx, typev1.ListOptions{LabelSelector: appLabel})
}

func (c *QueryClient) GetNodes(ctx context.Context) (*v1core.NodeList, error) {
	return c.clientset.CoreV1().Nodes().List(ctx, typev1.ListOptions{})
}

func (c *QueryClient) filterNodesByResources(ctx context.Context, pod *v1core.Pod, nodes *v1core.NodeList) ([]*v1core.Node, error) {
	var feasibleNodes []*v1core.Node
	for _, node := range nodes.Items {
		if node.Name == "" {
			continue
		}
		nodeInfo := framework.NewNodeInfo()
		nodeInfo.SetNode(&node)
		cpureq, memoryreq, err := c.GetNodeTotalRequests(ctx, &node)
		if err != nil {
			return nil, err
		}

		nodeInfo.Requested.MilliCPU += cpureq.MilliValue()
		nodeInfo.Requested.Memory += memoryreq.Value()
		// Check if the node has enough resources
		unavailable_resources := noderesources.Fits(pod, nodeInfo)
		if len(unavailable_resources) == 0 {
			feasibleNodes = append(feasibleNodes, &node)
		}
	}

	return feasibleNodes, nil
}
func (c *QueryClient) getAvailableNodesForPod(ctx context.Context, pod *v1core.Pod) ([]string, error) {
	nodes, err := c.GetNodes(ctx)
	if err != nil {
		return nil, err
	}
	res := []string{}
	// Iterate through all nodes, check if the Pod can be scheduled to that node
	available_nodes, err := c.filterNodesByResources(ctx, pod, nodes)
	if err != nil {
		return nil, err
	}
	for _, node := range available_nodes {
		res = append(res, node.Name)
	}
	return res, nil
}

func (c *QueryClient) GetNodeTotalRequests(ctx context.Context, node *v1core.Node) (cpuReqs, memoryReqs resource.Quantity, err error) {
	// 获取该节点上的所有Pod
	fieldSelector := fmt.Sprintf("spec.nodeName=%s", node.Name)
	podList, err := c.clientset.CoreV1().Pods("").List(ctx, typev1.ListOptions{
		FieldSelector: fieldSelector,
	})

	reqs := resourcehelper.GetPodsTotalRequests(podList)
	cpuReqs, memoryReqs = reqs[v1core.ResourceCPU], reqs[v1core.ResourceMemory]
	if err != nil {
		return resource.Quantity{}, resource.Quantity{}, fmt.Errorf("获取节点 %s 上的Pod失败: %v", node.Name, err)
	}
	return cpuReqs, memoryReqs, nil
}
