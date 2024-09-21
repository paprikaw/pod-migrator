package main

import (
	"context"
	"fmt"
	"strings"
	"time"

	resourcehelper "github.com/paprikaw/rscheduler/pkg/utils"
	v1core "k8s.io/api/core/v1"
	"k8s.io/api/policy/v1beta1"
	resource "k8s.io/apimachinery/pkg/api/resource"
	typev1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/scheduler/framework"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/noderesources"
)

type Migrator struct {
	clientset *kubernetes.Clientset
	proClient *ProClient
	// metricsClient    metricsv.Interface
	appLabel       string
	gatewayService string
	namespace      string
	qosThreshold   int
}

func NewMigrator(kubeconfig string, prometheusAddr string, appLabel string, gatewayService string, namespace string, qosThreshold int) (*Migrator, error) {

	config, err := clientcmd.BuildConfigFromFlags("", kubeconfig)
	if err != nil {
		return nil, fmt.Errorf("failed to load kubeconfig: %v", err)
	}

	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		return nil, fmt.Errorf("failed to create kubernetes client: %v", err)
	}
	proClient, err := NewProClient(prometheusAddr)
	if err != nil {
		return nil, fmt.Errorf("failed to create prometheus client: %v", err)
	}
	return &Migrator{
		clientset:      clientset,
		proClient:      proClient,
		appLabel:       appLabel,
		gatewayService: gatewayService,
		namespace:      namespace,
		qosThreshold:   qosThreshold,
	}, nil
}

func (m *Migrator) MigratePod(ctx context.Context, podName string, targetNode string) error {
	logger := klog.FromContext(ctx)
	logger.V(1).Info("开始迁移Pod", "podName", podName, "targetNode", targetNode)
	// Get all nodes
	nodes, err := m.clientset.CoreV1().Nodes().List(ctx, typev1.ListOptions{})
	if err != nil {
		return fmt.Errorf("failed to get node list: %v", err)
	}
	// Get all pods of the same deployment
	pods, err := m.clientset.CoreV1().Pods(m.namespace).List(ctx, typev1.ListOptions{
		LabelSelector: m.appLabel,
	})
	if err != nil {
		return fmt.Errorf("failed to get Pod list: %v", err)
	}
	podMap := make(map[string]v1core.Pod)

	// Check if the target pod is in the list
	for _, pod := range pods.Items {
		podMap[pod.Name] = pod
	}
	if _, ok := podMap[podName]; !ok {
		return fmt.Errorf("target Pod does not exist: %s", podName)
	}
	logger.V(2).Info("开始设置节点为不可调度", "targetNode", targetNode)
	// Iterate through all nodes, cordon all except the target node
	for _, node := range nodes.Items {
		if node.Name != targetNode {
			node.Spec.Unschedulable = true
			_, err := m.clientset.CoreV1().Nodes().Update(ctx, &node, typev1.UpdateOptions{})
			if err != nil {
				logger.Error(err, "failed to set node as unschedulable", "node", node.Name)
			} else {
				logger.V(2).Info("successfully set node as unschedulable", "node", node.Name)
			}
		}
	}
	logger.V(2).Info("开始驱逐Pod", "podName", podName)
	// Evict the current pod
	err = m.clientset.CoreV1().Pods(m.namespace).Evict(ctx, &v1beta1.Eviction{
		ObjectMeta: typev1.ObjectMeta{
			Name:      podName,
			Namespace: m.namespace,
		},
	})

	if err != nil {
		return fmt.Errorf("failed to evict Pod: %v", err)
	}
	// Get the status of the new pod and wait for it to initialize successfully
	for {
		pods, err := m.clientset.CoreV1().Pods(m.namespace).List(ctx, typev1.ListOptions{
			LabelSelector: m.appLabel,
		})
		if err != nil {
			return fmt.Errorf("failed to get Pod list: %v", err)
		}

		// The new node hasn't appeared in the previous map
		var newPod *v1core.Pod
		for _, pod := range pods.Items {
			if _, ok := podMap[pod.Name]; !ok && pod.Status.Phase == v1core.PodRunning {
				newPod = &pod
				break
			}
		}
		if newPod == nil {
			logger.V(2).Info("new Pod is not ready yet, waiting...")
			time.Sleep(3 * time.Second)
			continue
		} else {
			logger.V(1).Info("new Pod has been successfully initialized", "podName", newPod.Name)
			break
		}
	}

	// Restore all nodes to schedulable state
	logger.V(1).Info("开始恢复节点为可调度", "targetNode", targetNode)
	nodes, err = m.clientset.CoreV1().Nodes().List(ctx, typev1.ListOptions{})
	if err != nil {
		return fmt.Errorf("failed to get node list: %v", err)
	}
	for _, node := range nodes.Items {
		if node.Spec.Unschedulable {
			logger.V(2).Info("restoring node to schedulable state", "node", node.Name)
			node.Spec.Unschedulable = false
			_, err := m.clientset.CoreV1().Nodes().Update(ctx, &node, typev1.UpdateOptions{})
			if err != nil {
				return fmt.Errorf("failed to restore node to schedulable state: %v", err)
			} else {
				logger.V(2).Info("successfully restored node to schedulable state", "node", node.Name)
			}
		}
	}
	return nil
}

func (m *Migrator) GetClusterState(ctx context.Context) (*ClusterState, error) {
	logger := klog.FromContext(ctx)
	// 获取所有节点
	nodes, err := m.getNodes(ctx)
	if err != nil {
		return nil, err
	}
	Nodes := make(map[string]Node, len(nodes.Items))
	for _, node := range nodes.Items {
		address := node.Status.Addresses[0].Address + ":9100"
		bandwidth, err := m.proClient.QueryNodeBandwidthByMBytes(ctx, address)
		if err != nil {
			return nil, err
		}
		cpuReqs, memoryReqs, err := m.getNodeTotalRequests(ctx, &node)
		if err != nil {
			return nil, err
		}
		Nodes[node.Name] = Node{
			NodeName:           node.Name,
			CPUAvailability:    float64(node.Status.Allocatable.Cpu().MilliValue() - cpuReqs.MilliValue()),
			MemoryAvailability: float64(node.Status.Allocatable.Memory().Value()-memoryReqs.Value()) / (1024 * 1024 * 1024),
			BandwidthUsage:     float64(bandwidth),
		}
	}

	pods, err := m.getPods(ctx)
	if err != nil {
		return nil, err
	}

	Services := make(map[string]Service, len(pods.Items))
	for _, pod := range pods.Items {
		// 从pod.Name中提取serviceName
		serviceName := pod.Name
		if parts := strings.Split(pod.Name, "-"); len(parts) > 2 {
			serviceName = strings.Join(parts[:len(parts)-2], "-")
		}
		if _, ok := Services[serviceName]; !ok {
			Services[serviceName] = Service{ServiceName: serviceName, Pods: []Pod{}}
		}
		service := Services[serviceName]
		service.Pods = append(service.Pods, Pod{
			NodeName: pod.Spec.NodeName,
			PodName:  pod.Name,
		})
		Services[serviceName] = service
	}
	clusterState := &ClusterState{
		Nodes:    Nodes,
		Services: Services,
	}
	logger.V(5).Info("当前集群状态", "state", clusterState)
	return clusterState, nil
}

func (m *Migrator) GetPodsAvailableNodes(ctx context.Context) (PodDeployable, error) {
	logger := klog.FromContext(ctx)
	podList, err := m.getPods(ctx)
	if err != nil {
		return nil, err
	}
	podDeployable := make(PodDeployable)
	for _, pod := range podList.Items {
		availableNodes, err := m.getAvailableNodesForPod(ctx, &pod)
		if err != nil {
			return nil, err
		}
		podDeployable[pod.Name] = availableNodes
	}
	logger.V(5).Info("当前pod可部署节点", "state", podDeployable)
	return podDeployable, nil
}

func (m *Migrator) GetQosThreshold(ctx context.Context) int {
	return m.qosThreshold
}

func (m *Migrator) getPods(ctx context.Context) (*v1core.PodList, error) {
	return m.clientset.CoreV1().Pods(m.namespace).List(ctx, typev1.ListOptions{LabelSelector: m.appLabel})
}

func (m *Migrator) getNodes(ctx context.Context) (*v1core.NodeList, error) {
	return m.clientset.CoreV1().Nodes().List(ctx, typev1.ListOptions{})
}

func (m *Migrator) filterNodesByResources(ctx context.Context, pod *v1core.Pod, nodes *v1core.NodeList) ([]*v1core.Node, error) {
	var feasibleNodes []*v1core.Node
	for _, node := range nodes.Items {
		if node.Name == "" {
			continue
		}
		nodeInfo := framework.NewNodeInfo()
		nodeInfo.SetNode(&node)
		cpureq, memoryreq, err := m.getNodeTotalRequests(ctx, &node)
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
func (m *Migrator) getAvailableNodesForPod(ctx context.Context, pod *v1core.Pod) ([]string, error) {
	nodes, err := m.getNodes(ctx)
	if err != nil {
		return nil, err
	}
	res := []string{}
	// Iterate through all nodes, check if the Pod can be scheduled to that node
	available_nodes, err := m.filterNodesByResources(ctx, pod, nodes)
	if err != nil {
		return nil, err
	}
	for _, node := range available_nodes {
		res = append(res, node.Name)
	}
	return res, nil
}

func (m *Migrator) getNodeTotalRequests(ctx context.Context, node *v1core.Node) (cpuReqs, memoryReqs resource.Quantity, err error) {
	// 获取该节点上的所有Pod
	fieldSelector := fmt.Sprintf("spec.nodeName=%s", node.Name)
	podList, err := m.clientset.CoreV1().Pods("").List(ctx, typev1.ListOptions{
		FieldSelector: fieldSelector,
	})

	reqs := resourcehelper.GetPodsTotalRequests(podList)
	cpuReqs, memoryReqs = reqs[v1core.ResourceCPU], reqs[v1core.ResourceMemory]
	if err != nil {
		return resource.Quantity{}, resource.Quantity{}, fmt.Errorf("获取节点 %s 上的Pod失败: %v", node.Name, err)
	}
	return cpuReqs, memoryReqs, nil
}
