package main

import (
	"context"
	"fmt"
	"time"

	promapi "github.com/prometheus/client_golang/api"
	v1prom "github.com/prometheus/client_golang/api/prometheus/v1"
	"github.com/prometheus/common/model"
	v1apps "k8s.io/api/apps/v1"
	v1core "k8s.io/api/core/v1"
	"k8s.io/api/policy/v1beta1"
	typev1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/scheduler/framework"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/noderesources"
)

type Migrator struct {
	clientset        *kubernetes.Clientset
	prometheusClient v1prom.API
	appLabel         string
	gatewayService   string
	namespace        string
}

func NewMigrator(kubeconfig string, prometheusAddr string, appLabel string, gatewayService string, namespace string) (*Migrator, error) {

	config, err := clientcmd.BuildConfigFromFlags("", kubeconfig)
	if err != nil {
		return nil, fmt.Errorf("加载kubeconfig失败: %v", err)
	}

	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		return nil, fmt.Errorf("创建kubernetes客户端失败: %v", err)
	}

	client, err := promapi.NewClient(promapi.Config{
		Address: prometheusAddr,
	})
	if err != nil {
		return nil, fmt.Errorf("创建Prometheus客户端失败: %v", err)
	}

	prometheusClient := v1prom.NewAPI(client)

	return &Migrator{
		clientset:        clientset,
		prometheusClient: prometheusClient,
		appLabel:         appLabel,
		gatewayService:   gatewayService,
		namespace:        namespace,
	}, nil
}
func filterNodesByResources(pod *v1core.Pod, nodes []*framework.NodeInfo) ([]*framework.NodeInfo, error) {
	var feasibleNodes []*framework.NodeInfo

	for _, nodeInfo := range nodes {
		if nodeInfo.Node() == nil {
			continue
		}

		// 检查节点是否有足够的资源
		unavailable_resources := noderesources.Fits(pod, nodeInfo)
		if len(unavailable_resources) == 0 {
			feasibleNodes = append(feasibleNodes, nodeInfo)
		} else {
			fmt.Println("unavailable resources for node", nodeInfo.Node().Name, unavailable_resources)
		}
	}

	return feasibleNodes, nil
}

func setNodeAffinity(deployment *v1apps.Deployment, nodeName string) {
	deployment.Spec.Template.Spec.Affinity = &v1core.Affinity{
		NodeAffinity: &v1core.NodeAffinity{
			RequiredDuringSchedulingIgnoredDuringExecution: &v1core.NodeSelector{
				NodeSelectorTerms: []v1core.NodeSelectorTerm{
					{
						MatchExpressions: []v1core.NodeSelectorRequirement{
							{
								Key:      "kubernetes.io/hostname",
								Operator: v1core.NodeSelectorOpIn,
								Values:   []string{nodeName},
							},
						},
					},
				},
			},
		},
	}
}

func (m *Migrator) MigratePod(ctx context.Context, deploymentName string, podName string, targetNode string) error {
	logger := klog.FromContext(ctx)
	// 获取所有节点
	nodes, err := m.clientset.CoreV1().Nodes().List(ctx, typev1.ListOptions{})
	if err != nil {
		return fmt.Errorf("获取节点列表失败: %v", err)
	}
	// 获取所有相同deployment的pod
	pods, err := m.clientset.CoreV1().Pods(m.namespace).List(ctx, typev1.ListOptions{
		LabelSelector: "app=" + deploymentName,
	})
	if err != nil {
		return fmt.Errorf("获取Pod列表失败: %v", err)
	}
	podMap := make(map[string]v1core.Pod)

	// 查看目标pod是否在列表中
	for _, pod := range pods.Items {
		podMap[pod.Name] = pod
	}
	if _, ok := podMap[podName]; !ok {
		return fmt.Errorf("目标Pod不存在", "pod", podName)
	}

	// 遍历所有节点，除了目标节点外全部cordon
	for _, node := range nodes.Items {
		if node.Name != targetNode {
			node.Spec.Unschedulable = true
			_, err := m.clientset.CoreV1().Nodes().Update(ctx, &node, typev1.UpdateOptions{})
			if err != nil {
				logger.Error(err, "设置节点为不可调度失败", "node", node.Name)
			} else {
				logger.V(0).Info("成功将节点设置为不可调度", "node", node.Name)
			}
		}
	}

	// evict当前的pod
	err = m.clientset.CoreV1().Pods(m.namespace).Evict(ctx, &v1beta1.Eviction{
		ObjectMeta: typev1.ObjectMeta{
			Name:      podName,
			Namespace: m.namespace,
		},
	})

	if err != nil {
		return fmt.Errorf("驱逐Pod失败: %v", err)
	}

	// 获取新的pod的状态，并且等待这个pod initialize成功
	for {
		pods, err := m.clientset.CoreV1().Pods(m.namespace).List(ctx, typev1.ListOptions{
			LabelSelector: "app=" + deploymentName,
		})
		if err != nil {
			return fmt.Errorf("获取Pod列表失败: %v", err)
		}

		// 新的node没有出现在刚才的map里面
		var newPod *v1core.Pod
		for _, pod := range pods.Items {
			if _, ok := podMap[pod.Name]; !ok && pod.Status.Phase == v1core.PodRunning {
				newPod = &pod
				break
			}
		}
		if newPod == nil {
			logger.V(0).Info("新的Pod尚未就绪，等待中...")
			time.Sleep(3 * time.Second)
			continue
		} else {
			logger.V(0).Info("新的Pod已成功初始化", "podName", newPod.Name)
			break
		}
	}

	// 恢复所有节点为可调度状态

	nodes, err = m.clientset.CoreV1().Nodes().List(ctx, typev1.ListOptions{})
	if err != nil {
		return fmt.Errorf("获取节点列表失败: %v", err)
	}
	for _, node := range nodes.Items {
		fmt.Printf("node: %s\n is unschedulable: %v\n", node.Name, node.Spec.Unschedulable)
		if node.Spec.Unschedulable {
			logger.V(0).Info("恢复节点为可调度状态", "node", node.Name)
			node.Spec.Unschedulable = false
			_, err := m.clientset.CoreV1().Nodes().Update(ctx, &node, typev1.UpdateOptions{})
			if err != nil {
				return fmt.Errorf("恢复节点为可调度状态失败: %v", err)
			} else {
				logger.V(0).Info("成功将节点恢复为可调度状态", "node", node.Name)
			}
		}
	}
	return nil
}

// func (m *Migrator) MigratePod(ctx context.Context, podName string, targetNode string) error {
// 	logger := klog.FromContext(ctx)

// 	// 获取Pod信息
// 	pod, err := m.clientset.CoreV1().Pods(m.namespace).Get(ctx, podName, typev1.GetOptions{})
// 	if err != nil {
// 		return fmt.Errorf("获取Pod失败: %v", err)
// 	}
// 	// 更新Pod的nodeName
// 	pod.Spec.NodeName = targetNode
// 	// 更新Pod

// 	_, err = m.clientset.CoreV1().Pods(m.namespace).Update(ctx, pod, typev1.UpdateOptions{})
// 	if err != nil {
// 		return fmt.Errorf("更新Pod失败: %v", err)
// 	}

// 	logger.V(0).Info("成功将Pod迁移到目标节点", "pod", podName, "targetNode", targetNode)
// 	// 从Pod的标签中获取deployment名称
// 	deploymentName, ok := pod.Labels["app"]
// 	if !ok {
// 		return fmt.Errorf("无法从Pod标签中获取deployment名称")
// 	}

// 	// 获取对应的Deployment
// 	deployment, err := m.GetDeployment(ctx, deploymentName)
// 	if err != nil {
// 		return fmt.Errorf("获取Deployment失败: %v", err)
// 	}

// 	fmt.Printf("成功获取到Deployment: %s\n", deployment.Name)

// 	// 更新Deployment的node affinity
// 	setNodeAffinity(deployment, targetNode)
// 	err = m.UpdateDeployment(ctx, deployment)
// 	if err != nil {
// 	return fmt.Errorf("更新Deployment失败: %v", err)
// }

func (m *Migrator) QeuryAppLatency(ctx context.Context, destination_service string) (float64, error) {
	logger := klog.FromContext(ctx)

	// 构建查询语句，获取特定服务过去一分钟的平均端到端延迟
	query := fmt.Sprintf(`avg_over_time(histogram_quantile(0.95, sum(rate(istio_request_duration_milliseconds_bucket{destination_service="%s"}[1m])) by (le))[1m:])`, destination_service)

	logger.V(1).Info("查询语句", "query", query)
	// 执行查询
	result, warnings, err := m.prometheusClient.Query(ctx, query, time.Now())
	if err != nil {
		return 0, fmt.Errorf("查询Prometheus失败: %v", err)
	}
	if len(warnings) > 0 {
		fmt.Printf("警告: %v", warnings)
	}

	// 解析查询结果
	vector, ok := result.(model.Vector)
	if !ok || vector.Len() == 0 {
		return 0, fmt.Errorf("未找到延迟数据")
	}

	// 获取延迟值（以毫秒为单位）
	latency := float64(vector[0].Value)

	logger.V(1).Info("服务 %s 的95百分位端到端延迟为 %.2f 毫秒", m.appLabel, latency)

	return latency, nil
}

func (m *Migrator) UpdateDeployment(ctx context.Context, deployment *v1apps.Deployment) error {
	_, err := m.clientset.AppsV1().Deployments(m.namespace).Update(ctx, deployment, typev1.UpdateOptions{})
	return err
}

func (m *Migrator) GetDeployment(ctx context.Context, deploymentName string) (*v1apps.Deployment, error) {
	return m.clientset.AppsV1().Deployments(m.namespace).Get(ctx, deploymentName, typev1.GetOptions{})
}

func (m *Migrator) GetPods(ctx context.Context) (*v1core.PodList, error) {
	return m.clientset.CoreV1().Pods(m.namespace).List(ctx, typev1.ListOptions{LabelSelector: m.appLabel})
}

func (m *Migrator) GetAvailableNodesForPod(ctx context.Context, pod *v1core.Pod) ([]string, error) {
	nodes, err := m.clientset.CoreV1().Nodes().List(ctx, typev1.ListOptions{})
	if err != nil {
		return nil, fmt.Errorf("获取节点失败: %v", err)
	}
	var nodeInfos []*framework.NodeInfo
	for _, node := range nodes.Items {
		nodeInfo := framework.NewNodeInfo()
		nodeInfo.SetNode(&node)
		nodeInfos = append(nodeInfos, nodeInfo)
	}

	res := []string{}
	// 遍历所有节点，检查Pod是否可以调度到该节点
	for _, node := range nodeInfos {
		// 使用filterPodByResource函数检查Pod是否可以调度到该节点
		available_nodes, err := filterNodesByResources(pod, nodeInfos)
		if err != nil {
			fmt.Printf("  - %s\n", node.Node().Name)
		}
		for _, node := range available_nodes {
			res = append(res, node.Node().Name)
		}
	}
	return res, nil
}
