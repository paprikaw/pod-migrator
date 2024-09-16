package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/joho/godotenv"
	"k8s.io/klog/v2"
)

func monitor(ctx context.Context, migrator *Migrator) {
	// 导入所需的包
	interval := 3 * time.Second // 设置监控间隔为5分钟
	logger := klog.FromContext(ctx)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			// 执行Prometheus查询
			latency, err := migrator.QeuryAppLatency(ctx, "client.default.svc.cluster.local")
			if err != nil {
				logger.Error(err, "查询应用延迟失败")
				continue
			}
			logger.V(0).Info("当前应用延迟", "latency", latency)
		case <-ctx.Done():
			return
		}
	}
}
func main() {
	// 初始化klog
	klog.InitFlags(nil)
	flag.Parse()
	logger := klog.NewKlogr()
	ctx := klog.NewContext(context.Background(), logger)
	err := godotenv.Load()
	if err != nil {
		logger.Error(err, "Error loading .env file")
	}

	// 加载集群配置，初始化kubernetes client
	kubeconfig := os.Getenv("KUBECONFIG")
	prometheusAddr := os.Getenv("PROMETHEUS_ADDR")
	appLabel := os.Getenv("APP_LABEL")
	gatewayService := os.Getenv("GATEWAY_SERVICE")
	namespace := os.Getenv("NAMESPACE")
	migrator, err := NewMigrator(kubeconfig, prometheusAddr, appLabel, gatewayService, namespace)
	if err != nil {
		fmt.Println("Error creating migrator:", err)
		return
	}

	// monitor(ctx, migrator)
	oldPodName := "detection-7f78df9988-qmwp7"
	// err = migrator.MigratePod(ctx, "detection", oldPodName, "tb-cloud-vm1")
	err = migrator.MigratePod(ctx, "detection", oldPodName, "tb-edge-vm1")
	if err != nil {
		fmt.Print(err, "Error migrating pod")
		return
	}
	// // 获取名为client的deployment
	// deployment, err := migrator.GetDeployment(context.TODO(), "client")
	// if err != nil {
	// 	fmt.Printf("获取client deployment失败: %v\n", err)
	// 	return
	// }

	// // 修改node affinity
	// setNodeAffinity(deployment, "tb-edge-vm2")

	// // 更新deployment
	// if err != nil {
	// 	fmt.Printf("更新client deployment失败: %v\n", err)
	// 	return
	// }

	// fmt.Println("成功修改client deployment的node affinity")
	// 驱逐名为client-75c8859b-hwr5r的Pod
	// evictionPolicy := metav1.DeletePropagationForeground
	// err = migrator.clientset.CoreV1().Pods("default").Evict(context.TODO(), &v1beta1.Eviction{
	// 	ObjectMeta: metav1.ObjectMeta{
	// 		Name:      "client-75c8859b-hwr5r",
	// 		Namespace: "default",
	// 	},
	// 	DeleteOptions: &metav1.DeleteOptions{
	// 		PropagationPolicy: &evictionPolicy,
	// 	},
	// })
	// if err != nil {
	// 	fmt.Printf("驱逐Pod app-1失败: %v\n", err)
	// 	return
	// }
	// fmt.Println("成功驱逐Pod app-1")

	// // 创建 kubernetes client
	// clientset, err := kubernetes.NewForConfig(config)
	// if err != nil {
	// 	fmt.Println("Error creating kubernetes client:", err)
	// 	return
	// }

	// // 获取所有特定镜像的 Pod
	// podList, err := migrator.clientset.CoreV1().Pods("").List(context.TODO(), metav1.ListOptions{LabelSelector: "deployment=mubench"})
	// if err != nil {
	// 	fmt.Println("Error listing pods:", err)
	// 	return
	// }

	// // 获取所有节点
	// nodeList, err := migrator.clientset.CoreV1().Nodes().List(context.TODO(), metav1.ListOptions{})
	// if err != nil {
	// 	fmt.Println("Error listing nodes:", err)
	// 	return
	// }

	// // 遍历所有Pod
	// for _, pod := range podList.Items {
	// 	fmt.Printf("Pod %s 可以部署的节点:\n", pod.Name)

}
