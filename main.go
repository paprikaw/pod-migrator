package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/joho/godotenv"
	"k8s.io/klog/v2"
)

func getRequst(ctx context.Context, migrator *Migrator) (*Request, error) {
	clusterState, err := migrator.GetClusterState(ctx)
	if err != nil {
		return nil, err
	}
	podDeployable, err := migrator.GetPodsAvailableNodes(ctx)
	if err != nil {
		return nil, err
	}
	request := Request{
		ClusterState:  *clusterState,
		PodDeployable: podDeployable,
	}
	return &request, nil
}
func getMigrationResult(ctx context.Context, migrator *Migrator) (*Response, error) {
	logger := klog.FromContext(ctx)
	// 初始化http client
	httpClient := &http.Client{
		Timeout: time.Second * 10, // 设置10秒超时
	}
	logger.V(1).Info("HTTP客户端已初始化")
	request, err := getRequst(ctx, migrator)
	if err != nil {
		return nil, err
	}
	logger.V(1).Info("请求已生成", "request", request)
	jsonData, err := json.Marshal(request)
	if err != nil {
		logger.Error(err, "JSON编码失败")
		return nil, err
	}

	// 发送POST请求
	resp, err := httpClient.Post("http://localhost:5000/get_action", "application/json", bytes.NewBuffer(jsonData))
	if err != nil {
		logger.Error(err, "发送POST请求失败")
		return nil, err
	}

	defer resp.Body.Close()

	logger.V(1).Info("POST请求已发送", "状态码", resp.StatusCode)
	return nil, nil
}

func monitor(ctx context.Context, migrator *Migrator) {
	// 导入所需的包
	interval := 3 * time.Second // 设置监控间隔为5分钟
	logger := klog.FromContext(ctx)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	qosThreshold := migrator.qosThreshold
	for {
		select {
		case <-ticker.C:
			// 执行Prometheus查询
			latency, err := migrator.proClient.QeuryAppLatencyByMilli(ctx, "client")
			if err != nil {
				logger.Error(err, "查询应用延迟失败")
				continue
			}
			if latency > int64(qosThreshold) {
				getMigrationResult(ctx, migrator)
			}
			ticker.Reset(interval)
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
	qosThreshold, err := strconv.Atoi(os.Getenv("QOS_THRESHOLD"))
	if err != nil {
		logger.Error(err, "QOS_THRESHOLD 不是有效的整数")
		return
	}
	migrator, err := NewMigrator(kubeconfig, prometheusAddr, appLabel, gatewayService, namespace, qosThreshold)
	if err != nil {
		fmt.Println("Error creating migrator:", err)
		return
	}
	getMigrationResult(ctx, migrator)
	// // monitor(ctx, migrator)
	// oldPodName := "detection-7f78df9988-qmwp7"
	// // err = migrator.MigratePod(ctx, "detection", oldPodName, "tb-cloud-vm1")
	// err = migrator.MigratePod(ctx, "detection", oldPodName, "tb-edge-vm1")
	// if err != nil {
	// 	fmt.Print(err, "Error migrating pod")
	// 	return
	// }
}
