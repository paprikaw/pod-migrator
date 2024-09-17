package main

import (
	"context"
	"fmt"
	"time"

	promapi "github.com/prometheus/client_golang/api"
	v1prom "github.com/prometheus/client_golang/api/prometheus/v1"
	"github.com/prometheus/common/model"
	"k8s.io/klog/v2"
)

type ProClient struct {
	prometheusClient v1prom.API
}

func NewProClient(Addr string) (*ProClient, error) {
	client, err := promapi.NewClient(promapi.Config{
		Address: Addr,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create Prometheus client: %v", err)
	}

	prometheusClient := v1prom.NewAPI(client)
	return &ProClient{prometheusClient: prometheusClient}, nil
}

func (c *ProClient) QeuryAppLatencyByMilli(ctx context.Context, destination_service string) (int64, error) {
	logger := klog.FromContext(ctx)

	// Construct query to get average end-to-end latency for a specific service over the past minute
	query := fmt.Sprintf(`sum by (app_name) (increase(mub_request_processing_latency_milliseconds_sum{app_name="%s"}[2m])) / sum by (app_name) (increase(mub_request_processing_latency_milliseconds_count{app_name="%s"}[2m]))`, destination_service, destination_service)

	logger.V(5).Info("query statement", "query", query)
	// Execute query
	result, warnings, err := c.prometheusClient.Query(ctx, query, time.Now())
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

	logger.V(5).Info("95th percentile end-to-end latency for service %s is %.2f milliseconds", destination_service, latency)

	return latency, nil
}

func (c *ProClient) QueryNodeBandwidthByMBytes(ctx context.Context, nodeAddress string) (int64, error) {
	logger := klog.FromContext(ctx)

	// 构造查询语句，获取指定节点eth0接口的带宽使用量（以字节/秒为单位）
	query := fmt.Sprintf(`sum(rate(node_network_receive_bytes_total{device="eth0",instance="%s"}[1m]) + rate(node_network_transmit_bytes_total{device="eth0",instance="%s"}[1m]))`, nodeAddress, nodeAddress)

	logger.V(2).Info("查询语句", "query", query)

	// 执行查询
	result, warnings, err := c.prometheusClient.Query(ctx, query, time.Now())
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

	logger.V(1).Info("节点带宽使用量", "node", nodeAddress, "bandwidth", fmt.Sprintf("%d MBps", bandwidthMbps))

	return bandwidthMbps, nil
}
