package main

import (
	"fmt"
	"os"
	"strconv"
	"time"
)

type Config struct {
	URL            string
	ServiceName    string
	Lookback       string
	Limit          int
	PollingPeriod  time.Duration
	Alpha          float64
	KubeConfigPath string
	PrometheusAddr string
	AppLabel       string
	GatewayService string
	Namespace      string
	QosThreshold   float64
}

func ConfigFromEnv() (*Config, error) {
	config := &Config{}

	// 加载基础配置
	config.URL = os.Getenv("JAEGER_URL")
	config.ServiceName = os.Getenv("SERVICE_NAME")
	config.Lookback = os.Getenv("LOOKBACK")

	// Limit 参数转换为整数
	limit, err := strconv.Atoi(os.Getenv("LIMIT"))
	if err != nil {
		return nil, fmt.Errorf("invalid LIMIT value: %w", err)
	}
	config.Limit = limit

	// PollingPeriod 转换为 time.Duration
	pollingPeriod, err := time.ParseDuration(os.Getenv("POLLING_PERIOD"))
	if err != nil {
		return nil, fmt.Errorf("invalid POLLING_PERIOD value: %w", err)
	}
	config.PollingPeriod = pollingPeriod

	// Alpha 转换为 float64
	alpha, err := strconv.ParseFloat(os.Getenv("ALPHA"), 64)
	if err != nil {
		return nil, fmt.Errorf("invalid ALPHA value: %w", err)
	}
	config.Alpha = alpha

	// 加载 Kubernetes 和 Prometheus 相关配置
	config.KubeConfigPath = os.Getenv("KUBECONFIG")
	config.PrometheusAddr = os.Getenv("PROMETHEUS_ADDR")
	config.AppLabel = os.Getenv("APP_LABEL")
	config.GatewayService = os.Getenv("GATEWAY_SERVICE")
	config.Namespace = os.Getenv("NAMESPACE")

	// QosThreshold 转换为整数
	qosThreshold, err := strconv.ParseFloat(os.Getenv("QOS_THRESHOLD"), 64)
	if err != nil {
		return nil, fmt.Errorf("invalid QOS_THRESHOLD value: %w", err)
	}
	config.QosThreshold = qosThreshold

	return config, nil
}
