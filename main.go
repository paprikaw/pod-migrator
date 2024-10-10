package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/joho/godotenv"
	m "github.com/paprikaw/rscheduler/pkg/migrator"
	d "github.com/paprikaw/rscheduler/pkg/model"
	q "github.com/paprikaw/rscheduler/pkg/queryclient"
	"k8s.io/klog/v2"
)

var latestTraceTimestamp int64 = 0
var monitorStartTime time.Time = time.Now()
var maxPodWaitingRetries = 100
var replicaCnt int32 = 2

type EstimatedRT struct {
	mu    sync.RWMutex
	value float64
}

func (e *EstimatedRT) Get() float64 {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.value
}

func (e *EstimatedRT) Set(val float64) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.value = val
}

var estimatedRT = &EstimatedRT{value: 0.0}
var startWaitingTime = 10 * time.Second
var endWaitingTime = 10 * time.Second

func startOneReschedulingEpisodeBestEffort(ctx context.Context, config *Config, migrator *m.Migrator, httpClient *http.Client, queryClient *q.QueryClient, maxStep int) error {
	logger := klog.FromContext(ctx)
	retries := 0
	for retries < maxPodWaitingRetries {
		ready, err := queryClient.CheckDeploymentsPodsReady(ctx, config.Namespace, config.AppLabel)
		if err != nil {
			logger.Error(err, "无法获取 Pods 列表")
			return err
		}
		if ready {
			logger.V(1).Info("Pods are ready")
			break
		}
		logger.V(1).Info("Waiting for pods to be ready", "retries", retries)
		time.Sleep(1 * time.Second)
		retries++
	}
	estimatedRT.Set(0)
	time.Sleep(startWaitingTime)
	start_latency := estimatedRT.Get()
	step := 0
	for step < maxStep {
		latency := estimatedRT.Get()
		logger.V(1).Info("----------Rescheduling process starts----------")
		logger.V(1).Info("Current latency", "latency", latency)
		response, err := GetMigrationResult(ctx, config, queryClient, httpClient, migrator, latency)
		if err != nil {
			logger.Error(err, "Failed to get migration result")
			continue
		}
		if response.IsStop {
			logger.V(1).Info("Stop the migration")
			break
		}
		err = migrator.MigratePod(ctx, config.Namespace, response.PodName, config.AppLabel, response.TargetNode, replicaCnt)
		if err != nil {
			logger.Error(err, "Failed to migrate Pod")
			continue
		}
		step++
	}
	estimatedRT.Set(0)
	time.Sleep(endWaitingTime)
	logger.V(0).Info(fmt.Sprintf("%.2f %.2f %d", start_latency, estimatedRT.Get(), step))
	return nil
}

func monitor(ctx context.Context, config *Config, migrator *m.Migrator, httpClient *http.Client, queryClient *q.QueryClient) {
	// Import necessary packages
	interval := 10 * time.Second // Set monitoring interval to 5 minutes
	logger := klog.FromContext(ctx)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		ticker.Reset(interval)
		select {
		case <-ticker.C:
			latency := estimatedRT.Get()
			logger.V(1).Info("----------Monitoring process starts----------")
			if latency > config.QosThreshold {
				logger.V(1).Info("Current latency", "latency", latency)
				response, err := GetMigrationResult(ctx, config, queryClient, httpClient, migrator, latency)
				if err != nil {
					logger.Error(err, "Failed to get migration result")
					continue
				}
				if response.IsStop {
					logger.V(1).Info("Skip this migration")
					continue
				}

				err = migrator.MigratePod(ctx, config.Namespace, response.PodName, config.AppLabel, response.TargetNode, replicaCnt)
				if err != nil {
					logger.Error(err, "Failed to migrate Pod")
					continue
				}
			} else {
				logger.V(1).Info("Current latency is within the threshold", "latency", latency)
			}
		case <-ctx.Done():
			return
		}
	}
}

func getLatestTrace(ctx context.Context, config *Config) {
	logger := klog.FromContext(ctx)
	// Build the request URL with parameters
	url := fmt.Sprintf("%s?service=%s&limit=%d&lookback=%s", config.URL, config.ServiceName, config.Limit, config.Lookback)

	// 发送 HTTP 请求
	resp, err := http.Get(url)
	if err != nil {
		logger.Error(err, "Error fetching trace")
		return
	}
	defer resp.Body.Close()

	// 读取响应数据
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		logger.Error(err, "Error reading response body")
		return
	}

	// 解析 JSON 响应
	var traceResponse d.TraceResponse
	err = json.Unmarshal(body, &traceResponse)
	if err != nil {
		log.Println("Error parsing JSON:", err)
		return
	}

	// 检查是否有数据
	if len(traceResponse.Data) == 0 || len(traceResponse.Data[0].Spans) == 0 {
		log.Println("No trace data found")
		return
	}

	// 获取最新的 trace 数据
	latestTrace := traceResponse.Data[0]
	traceStartTime := latestTrace.Spans[0].StartTime
	traceDuration := latestTrace.Spans[0].Duration

	// 如果 trace 是旧的，则跳过
	if traceStartTime/1000 < monitorStartTime.UnixMilli() || traceStartTime < time.Now().UnixMilli()-10*time.Second.Milliseconds() {
		logger.V(3).Info(fmt.Sprintf("Trace is outdated. Trace start time: %d, Monitor start time: %d", traceStartTime, monitorStartTime.UnixMilli()))
		return
	}

	// 如果该 trace 是新的，则更新移动平均值
	if traceStartTime > latestTraceTimestamp {
		latestTraceTimestamp = traceStartTime
		// 将新的 Sample RT 加入移动平均计算中
		sampleRT := float64(traceDuration) / 1000.0 // 将微秒转换为毫秒
		result := 0.0
		if estimatedRT.Get() == 0 {
			result = sampleRT
		} else {
			result = (1-config.Alpha)*estimatedRT.Get() + config.Alpha*sampleRT
		}
		logger.V(2).Info(fmt.Sprintf("New trace received. Duration: %.2f ms, Updated Estimated RT: %.2f ms", sampleRT, result))
		estimatedRT.Set(result)
	}
}
func main() {
	// Initialize klog
	klog.InitFlags(nil)
	is_tester := flag.Bool("test", false, "Whether to run in test mode")
	max_step := flag.Int("max_step", 15, "Maximum number of steps to run")
	flag.Parse()
	logger := klog.NewKlogr()
	ctx := klog.NewContext(context.Background(), logger)

	// Load cluster configuration and initialize kubernetes client
	err := godotenv.Load()
	if err != nil {
		fmt.Println("Error loading .env:", err)
		return
	}
	config, err := ConfigFromEnv()
	if err != nil {
		fmt.Println("Error loading config:", err)
		return
	}
	go func() {
		for {
			getLatestTrace(ctx, config)
			time.Sleep(config.PollingPeriod)
		}
	}()
	migrator, err := m.NewMigrator(config.KubeConfigPath)
	if err != nil {
		fmt.Println("Error creating migrator:", err)
		return
	}

	httpClient := &http.Client{
		Timeout: 10 * time.Second,
	}
	queryClient, err := q.NewQueryClient(config.PrometheusAddr, config.KubeConfigPath)
	if err != nil {
		fmt.Println("Error creating query client:", err)
		return
	}
	if *is_tester {
		// Start Monitoring Loop
		startOneReschedulingEpisodeBestEffort(ctx, config, migrator, httpClient, queryClient, *max_step)
	} else {
		// Start Monitoring Loop
		monitor(ctx, config, migrator, httpClient, queryClient)
	}

}
