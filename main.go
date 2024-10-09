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
			logger.V(0).Info("----------Monitoring process starts----------")
			if latency > config.QosThreshold {
				logger.V(0).Info("Current latency", "latency", latency)
				response, err := GetMigrationResult(ctx, config, queryClient, httpClient, migrator, latency)
				if err != nil {
					logger.Error(err, "Failed to get migration result")
					continue
				}
				if response.IsStop {
					logger.V(0).Info("Skip this migration")
					continue
				}

				err = migrator.MigratePod(ctx, config.Namespace, response.PodName, config.AppLabel, response.TargetNode)
				if err != nil {
					logger.Error(err, "Failed to migrate Pod")
					continue
				}
			} else {
				logger.V(0).Info("Current latency is within the threshold", "latency", latency)
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
	if traceStartTime/1000 < monitorStartTime.UnixMilli() {
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
		logger.V(3).Info(fmt.Sprintf("New trace received. Duration: %.2f ms, Updated Estimated RT: %.2f ms", sampleRT, result))
		estimatedRT.Set(result)
	}
}
func main() {
	// Initialize klog
	klog.InitFlags(nil)
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
	// Start Monitoring Loop
	monitor(ctx, config, migrator, httpClient, queryClient)
}
