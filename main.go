package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/joho/godotenv"
	de "github.com/paprikaw/rscheduler/pkg/dataexporter"
	m "github.com/paprikaw/rscheduler/pkg/migrator"
	d "github.com/paprikaw/rscheduler/pkg/model"
	q "github.com/paprikaw/rscheduler/pkg/queryclient"
	"k8s.io/klog/v2"
)

var startWaitingTime = 30 * time.Second
var endWaitingTime = 15 * time.Second
var latestTraceTimestamp int64 = 0

type FailedToGetValidTraceError struct {
	err error
}

func (e FailedToGetValidTraceError) Error() string {
	return e.err.Error()
}

type LatencyData struct {
	client_latency     float64
	aggregator_latency float64
	detection_latency  float64
	ml_latency         float64
	db_latency         float64
}

type EstimatedRT struct {
	mu    sync.RWMutex
	value LatencyData
}

func (e *EstimatedRT) Get() LatencyData {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.value
}

func (e *EstimatedRT) Set(val LatencyData) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.value = val
}

var estimatedRT = &EstimatedRT{value: LatencyData{}}

func getValidTrace(ctx context.Context, config *Config) (LatencyData, error) {
	logger := klog.FromContext(ctx)
	retries := 0
	var averageLatency LatencyData
	for retries < config.MaxPodWaitingRetries {
		result, err := getLatestTrace(ctx, config, 200, 5, 10)
		if err != nil {
			logger.Error(err, "Failed to get immediate trace")
			return LatencyData{}, err
		}

		if result.client_latency != 0 {
			averageLatency = result
			break
		}
		time.Sleep(config.PollingPeriod)
		retries++
	}
	if averageLatency.client_latency == 0 {
		err := FailedToGetValidTraceError{err: errors.New("not enough valid trace data")}
		logger.Error(err, "Not enough valid trace data")
		return LatencyData{}, err
	}
	return averageLatency, nil
}

func waitForPodReady(ctx context.Context, config *Config, queryClient *q.QueryClient) error {
	logger := klog.FromContext(ctx)
	isPodReady := false
	retries := 0
	for retries < config.MaxPodWaitingRetries {
		ready, err := queryClient.CheckDeploymentsPodsReady(ctx, config.Namespace, config.AppLabel)
		if err != nil {
			logger.Error(err, "无法获取 Pods 列表")
			return err
		}
		if ready {
			isPodReady = true
			logger.V(1).Info("Pods are ready")
			break
		}
		logger.V(1).Info("Waiting for pods to be ready", "retries", retries)
		time.Sleep(config.PollingPeriod)
		retries++
	}
	if !isPodReady {
		logger.V(1).Info("Pods are probabily undeployable not ready")
		return errors.New("pods are probabily undeployable not ready")
	}
	return nil
}

func recordBestEffortData(ctx context.Context, config *Config, queryClient *q.QueryClient, dataExporter *de.DataExporter) (LatencyData, error) {
	logger := klog.FromContext(ctx)
	err := waitForPodReady(ctx, config, queryClient)
	if err != nil {
		return LatencyData{}, err
	}
	time.Sleep(startWaitingTime)
	averageLatency, err := getValidTrace(ctx, config)
	if err != nil {
		return LatencyData{}, err
	}
	logger.V(1).Info("Best effort data recorded", "client_latency", averageLatency.client_latency, "aggregator_latency", averageLatency.aggregator_latency, "detection_latency", averageLatency.detection_latency, "ml_latency", averageLatency.ml_latency, "db_latency", averageLatency.db_latency)
	dataExporter.WriteBE(averageLatency.client_latency, averageLatency.aggregator_latency, averageLatency.detection_latency, averageLatency.ml_latency, averageLatency.db_latency)
	return averageLatency, nil
}
func startOneReschedulingEpisode(ctx context.Context, config *Config, migrator *m.Migrator, httpClient *http.Client, queryClient *q.QueryClient, replicaCnt int, dataExporter *de.DataExporter) error {
	logger := klog.FromContext(ctx)
	err := waitForPodReady(ctx, config, queryClient)
	if err != nil {
		return err
	}
	time.Sleep(startWaitingTime)
	startAverageLatency, err := getValidTrace(ctx, config)
	if err != nil {
		return err
	}
	step := 0
	for step < config.MaxStep {
		logger.V(1).Info("----------Rescheduling process starts----------")
		response, err := GetMigrationResult(ctx, config, queryClient, httpClient, migrator)
		if err != nil {
			logger.Error(err, "Failed to get migration result")
			continue
		}
		if response.IsStop {
			logger.V(1).Info("Stop the migration")
			break
		}
		err = migrator.MigratePod(ctx, config.PollingPeriod, config.Namespace, response.PodName, config.AppLabel, response.TargetNode, int32(replicaCnt))
		if err != nil {
			logger.Error(err, "Failed to migrate Pod")
			continue
		}
		step++
	}
	time.Sleep(endWaitingTime)
	endAverageLatency, err := getValidTrace(ctx, config)
	if err != nil {
		return err
	}
	dataExporter.WriteRL(
		startAverageLatency.client_latency,
		endAverageLatency.client_latency,
		startAverageLatency.aggregator_latency,
		endAverageLatency.aggregator_latency,
		startAverageLatency.detection_latency,
		endAverageLatency.detection_latency,
		startAverageLatency.ml_latency,
		endAverageLatency.ml_latency,
		startAverageLatency.db_latency,
		endAverageLatency.db_latency,
		step,
	)
	return nil
}

func monitor(ctx context.Context, config *Config, migrator *m.Migrator, httpClient *http.Client, queryClient *q.QueryClient, replicaCnt int) {
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
			if latency.client_latency > config.QosThreshold {
				logger.V(1).Info("Current latency", "latency", latency.client_latency)
				response, err := GetMigrationResult(ctx, config, queryClient, httpClient, migrator)
				if err != nil {
					logger.Error(err, "Failed to get migration result")
					continue
				}
				if response.IsStop {
					logger.V(1).Info("Skip this migration")
					continue
				}

				err = migrator.MigratePod(ctx, config.PollingPeriod, config.Namespace, response.PodName, config.AppLabel, response.TargetNode, int32(replicaCnt))
				if err != nil {
					logger.Error(err, "Failed to migrate Pod")
					continue
				}
			} else {
				logger.V(1).Info("Current latency is within the threshold", "latency", latency.client_latency)
			}
		case <-ctx.Done():
			return
		}
	}
}

func getLatestTrace(ctx context.Context, config *Config, maximumTraceCount int, minimumValidTraceCount int, timeSpan int) (LatencyData, error) {
	curTime := time.Now()
	logger := klog.FromContext(ctx)
	// 构建请求 URL
	url := ""
	url = fmt.Sprintf("%s?service=%s&limit=%d", config.URL, config.ServiceName, maximumTraceCount)

	// 发送 HTTP 请求
	resp, err := http.Get(url)
	if err != nil {
		logger.Error(err, "Error fetching trace")
		return LatencyData{}, err
	}
	defer resp.Body.Close()

	// 读取响应数据
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		logger.Error(err, "Error reading response body")
		return LatencyData{}, err
	}
	// 解析 JSON 响应
	var traceResponse d.TraceResponse
	err = json.Unmarshal(body, &traceResponse)
	if err != nil {
		log.Println("Error parsing JSON:", err)
		return LatencyData{}, err
	}

	// 检查是否有数据
	if len(traceResponse.Data) == 0 || len(traceResponse.Data[0].Spans) == 0 {
		log.Println("No trace data found")
		return LatencyData{}, err
	}
	var latencySum LatencyData
	totalValidTraceCount := 0
	for i := len(traceResponse.Data) - 1; i >= 0; i-- {
		// 获取最新的 trace 数据
		latestTrace := traceResponse.Data[i]
		traceStartTime := latestTrace.Spans[0].StartTime
		traceStartTimeFormatted := time.Unix(0, traceStartTime*int64(time.Microsecond)).Format(time.RFC3339)
		// 如果 trace 是旧的，则跳过
		if traceStartTime < curTime.Add(-time.Duration(timeSpan)*time.Second).UnixMicro() {
			logger.V(3).Info(fmt.Sprintf("Trace is not within %d seconds. Trace start time: %s", timeSpan, traceStartTimeFormatted))
			continue
		}
		logger.V(3).Info(fmt.Sprintf("New trace found. Trace start time: %s", traceStartTimeFormatted))
		// 初始化 Latencies 结构体
		var latencies LatencyData
		traceCount := 0
		// 遍历所有 spans，提取延迟信息
		for _, span := range latestTrace.Spans {
			// Extract span.kind from tags
			spanKind := ""
			isString := false
			for _, tag := range span.Tags {
				if tag.Key == "span.kind" && tag.Type == "string" {
					// Perform type assertion
					if val, ok := tag.Value.(string); ok {
						spanKind = val
						isString = true
					} else {
						logger.V(2).Info("span.kind is not a string", "value", tag.Value)
					}
					break
				}
			}
			if !isString {
				continue
			}
			// 根据服务名和 span 类型来计算延迟
			operation := span.OperationName
			durationMs := float64(span.Duration) / 1000.0 // 将微秒转换为毫秒

			switch {
			// 对于 client 服务，计算 client 类型 span 的延迟
			case strings.Contains(operation, "client.default.svc.cluster.local") && spanKind == "client":
				latencies.client_latency = durationMs
				traceCount++
			// 对于其他服务，计算 server 类型 span 的延迟
			case strings.Contains(operation, "aggregator.default.svc.cluster.local") && spanKind == "server":
				latencies.aggregator_latency = durationMs
				traceCount++
			case strings.Contains(operation, "detection.default.svc.cluster.local") && spanKind == "server":
				latencies.detection_latency = durationMs
				traceCount++
			case strings.Contains(operation, "machine-learning.default.svc.cluster.local") && spanKind == "server":
				latencies.ml_latency = durationMs
				traceCount++
			case strings.Contains(operation, "db.default.svc.cluster.local") && spanKind == "server":
				latencies.db_latency = durationMs
				traceCount++
			}
		}
		if traceCount < 5 {
			logger.V(3).Info("Not enough trace data", "traceCount", traceCount)
			continue
		}
		latencySum.client_latency += latencies.client_latency
		latencySum.aggregator_latency += latencies.aggregator_latency
		latencySum.detection_latency += latencies.detection_latency
		latencySum.ml_latency += latencies.ml_latency
		latencySum.db_latency += latencies.db_latency
		totalValidTraceCount++
		latencies = LatencyData{}
	}
	if totalValidTraceCount < minimumValidTraceCount {
		logger.V(2).Info("Not enough valid trace data", "totalValidTraceCount", totalValidTraceCount)
		return LatencyData{}, nil
	}
	latencySum.client_latency /= float64(totalValidTraceCount)
	latencySum.aggregator_latency /= float64(totalValidTraceCount)
	latencySum.detection_latency /= float64(totalValidTraceCount)
	latencySum.ml_latency /= float64(totalValidTraceCount)
	latencySum.db_latency /= float64(totalValidTraceCount)
	return latencySum, nil
}
func updateTrace(ctx context.Context, config *Config) {
	logger := klog.FromContext(ctx)
	// 构建请求 URL
	url := ""
	url = fmt.Sprintf("%s?service=%s&limit=%d&lookback=%s", config.URL, config.ServiceName, config.Limit, config.Lookback)

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

	for i := len(traceResponse.Data) - 1; i >= 0; i-- {
		// 获取最新的 trace 数据
		latestTrace := traceResponse.Data[i]
		traceStartTime := latestTrace.Spans[0].StartTime
		traceStartTimeFormatted := time.Unix(0, traceStartTime*int64(time.Microsecond)).Format(time.RFC3339)
		// 如果 trace 是旧的，则跳过
		if traceStartTime < time.Now().Add(-10*time.Second).UnixMicro() {
			logger.V(2).Info(fmt.Sprintf("Trace is not within 10 seconds. Trace start time: %s", traceStartTimeFormatted))
			continue
		}
		// trace是旧的
		latestTraceTimestampFormatted := time.Unix(0, latestTraceTimestamp*int64(time.Microsecond)).Format(time.RFC3339)
		if traceStartTime <= latestTraceTimestamp {
			logger.V(2).Info(fmt.Sprintf("Trace is outdated. Trace start time: %s, Latest trace timestamp: %s", traceStartTimeFormatted, latestTraceTimestampFormatted))
			continue
		}
		latestTraceTimestamp = traceStartTime
		logger.V(2).Info(fmt.Sprintf("New trace found. Trace start time: %s", traceStartTimeFormatted))
		// 初始化 Latencies 结构体
		var latencies LatencyData
		traceCount := 0
		// 遍历所有 spans，提取延迟信息
		for _, span := range latestTrace.Spans {
			// Extract span.kind from tags
			spanKind := ""
			isString := false
			for _, tag := range span.Tags {
				if tag.Key == "span.kind" && tag.Type == "string" {
					// Perform type assertion
					if val, ok := tag.Value.(string); ok {
						spanKind = val
						isString = true
					} else {
						logger.V(2).Info("span.kind is not a string", "value", tag.Value)
					}
					break
				}
			}
			if !isString {
				continue
			}
			// 根据服务名和 span 类型来计算延迟
			operation := span.OperationName
			durationMs := float64(span.Duration) / 1000.0 // 将微秒转换为毫秒

			switch {
			// 对于 client 服务，计算 client 类型 span 的延迟
			case strings.Contains(operation, "client.default.svc.cluster.local") && spanKind == "client":
				latencies.client_latency = durationMs
				traceCount++
			// 对于其他服务，计算 server 类型 span 的延迟
			case strings.Contains(operation, "aggregator.default.svc.cluster.local") && spanKind == "server":
				latencies.aggregator_latency = durationMs
				traceCount++
			case strings.Contains(operation, "detection.default.svc.cluster.local") && spanKind == "server":
				latencies.detection_latency = durationMs
				traceCount++
			case strings.Contains(operation, "machine-learning.default.svc.cluster.local") && spanKind == "server":
				latencies.ml_latency = durationMs
				traceCount++
			case strings.Contains(operation, "db.default.svc.cluster.local") && spanKind == "server":
				latencies.db_latency = durationMs
				traceCount++
			}
		}
		if traceCount < 5 {
			logger.V(2).Info("Not enough trace data", "traceCount", traceCount)
			continue
		}
		// 获取当前的 estimatedRT
		currentRT := estimatedRT.Get()

		// 更新移动平均值的函数
		updateLatency := func(current, sample float64, alpha float64) float64 {
			if current == 0 {
				return sample
			}
			return (1-alpha)*current + alpha*sample
		}

		// 计算并更新每个延迟的移动平均值
		currentRT.client_latency = updateLatency(currentRT.client_latency, latencies.client_latency, config.Alpha)
		currentRT.aggregator_latency = updateLatency(currentRT.aggregator_latency, latencies.aggregator_latency, config.Alpha)
		currentRT.detection_latency = updateLatency(currentRT.detection_latency, latencies.detection_latency, config.Alpha)
		currentRT.ml_latency = updateLatency(currentRT.ml_latency, latencies.ml_latency, config.Alpha)
		currentRT.db_latency = updateLatency(currentRT.db_latency, latencies.db_latency, config.Alpha)

		// 更新 estimatedRT
		estimatedRT.Set(currentRT)
		logger.V(1).Info(fmt.Sprintf("client_latency: %.2f, aggregator_latency: %.2f, detection_latency: %.2f, ml_latency: %.2f, db_latency: %.2f", currentRT.client_latency, currentRT.aggregator_latency, currentRT.detection_latency, currentRT.ml_latency, currentRT.db_latency))
	}
}

func main() {
	// Initialize klog
	klog.InitFlags(nil)
	is_tester := flag.Bool("test", false, "Whether to run in test mode")
	strategy := flag.String("strategy", "rl", "The strategy of the rescheduling")
	replica_cnt := flag.Int("replicaCnt", 2, "The number of replicas")
	output_file := flag.String("output", "", "The output file")
	flag.Parse()
	// 创建data exporter
	dataExporter := de.NewDataExporter(*output_file, *strategy == "rl")
	defer dataExporter.Close()

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
	if !*is_tester {
		go func() {
			for {
				updateTrace(ctx, config)
				time.Sleep(config.PollingPeriod)
			}
		}()
	}
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
		if *strategy == "rl" {
			startOneReschedulingEpisode(ctx, config, migrator, httpClient, queryClient, *replica_cnt, dataExporter)
		} else if *strategy == "be" {
			recordBestEffortData(ctx, config, queryClient, dataExporter)
		}
	} else {
		// Start Monitoring Loop
		monitor(ctx, config, migrator, httpClient, queryClient, *replica_cnt)
	}
}
