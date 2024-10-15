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

var startWaitingTime = 60 * time.Second
var endWaitingTime = 20 * time.Second

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

// Estimated Moving Average
type EstimatedMA struct {
	mu      sync.RWMutex
	latency float64
}

func (e *EstimatedMA) Get() float64 {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.latency
}

func (e *EstimatedMA) Set(val float64) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.latency = val
}

var estimatedRT = &EstimatedMA{}

func waitForValidTraces(ctx context.Context, config *Config, maximumTraceCount int, minimumValidTraceCount int) (LatencyData, error) {
	logger := klog.FromContext(ctx)
	retries := 0
	var averageLatency LatencyData
	for retries < config.MaxPodWaitingRetries {
		retries++
		result, _, err := getValidTraces(ctx, config, maximumTraceCount, minimumValidTraceCount, true, time.Time{})
		if err != nil {
			logger.Error(err, "Failed to get immediate trace")
			return LatencyData{}, err
		}
		if len(result) < minimumValidTraceCount {
			logger.V(2).Info("Not enough valid trace data", "retries", retries)
			time.Sleep(config.PollingPeriod)
			continue
		}
		latencySum := LatencyData{}
		for _, latency := range result {
			latencySum.client_latency += latency.client_latency
			latencySum.aggregator_latency += latency.aggregator_latency
			latencySum.detection_latency += latency.detection_latency
			latencySum.ml_latency += latency.ml_latency
			latencySum.db_latency += latency.db_latency
		}
		averageLatency.client_latency = latencySum.client_latency / float64(len(result))
		averageLatency.aggregator_latency = latencySum.aggregator_latency / float64(len(result))
		averageLatency.detection_latency = latencySum.detection_latency / float64(len(result))
		averageLatency.ml_latency = latencySum.ml_latency / float64(len(result))
		averageLatency.db_latency = latencySum.db_latency / float64(len(result))
		return averageLatency, nil
	}
	if averageLatency.client_latency == 0 {
		err := FailedToGetValidTraceError{err: errors.New("not enough valid trace data")}
		logger.Error(err, "Not enough valid trace data")
		return LatencyData{}, err
	}
	return averageLatency, nil
}

//	func getLatestValidTracesAvgLatency(ctx context.Context, config *Config, maximumTraceCount int, minimumValidTraceCount int, timeSpan int) (LatencyData, error) {
//		latencies, err := getValidTraces(ctx, config, maximumTraceCount, timeSpan)
//		if err != nil {
//			return LatencyData{}, err
//		}
//		if len(latencies) < minimumValidTraceCount {
//			return LatencyData{}, errors.New("not enough valid trace data")
//		}
//		latencySum := LatencyData{}
//		for _, latency := range latencies {
//			latencySum.client_latency += latency.client_latency
//		}
//		return latencies[0], nil
//	}
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
	maximumTraceCount := 100
	minimumValidTraceCount := 5
	if err != nil {
		return LatencyData{}, err
	}
	time.Sleep(startWaitingTime)
	averageLatency, err := waitForValidTraces(ctx, config, maximumTraceCount, minimumValidTraceCount)
	if err != nil {
		return LatencyData{}, err
	}
	logger.V(1).Info("Best effort data recorded", "client_latency", averageLatency.client_latency, "aggregator_latency", averageLatency.aggregator_latency, "detection_latency", averageLatency.detection_latency, "ml_latency", averageLatency.ml_latency, "db_latency", averageLatency.db_latency)
	dataExporter.WriteBE(averageLatency.client_latency, averageLatency.aggregator_latency, averageLatency.detection_latency, averageLatency.ml_latency, averageLatency.db_latency)
	return averageLatency, nil
}

func startOneReschedulingEpisode(ctx context.Context, config *Config, migrator *m.Migrator, httpClient *http.Client, queryClient *q.QueryClient, replicaCnt int, dataExporter *de.DataExporter) error {
	maximumTraceCount := 100
	minimumValidTraceCount := 5
	logger := klog.FromContext(ctx)
	err := waitForPodReady(ctx, config, queryClient)
	if err != nil {
		return err
	}
	time.Sleep(startWaitingTime)
	startAverageLatency, err := waitForValidTraces(ctx, config, maximumTraceCount, minimumValidTraceCount)
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
	endAverageLatency, err := waitForValidTraces(ctx, config, maximumTraceCount, minimumValidTraceCount)
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
	// TODO: 完成monitor的数据写入逻辑，以供后面case study的时候记录
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
				logger.V(1).Info("Current latency is within the threshold", "latency", latency)
			}
		case <-ctx.Done():
			return
		}
	}
}

// get traces based on facts:
// 1. the trace is not older than {spanTime} seconds
// 2. the trace has full spans or only need client span {fullSpan}
// 3. trace start time start from {after}
// 4. how many traces to get from jaeger api {maximumTraceCount}
// If there is newer traces, also return back the start time of latest traces
func getValidTraces(ctx context.Context, config *Config, maximumTraceCount int, spanTime int, fullSpan bool, after time.Time) ([]LatencyData, time.Time, error) {
	logger := klog.FromContext(ctx)
	// 构建请求 URL
	url := ""
	url = fmt.Sprintf("%s?service=%s&limit=%d", config.URL, config.ServiceName, maximumTraceCount)
	latestTraceStartTime := after.UnixMicro()
	// 发送 HTTP 请求
	resp, err := http.Get(url)
	if err != nil {
		logger.Error(err, "Error fetching trace")
		return []LatencyData{}, time.Time{}, err
	}
	defer resp.Body.Close()

	// 读取响应数据
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		logger.Error(err, "Error reading response body")
		return []LatencyData{}, time.Time{}, err
	}

	// 解析 JSON 响应
	var traceResponse d.TraceResponse
	err = json.Unmarshal(body, &traceResponse)
	if err != nil {
		log.Println("Error parsing JSON:", err)
		return []LatencyData{}, time.Time{}, err
	}

	// 检查是否有数据
	if len(traceResponse.Data) == 0 || len(traceResponse.Data[0].Spans) == 0 {
		logger.V(2).Info("No trace data found")
		return []LatencyData{}, time.Time{}, nil
	}
	var latencies []LatencyData
	// 遍历所有 spans，提取延迟信息
	for i := len(traceResponse.Data) - 1; i >= 0; i-- {
		trace := traceResponse.Data[i]
		latestTrace := traceResponse.Data[i]
		traceStartTime := latestTrace.Spans[0].StartTime
		if traceStartTime < after.UnixMicro()-int64(spanTime*1e6) {
			continue
		}
		// 如果 trace 是旧的，则跳过
		if traceStartTime < latestTraceStartTime {
			continue
		}
		latestTraceStartTime = traceStartTime
		latency := LatencyData{}
		traceCount := 0
		isClientPresent := false
		for _, span := range trace.Spans {
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
				latency.client_latency = durationMs
				isClientPresent = true
				traceCount++
			// 对于其他服务，计算 server 类型 span 的延迟
			case strings.Contains(operation, "aggregator.default.svc.cluster.local") && spanKind == "server":
				latency.aggregator_latency = durationMs
				traceCount++
			case strings.Contains(operation, "detection.default.svc.cluster.local") && spanKind == "server":
				latency.detection_latency = durationMs
				traceCount++
			case strings.Contains(operation, "machine-learning.default.svc.cluster.local") && spanKind == "server":
				latency.ml_latency = durationMs
				traceCount++
			case strings.Contains(operation, "db.default.svc.cluster.local") && spanKind == "server":
				latency.db_latency = durationMs
				traceCount++
			}
		}
		if traceCount == 0 {
			continue
		}
		// If the trace is not full span and client is present, or the trace has full spans, add it to the latencies
		if (!fullSpan && isClientPresent) || traceCount == 5 {
			latencies = append(latencies, latency)
		}
	}
	return latencies, time.UnixMicro(latestTraceStartTime), nil
}

func updateEastimateMA(ctx context.Context, config *Config, now time.Time) time.Time {
	logger := klog.FromContext(ctx)
	latencies, latestTraceStartTime, err := getValidTraces(ctx, config, 1, 5, false, now)
	if err != nil {
		logger.Error(err, "Failed to get valid traces")
		return now
	}
	if len(latencies) == 0 {
		return now
	}
	// 获取当前的 estimatedRT
	currentMA := estimatedRT.Get()
	if currentMA == 0 {
		currentMA = latencies[0].client_latency
	} else {
		// 更新移动平均值的函数
		latency := currentMA
		currentMA = (1-config.Alpha)*latency + config.Alpha*latencies[0].client_latency
	}

	// 更新 estimatedRT
	estimatedRT.Set(currentMA)
	return latestTraceStartTime
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
	dataExporter := de.NewDataExporter(*output_file, de.DataExporterType(*strategy))
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
			tsDataExporter := de.NewDataExporter(*output_file, de.TIME_SERIES)
			lastUpdateTime := time.Now()
			for {
				updatedTime := updateEastimateMA(ctx, config, lastUpdateTime)
				if updatedTime != lastUpdateTime {
					tsDataExporter.WriteTS(updatedTime.Unix(), estimatedRT.Get())
					lastUpdateTime = updatedTime
				}
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
