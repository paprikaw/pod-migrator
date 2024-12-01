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
	traceStartTime     time.Time
	client_latency     float64
	aggregator_latency float64
	detection_latency  float64
	ml_latency         float64
	db_latency         float64
}

var schedulingMutex = sync.Mutex{}

// Estimated Moving Average
type EstimatedMA struct {
	mu      sync.RWMutex
	latency float64
}

// Estimated Moving Average
type FailedNodes struct {
	mu    sync.RWMutex
	nodes map[string]bool
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
var failedNodes = &FailedNodes{nodes: make(map[string]bool)}

func (f *FailedNodes) Add(node string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.nodes[node] = true
}

func (f *FailedNodes) Get() []string {
	f.mu.RLock()
	defer f.mu.RUnlock()
	var nodes []string
	for node := range f.nodes {
		nodes = append(nodes, node)
	}
	return nodes
}

func (f *FailedNodes) Remove(node string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.nodes, node)
}

func waitForValidTraces(ctx context.Context, config *Config, maximumTraceCount int, minimumValidTraceCount int, now time.Time) (LatencyData, error) {
	logger := klog.FromContext(ctx)
	retries := 0
	var averageLatency LatencyData
	for retries < config.MaxPodWaitingRetries {
		retries++
		result, err := getValidTraces(ctx, config, maximumTraceCount, true, now)
		if err != nil {
			logger.Error(err, "Failed to get immediate trace")
			return LatencyData{}, err
		}
		if len(result) < minimumValidTraceCount {
			logger.V(2).Info("Not enough valid trace data", "retries", retries, "number of results", len(result))
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

func monitor(ctx context.Context, config *Config, migrator *m.Migrator, httpClient *http.Client, queryClient *q.QueryClient, dataExporter *de.DataExporter, thresholdInvokingCh chan bool, reschedulingDoneCh chan bool) {
	// Import necessary packages
	// TODO: 完成monitor的数据写入逻辑，以供后面case study的时候记录
	interval := 10 * time.Second // Set monitoring interval to 5 minutes
	logger := klog.FromContext(ctx)
	ticker := time.NewTicker(interval)
	waitForPodReady(ctx, config, queryClient)
	defer ticker.Stop()
	for {
		ticker.Reset(interval)
		select {
		case <-thresholdInvokingCh:
			latency := estimatedRT.Get()
			logger.V(1).Info("----------Monitoring process starts----------")
			logger.V(1).Info("----------Invoking by threshold----------")
			waitForPodReady(ctx, config, queryClient)
			schedulingMutex.Lock()
			logger.V(1).Info("Current latency", "latency", latency)
			//TODO: 可能会有一些race condition
			response, err := GetMigrationResult(ctx, config, queryClient, httpClient, migrator, failedNodes.Get())
			if err != nil {
				logger.Error(err, "Failed to get migration result")
				schedulingMutex.Unlock()
				continue
			}
			if response.IsStop {
				logger.V(1).Info("Skip this migration")
				select {
				case reschedulingDoneCh <- true:
				default:
				}
				schedulingMutex.Unlock()
				continue
			}
			err = migrator.MigratePod(ctx, config.PollingPeriod, config.Namespace, response.PodName, config.AppLabel, response.TargetNode, dataExporter, latency, true)
			if err != nil {
				logger.Error(err, "Failed to migrate Pod")
				schedulingMutex.Unlock()
				continue
			}
			schedulingMutex.Unlock()
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
func getValidTraces(ctx context.Context, config *Config, maximumTraceCount int, fullSpan bool, after time.Time) ([]LatencyData, error) {
	logger := klog.FromContext(ctx)
	// 构建请求 URL
	url := ""
	url = fmt.Sprintf("%s?service=%s&limit=%d", config.JaegerURL, config.ServiceName, maximumTraceCount)
	latestTraceStartTime := after.UnixMicro()
	logger.V(3).Info("Latest trace start time", "time", latestTraceStartTime)
	logger.V(3).Info("Latest trace start time readable", "time", time.UnixMicro(latestTraceStartTime))
	// 发送 HTTP 请求
	resp, err := http.Get(url)
	if err != nil {
		logger.Error(err, "Error fetching trace")
		return []LatencyData{}, err
	}
	defer resp.Body.Close()

	// 读取响应数据
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		logger.Error(err, "Error reading response body")
		return []LatencyData{}, err
	}

	// 解析 JSON 响应
	var traceResponse d.TraceResponse
	err = json.Unmarshal(body, &traceResponse)
	if err != nil {
		log.Println("Error parsing JSON:", err)
		return []LatencyData{}, err
	}

	// 检查是否有数据
	if len(traceResponse.Data) == 0 || len(traceResponse.Data[0].Spans) == 0 {
		logger.V(2).Info("No trace data found")
		return []LatencyData{}, err
	}
	var latencies []LatencyData
	// 遍历所有 spans，提取延迟信息
	for i := 0; i < len(traceResponse.Data); i++ {
		trace := traceResponse.Data[i]
		latestTrace := traceResponse.Data[i]
		traceStartTime := latestTrace.Spans[0].StartTime
		// 如果 trace 是旧的，则跳过
		if traceStartTime < latestTraceStartTime {
			// logger.V(0).Info("Trace is old", "traceStartTime", time.UnixMicro(traceStartTime), "latestTraceStartTime", time.UnixMicro(latestTraceStartTime))
			continue
		}
		// logger.V(0).Info("Trace is valid", "traceStartTime", time.UnixMicro(traceStartTime), "latestTraceStartTime", time.UnixMicro(latestTraceStartTime))
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
				logger.V(0).Info("span.kind is not a string", "value", spanKind)
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
		if traceCount == 5 {
			logger.V(0).Info("Trace has full spans", "traceStartTime", time.UnixMicro(traceStartTime), "latestTraceStartTime", time.UnixMicro(latestTraceStartTime))
		}
		// If the trace is not full span and client is present, or the trace has full spans, add it to the latencies
		if (!fullSpan && isClientPresent) || traceCount == 5 {
			latency.traceStartTime = time.UnixMicro(traceStartTime)
			latencies = append(latencies, latency)
		}

	}
	return latencies, nil
}

func monitorLatencyJaeger(ctx context.Context, config *Config, dataExporter *de.DataExporter, thresholdInvokingCh chan bool, thresholdSatifiedCh chan bool, qoeThreshold int) {
	lastUpdateTime := time.Now()
	for {
		logger := klog.FromContext(ctx)
		// Get all the traces that is from now, maximum 20 traces
		latencies, err := getValidTraces(ctx, config, 20, true, lastUpdateTime)
		if err != nil {
			logger.Error(err, "Failed to get valid traces")
			continue
		}
		if len(latencies) == 0 {
			logger.V(0).Info("No valid traces found")
			continue
		}
		cur_latency := latencies[0]
		// Update and Calculate Estimated Moving Average
		currentMA := estimatedRT.Get()
		if currentMA == 0 {
			currentMA = cur_latency.client_latency
		} else {
			// 更新移动平均值的函数
			latency := currentMA
			currentMA = (1-config.Alpha)*latency + config.Alpha*cur_latency.client_latency
		}
		estimatedRT.Set(currentMA)

		dataExporter.WriteTS(cur_latency.traceStartTime.Unix(), currentMA, "", "", "", "", de.LATENCY)

		// 通知其他线程进行迁移
		if currentMA > float64(qoeThreshold) {
			select {
			case thresholdInvokingCh <- true:
			default:
			}
		} else if currentMA == 0 {
			continue
		} else {
			select {
			case thresholdSatifiedCh <- true:
			default:
			}
		}
		time.Sleep(config.PollingPeriod)
	}
}

func monitorLatencyHttp(ctx context.Context, config *Config, dataExporter *de.DataExporter, queryClient *q.QueryClient, thresholdInvokingCh chan bool, thresholdSatifiedCh chan bool, qoeThreshold int) {
	logger := klog.FromContext(ctx)
	// 新建一个http client，准备进行http请求
	responseBody := struct {
		Latency   float64 `json:"service_calling_latency"`
		TimeStamp int64   `json:"timestamp"`
	}{}
	httpClient := &http.Client{
		Timeout: 1 * time.Second,
	}
	url := config.EntryServiceURL
	// pollingPeriod := config.PollingPeriod
	waitForPodReady(ctx, config, queryClient)
	for {
		// 计算延迟
		resp, err := httpClient.Get(url)
		if err != nil {
			logger.Error(err, "Failed to get entry service")
			continue
		}
		// Ensure response body is closed
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			logger.Error(fmt.Errorf("unexpected status code: %d", resp.StatusCode), "Failed to get entry service")
			continue
		}

		logger.V(2).Info("Get entry service response", "response", resp.Status)

		// parse response
		err = json.NewDecoder(resp.Body).Decode(&responseBody)
		if err != nil {
			logger.Error(err, "Failed to parse response")
			continue
		}

		logger.Info("Service calling latency and timestamp received",
			"latency", responseBody.Latency, "timestamp", responseBody.TimeStamp)

		// currentMA := estimatedRT.Get()
		// if currentMA == 0 {
		// 	currentMA = float64(responseBody.Latency)
		// } else {
		// 	// 更新移动平均值的函数
		// 	currentLatency := currentMA
		// 	currentMA = (1-config.Alpha)*currentLatency + config.Alpha*float64(responseBody.Latency)
		// }
		// 更新 estimatedRT
		estimatedRT.Set(responseBody.Latency)
		dataExporter.WriteTS(responseBody.TimeStamp, responseBody.Latency, "", "", "", "", de.LATENCY)

		if responseBody.Latency > float64(qoeThreshold) {
			select {
			case thresholdInvokingCh <- true:
			default:
			}
		} else if float64(responseBody.Latency) == 0 {
			continue
		} else {
			select {
			case thresholdSatifiedCh <- true:
			default:
			}
		}
	}
}

func main() {
	// Initialize klog
	klog.InitFlags(nil)
	mode := flag.String("mode", "monitoring", "The mode of the program: test, pod_distribution, monitoring")
	strategy := flag.String("strategy", "rl", "The strategy of the rescheduling: rl, be")
	output_file := flag.String("output", "", "The output file")
	targetReplica := flag.Int("target_replica", -1, "The target replica")
	caseWaitingTime := flag.Int("t", 120, "The waiting time for the case")
	afterWaitTime := flag.Int("after_wait_time", 200, "The waiting time after the case")
	qoeThreshold := flag.Int("qos", 250, "The QoS threshold for the case")
	targetNodeA := flag.String("target_node_a", "", "The target node for the node failed case")
	targetNodeB := flag.String("target_node_b", "", "The target node for the node failed case")
	stableStartTime := flag.Int("stable_start_time", 60, "The start time for the stable period")
	experimentWaitingTime := flag.Int("experiment_waiting_time", 60, "The waiting time for the experiment")
	if *targetReplica == -1 && *mode == "autoscaling" {
		fmt.Println("Target replica is not set")
		return
	}
	flag.Parse()
	// 创建data exporter
	var dataExporter *de.DataExporter
	if *mode == "pod_distribution" {
		dataExporter = de.NewDataExporter(*output_file, de.POD_DISTRIBUTION)
	} else if *mode == "test" {
		dataExporter = de.NewDataExporter(*output_file, de.DataExporterType(*strategy))
	} else if *mode == "monitoring" || *mode == "nodefailed" || *mode == "autoscaling" {
		dataExporter = de.NewDataExporter(*output_file, de.TIME_SERIES)
	} else {
		fmt.Println("Invalid mode")
		return
	}
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
	// // 创建一个通道来接收信号
	// sigChan := make(chan os.Signal, 1)
	// // 注册要接收的信号
	// signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	thresholdInvokingCh := make(chan bool, 1)
	thresholdSatifiedCh := make(chan bool, 1)
	reschedulingDoneCh := make(chan bool, 1)
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
	// 启动一个goroutine来处理信号
	// go func() {
	// 	sig := <-sigChan
	// 	fmt.Println("Received signal:", sig)
	// 	cleanup(ctx, config, migrator, dataExporter)
	// 	os.Exit(0)
	// }()

	// Start monitoring latency
	if *mode == "monitoring" || *mode == "nodefailed" || *mode == "autoscaling" {
		go monitorLatencyHttp(ctx, config, dataExporter, queryClient, thresholdInvokingCh, thresholdSatifiedCh, *qoeThreshold)
	}
	if *mode == "test" {
		if *strategy == "rl" {
			startOneReschedulingEpisode(ctx, config, migrator, httpClient, queryClient, dataExporter)
		} else if *strategy == "be" {
			recordBestEffortData(ctx, config, queryClient, dataExporter)
		}
	} else if *mode == "pod_distribution" {
		if *strategy == "rl" {
			recordPodReschedulingPodDistribution(ctx, config, queryClient, dataExporter, migrator, httpClient)
		} else if *strategy == "be" {
			recordPodBestEffortPodDistribution(ctx, config, queryClient, dataExporter, migrator)
		}
	} else if *mode == "monitoring" {
		monitor(ctx, config, migrator, httpClient, queryClient, dataExporter, thresholdInvokingCh, reschedulingDoneCh)
	} else if *mode == "nodefailed" {
		time.Sleep(time.Duration(*stableStartTime) * time.Second)
		dataExporter.WriteTS(time.Now().Unix(), 0, "", "", "", "", de.EXPERIMENT_START)
		time.Sleep(time.Duration(*experimentWaitingTime) * time.Second)
		if *strategy == "rl" {
			go monitor(ctx, config, migrator, httpClient, queryClient, dataExporter, thresholdInvokingCh, reschedulingDoneCh)
		}
		nodeFailedCase(ctx, config, migrator, queryClient, dataExporter, reschedulingDoneCh, config.Namespace, config.AppLabel, *caseWaitingTime, *afterWaitTime, []string{*targetNodeA, *targetNodeB})
	} else if *mode == "autoscaling" {
		go autoscalingCase(ctx, config, migrator, queryClient, dataExporter, reschedulingDoneCh, config.Namespace, int32(*targetReplica))
		monitor(ctx, config, migrator, httpClient, queryClient, dataExporter, thresholdInvokingCh, reschedulingDoneCh)
	} else if *mode == "debug" {

	}
}

// func cleanup(ctx context.Context, config *Config, migrator *m.Migrator, dataExporter *de.DataExporter) {
// 	logger := klog.FromContext(ctx)
// 	aggregator, detection, ml, db, err := GetPodDistribution(ctx, config, migrator)
// 	if err != nil {
// 		logger.Error(err, "Failed to get pod distribution")
// 		return
// 	}
// 	// dataExporter.WriteTSWithPodPlacement(time.Now().Unix(), 0, "", "", "", aggregator, detection, ml, db, de.POD_DISTRO)
// 	logger.V(1).Info("Cleanup done, write final pod distribution")
// }
