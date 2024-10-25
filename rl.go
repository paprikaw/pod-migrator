package main

import (
	"context"
	"net/http"
	"time"

	de "github.com/paprikaw/rscheduler/pkg/dataexporter"
	m "github.com/paprikaw/rscheduler/pkg/migrator"
	q "github.com/paprikaw/rscheduler/pkg/queryclient"
	"k8s.io/klog/v2"
)

func recordPodReschedulingPodDistribution(ctx context.Context, config *Config, queryClient *q.QueryClient, dataExporter *de.DataExporter, migrator *m.Migrator, httpClient *http.Client) error {
	logger := klog.FromContext(ctx)
	err := waitForPodReady(ctx, config, queryClient)
	if err != nil {
		return err
	}
	step := 0
	for step < config.MaxStep {
		logger.V(1).Info("----------Rescheduling process starts----------")
		response, err := GetMigrationResult(ctx, config, queryClient, httpClient, migrator, failedNodes.Get())
		if err != nil {
			logger.Error(err, "Failed to get migration result")
			continue
		}
		if response.IsStop {
			logger.V(1).Info("Stop the migration")
			break
		}
		err = migrator.MigratePod(ctx, 1*time.Second, config.Namespace, response.PodName, config.AppLabel, response.TargetNode, dataExporter, estimatedRT.Get(), false)
		if err != nil {
			logger.Error(err, "Failed to migrate Pod")
			return err
		}
		step++
	}
	aggregator, detection, ml, db, err := GetPodDistribution(ctx, config, migrator)
	if err != nil {
		return err
	}
	logger.V(1).Info("Pod distribution recorded", "aggregator", aggregator, "detection", detection, "ml", ml, "db", db)
	dataExporter.WritePodPlacement(aggregator, detection, ml, db)
	return nil
}

func startOneReschedulingEpisode(ctx context.Context, config *Config, migrator *m.Migrator, httpClient *http.Client, queryClient *q.QueryClient, dataExporter *de.DataExporter) error {
	maximumTraceCount := 100
	minimumValidTraceCount := 5
	logger := klog.FromContext(ctx)
	err := waitForPodReady(ctx, config, queryClient)
	if err != nil {
		return err
	}
	time.Sleep(startWaitingTime)
	startAverageLatency, err := waitForValidTraces(ctx, config, maximumTraceCount, minimumValidTraceCount, time.Now())
	if err != nil {
		return err
	}
	logger.V(1).Info("Start average latency", "client_latency", startAverageLatency.client_latency, "aggregator_latency", startAverageLatency.aggregator_latency, "detection_latency", startAverageLatency.detection_latency, "ml_latency", startAverageLatency.ml_latency, "db_latency", startAverageLatency.db_latency)
	step := 0
	for step < config.MaxStep {
		logger.V(1).Info("----------Rescheduling process starts----------")
		response, err := GetMigrationResult(ctx, config, queryClient, httpClient, migrator, failedNodes.Get())
		if err != nil {
			logger.Error(err, "Failed to get migration result")
			continue
		}
		if response.IsStop {
			logger.V(1).Info("Stop the migration")
			break
		}
		err = migrator.MigratePod(ctx, 1*time.Second, config.Namespace, response.PodName, config.AppLabel, response.TargetNode, dataExporter, estimatedRT.Get(), false)
		if err != nil {
			logger.Error(err, "Failed to migrate Pod")
			return err
		}
		step++
	}
	time.Sleep(endWaitingTime)
	endAverageLatency, err := waitForValidTraces(ctx, config, maximumTraceCount, minimumValidTraceCount, time.Now())
	logger.V(1).Info("End average latency", "client_latency", endAverageLatency.client_latency, "aggregator_latency", endAverageLatency.aggregator_latency, "detection_latency", endAverageLatency.detection_latency, "ml_latency", endAverageLatency.ml_latency, "db_latency", endAverageLatency.db_latency)
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
