package main

import (
	"context"
	"time"

	de "github.com/paprikaw/rscheduler/pkg/dataexporter"
	m "github.com/paprikaw/rscheduler/pkg/migrator"
	q "github.com/paprikaw/rscheduler/pkg/queryclient"
	"k8s.io/klog/v2"
)

func recordBestEffortData(ctx context.Context, config *Config, queryClient *q.QueryClient, dataExporter *de.DataExporter) error {
	logger := klog.FromContext(ctx)
	err := waitForPodReady(ctx, config, queryClient)
	maximumTraceCount := 100
	minimumValidTraceCount := 5
	if err != nil {
		return err
	}
	time.Sleep(startWaitingTime)
	averageLatency, err := waitForValidTraces(ctx, config, maximumTraceCount, minimumValidTraceCount, time.Now())
	if err != nil {
		return err
	}
	logger.V(1).Info("Best effort data recorded", "client_latency", averageLatency.client_latency, "aggregator_latency", averageLatency.aggregator_latency, "detection_latency", averageLatency.detection_latency, "ml_latency", averageLatency.ml_latency, "db_latency", averageLatency.db_latency)
	dataExporter.WriteBE(averageLatency.client_latency, averageLatency.aggregator_latency, averageLatency.detection_latency, averageLatency.ml_latency, averageLatency.db_latency)
	return nil
}

func recordPodBestEffortPodDistribution(ctx context.Context, config *Config, queryClient *q.QueryClient, dataExporter *de.DataExporter, migrator *m.Migrator) error {
	logger := klog.FromContext(ctx)
	err := waitForPodReady(ctx, config, queryClient)
	if err != nil {
		return err
	}
	aggregator, detection, ml, db, err := GetPodDistribution(ctx, config, migrator)
	if err != nil {
		return err
	}
	logger.V(1).Info("Pod distribution recorded", "aggregator", aggregator, "detection", detection, "ml", ml, "db", db)
	dataExporter.WritePodPlacement(aggregator, detection, ml, db)
	return nil
}
