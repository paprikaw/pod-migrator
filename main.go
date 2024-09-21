package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/joho/godotenv"
	"k8s.io/klog/v2"
)

func monitor(ctx context.Context, migrator *Migrator, httpClient *HttpClient) {
	// Import necessary packages
	interval := 3 * time.Second // Set monitoring interval to 5 minutes
	logger := klog.FromContext(ctx)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	qosThreshold := migrator.qosThreshold
	for {
		ticker.Reset(interval)
		select {
		case <-ticker.C:
			latency, err := migrator.proClient.QeuryAppLatencyByMilli(ctx, "client")
			if err != nil {
				logger.Error(err, "Failed to query application latency")
				continue
			}
			logger.V(1).Info("----------Monitoring process starts----------")
			if latency > int64(qosThreshold) {
				logger.V(1).Info("Current latency", "latency", latency)
				response, err := httpClient.getMigrationResult(ctx, migrator)
				if err != nil {
					logger.Error(err, "Failed to get migration result")
					continue
				}
				if response.IsStop {
					logger.V(1).Info("Skip this migration")
					continue
				}
				err = migrator.MigratePod(ctx, response.PodName, response.TargetNode)
				if err != nil {
					logger.Error(err, "Failed to migrate Pod")
					continue
				}
			}
		case <-ctx.Done():
			return
		}
	}
}

func main() {
	// Initialize klog
	klog.InitFlags(nil)
	flag.Parse()
	logger := klog.NewKlogr()
	ctx := klog.NewContext(context.Background(), logger)
	err := godotenv.Load()
	if err != nil {
		logger.Error(err, "Error loading .env file")
	}

	// Load cluster configuration and initialize kubernetes client
	kubeconfig := os.Getenv("KUBECONFIG")
	prometheusAddr := os.Getenv("PROMETHEUS_ADDR")
	appLabel := os.Getenv("APP_LABEL")
	gatewayService := os.Getenv("GATEWAY_SERVICE")
	namespace := os.Getenv("NAMESPACE")
	qosThreshold, err := strconv.Atoi(os.Getenv("QOS_THRESHOLD"))
	if err != nil {
		logger.Error(err, "QOS_THRESHOLD is not a valid integer")
		return
	}
	migrator, err := NewMigrator(kubeconfig, prometheusAddr, appLabel, gatewayService, namespace, qosThreshold)
	if err != nil {
		fmt.Println("Error creating migrator:", err)
		return
	}
	httpClient := NewHttpClient(10 * time.Second)
	monitor(ctx, migrator, httpClient)
	// // monitor(ctx, migrator)
	// oldPodName := "detection-7f78df9988-qmwp7"
	// // err = migrator.MigratePod(ctx, "detection", oldPodName, "tb-cloud-vm1")
	// err = migrator.MigratePod(ctx, "detection", oldPodName, "tb-edge-vm1")
	// if err != nil {
	// 	fmt.Print(err, "Error migrating pod")
	// 	return
	// }
}
