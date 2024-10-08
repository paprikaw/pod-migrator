package baseline

import (
	"context"
	"flag"
	"fmt"

	// MigrateDeployment runs the latency-aware rescheduling algorithm for deployments with the "mubench" tag
	"github.com/joho/godotenv"
	"k8s.io/klog/v2"
)

func main() {
	// Initialize klog
	klog.InitFlags(nil)
	flag.Parse()
	logger := klog.NewKlogr()
	ctx := klog.NewContext(context.Background(), logger)
	// 获取标签的所有deployment
	// 对于每一个deployment中的pod，检查其是否在node上运行
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

}
