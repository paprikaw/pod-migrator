package main

// Node 结构体定义
type Node struct {
	NodeName           string `json:"node_name"`
	CPUAvailability    int64  `json:"cpu_availability"`
	MemoryAvailability int64  `json:"memory_availability"`
	BandwidthUsage     int64  `json:"bandwidth_usage"`
	// Bandwidth          float32 `json:"bandwidth"`
	// Layer              int32   `json:"layer"`
	// CPUType            int32   `json:"cpu_type"`
}

// Pod 结构体定义
type Pod struct {
	NodeName string `json:"node_name"`
	PodName  string `json:"pod_name"`
	// TotalBandwidth float32 `json:"total_bandwidth"`
	// CPURequests    float32 `json:"cpu_requests"`
	// MemoryRequests float32 `json:"memory_requests"`
}

// ClusterState 结构体定义，包含所有节点和Pod的状态
type ClusterState struct {
	Nodes map[string]Node `json:"nodes"`
	Pods  map[string]Pod  `json:"pods"`
}

type PodDeployable struct {
	PodName   string   `json:"pod_name"`
	NodeNames []string `json:"node_names"`
}

type Request struct {
	ClusterState  ClusterState    `json:"cluster_state"`
	PodDeployable []PodDeployable `json:"pod_deployable"`
}

type Response struct {
	PodName    string `json:"pod_name"`
	TargetNode string `json:"target_node"`
}
