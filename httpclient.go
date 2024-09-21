package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"k8s.io/klog/v2"
)

type HttpClient struct {
	client *http.Client
}

func NewHttpClient(timeout time.Duration) *HttpClient {
	return &HttpClient{
		client: &http.Client{Timeout: timeout},
	}
}

func (h *HttpClient) getRequst(ctx context.Context, migrator *Migrator) (*Request, error) {
	clusterState, err := migrator.GetClusterState(ctx)
	if err != nil {
		return nil, err
	}
	podDeployable, err := migrator.GetPodsAvailableNodes(ctx)
	if err != nil {
		return nil, err
	}
	request := Request{
		ClusterState:  *clusterState,
		PodDeployable: podDeployable,
	}
	return &request, nil
}

func (h *HttpClient) getMigrationResult(ctx context.Context, migrator *Migrator) (*Response, error) {
	logger := klog.FromContext(ctx)
	// 初始化http client
	request, err := h.getRequst(ctx, migrator)
	if err != nil {
		return nil, err
	}
	jsonData, err := json.Marshal(request)
	if err != nil {
		logger.Error(err, "JSON编码失败")
		return nil, err
	}

	// 发送POST请求
	resp, err := h.client.Post("http://localhost:5000/get_action", "application/json", bytes.NewBuffer(jsonData))
	if err != nil {
		logger.Error(err, "发送POST请求失败")
		return nil, err
	}

	if resp.StatusCode != http.StatusOK {
		logger.Error(nil, "HTTP请求失败", "状态码", resp.StatusCode)
		return nil, fmt.Errorf("HTTP请求失败，状态码：%d", resp.StatusCode)
	}

	defer resp.Body.Close()

	// 读取响应体
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		logger.Error(err, "读取响应体失败")
		return nil, err
	}

	// 解析JSON响应
	var response Response
	err = json.Unmarshal(body, &response)
	if err != nil {
		logger.Error(err, "解析JSON响应失败")
		return nil, err
	}
	return &response, nil
}
