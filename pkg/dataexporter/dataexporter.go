package dataexporter

import (
	"encoding/csv"
	"fmt"
	"log"
	"os"
	"time"
)

var rlHeader = []string{
	"start_total_latency",
	"end_total_latency",
	"start_aggregator_latency",
	"end_aggregator_latency",
	"start_detection_latency",
	"end_detection_latency",
	"start_ml_latency",
	"end_ml_latency",
	"start_db_latency",
	"end_db_latency",
	"step",
}

var bestEffortHeader = []string{
	"total_latency",
	"aggregator_latency",
	"detection_latency",
	"ml_latency",
	"db_latency",
}

var timeSeriesHeader = []string{
	"timestamp",
	"human_readable_timestamp",
	"latency",
}

var reschedulingHeader = []string{
	"timestamp",
	"target_node",
	"target_pod",
	"is_stopped",
}

type DataExporterType string

const (
	REINFORCEMENT DataExporterType = "rl"
	BEST_EFFORT   DataExporterType = "be"
	TIME_SERIES   DataExporterType = "ts"
	RESCHEUDLING  DataExporterType = "rs"
)

type DataExporter struct {
	file      *os.File
	csvwriter *csv.Writer
}

func NewDataExporter(outputFile string, exporterType DataExporterType) *DataExporter {
	var file *os.File
	var err error

	// 检查文件是否存在
	if _, err := os.Stat(outputFile); os.IsNotExist(err) {
		// 文件不存在，创建新文件
		file, err = os.Create(outputFile)
		if err != nil {
			log.Fatalf("Error creating file: %v", err)
		}
	} else {
		// 文件存在，使用追加模式
		file, err = os.OpenFile(outputFile, os.O_APPEND|os.O_WRONLY, os.ModeAppend)
		if err != nil {
			log.Fatalf("Error opening file: %v", err)
		}
	}

	// 创建 CSV writer
	writer := csv.NewWriter(file)

	// 检查文件是否为空，如果为空则写入表头
	fileInfo, err := file.Stat()
	if err != nil {
		log.Fatalf("Error getting file info: %v", err)
	}
	if fileInfo.Size() == 0 {
		switch exporterType {
		case REINFORCEMENT:
			writer.Write(rlHeader)
		case BEST_EFFORT:
			writer.Write(bestEffortHeader)
		case TIME_SERIES:
			writer.Write(timeSeriesHeader)
		case RESCHEUDLING:
			writer.Write(reschedulingHeader)
		}
	}
	return &DataExporter{file: file, csvwriter: writer}
}

func (de *DataExporter) WriteRL(
	start_total_latency,
	end_total_latency,
	start_aggregator_latency,
	end_aggregator_latency,
	start_detection_latency,
	end_detection_latency,
	start_ml_latency,
	end_ml_latency,
	start_db_latency,
	end_db_latency float64,
	step int,
) error {
	record := []string{
		fmt.Sprintf("%.2f", start_total_latency),
		fmt.Sprintf("%.2f", end_total_latency),
		fmt.Sprintf("%.2f", start_aggregator_latency),
		fmt.Sprintf("%.2f", end_aggregator_latency),
		fmt.Sprintf("%.2f", start_detection_latency),
		fmt.Sprintf("%.2f", end_detection_latency),
		fmt.Sprintf("%.2f", start_ml_latency),
		fmt.Sprintf("%.2f", end_ml_latency),
		fmt.Sprintf("%.2f", start_db_latency),
		fmt.Sprintf("%.2f", end_db_latency),
		fmt.Sprintf("%d", step),
	}
	return de.csvwriter.Write(record)
}

func (de *DataExporter) WriteBE(
	total_latency,
	aggregator_latency,
	detection_latency,
	ml_latency,
	db_latency float64,
) error {
	record := []string{
		fmt.Sprintf("%.2f", total_latency),
		fmt.Sprintf("%.2f", aggregator_latency),
		fmt.Sprintf("%.2f", detection_latency),
		fmt.Sprintf("%.2f", ml_latency),
		fmt.Sprintf("%.2f", db_latency),
	}
	return de.csvwriter.Write(record)
}

func (de *DataExporter) WriteTS(unix_timestamp int64, latency float64) error {
	formatted_timestamp := time.Unix(unix_timestamp, 0).Format(time.DateTime)
	record := []string{
		fmt.Sprintf("%d", unix_timestamp),
		formatted_timestamp,
		fmt.Sprintf("%.2f", latency),
	}
	err := de.csvwriter.Write(record)
	if err != nil {
		return err
	}
	de.csvwriter.Flush()
	return de.csvwriter.Error()
}

func (de *DataExporter) WriteRS(unix_timestamp int64, target_node, target_pod string, is_stopped bool) error {
	record := []string{
		fmt.Sprintf("%d", unix_timestamp),
		target_node,
		target_pod,
		fmt.Sprintf("%t", is_stopped),
	}
	return de.csvwriter.Write(record)
}

// 添加Close方法
func (de *DataExporter) Close() error {
	de.csvwriter.Flush() // 确保所有缓冲数据被写入
	flushErr := de.csvwriter.Error()
	fileErr := de.file.Close() // 关闭文件

	if flushErr != nil {
		return flushErr // 返回Flush的错误
	}
	return fileErr // 返回文件关闭的错误（如果有）
}
