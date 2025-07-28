// Copyright 2025 The DBQ Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package dbqcore

import "context"

// DbqDataProfiler is the interface that wraps the basic data profiling methods
type DbqDataProfiler interface {
	// ProfileDataset is an entry point that runs profiling process by tying all specific profiling calls together
	// todo: consider extracting it into separate entity
	ProfileDataset(ctx context.Context, dataset string, sample bool, maxConcurrent int) (*TableMetrics, error)

	GetColumns(ctx context.Context, databaseName string, tableName string) ([]*ColumnInfo, error)
	GetTotalRows(ctx context.Context, dataset string) (uint64, error)
	GetNullCount(ctx context.Context, dataset string, column *ColumnInfo) (uint64, error)
	GetBlankCount(ctx context.Context, dataset string, column *ColumnInfo) (int64, error)
	GetNumericStats(ctx context.Context, dataset string, column *ColumnInfo) (*NumericStats, error)
	GetMostFrequentValue(ctx context.Context, dataset string, column *ColumnInfo) (*string, error)
	GetSampleData(ctx context.Context, dataset string) ([]map[string]interface{}, error)
	IsNumericType(dataType string) bool
	IsStringType(dataType string) bool
}

// NumericStats represents the numeric statistics of a column.
type NumericStats struct {
	MinValue    *float64
	MaxValue    *float64
	AvgValue    *float64
	StddevValue *float64
}

// TableMetrics represents the metrics of a table.
type TableMetrics struct {
	ProfiledAt          int64                     `json:"profiled_at"`
	TableName           string                    `json:"table_name"`
	DatabaseName        string                    `json:"database_name"`
	TotalRows           uint64                    `json:"total_rows"`
	ColumnsMetrics      map[string]*ColumnMetrics `json:"columns_metrics"`
	RowsSample          []map[string]interface{}  `json:"rows_sample"`
	ProfilingDurationMs int64                     `json:"profiling_duration_ms"`
	DbqErrors           []error                   `json:"__dbq_errors"`
}

// ColumnMetrics represents the metrics of a column.
type ColumnMetrics struct {
	ColumnName          string   `json:"col_name"`
	ColumnComment       string   `json:"col_comment"`
	ColumnPosition      uint     `json:"col_position"`
	DataType            string   `json:"data_type"`
	NullCount           uint64   `json:"null_count"`
	BlankCount          *int64   `json:"blank_count,omitempty"`         // string only
	MinValue            *float64 `json:"min_value,omitempty"`           // numeric only
	MaxValue            *float64 `json:"max_value,omitempty"`           // numeric only
	AvgValue            *float64 `json:"avg_value,omitempty"`           // numeric only
	StddevValue         *float64 `json:"stddev_value,omitempty"`        // numeric only (Population StdDev)
	MostFrequentValue   *string  `json:"most_frequent_value,omitempty"` // pointer to handle NULL as most frequent
	ProfilingDurationMs int64    `json:"profiling_duration_ms"`
}

// ColumnInfo represents the basic information of a column.
type ColumnInfo struct {
	Name     string
	Type     string
	Comment  string
	Position uint
}
