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

type DbqConnector interface {
	Ping() (string, error)
	ImportDatasets(filter string) ([]string, error)
	ProfileDataset(dataset string, sample bool, maxConcurrent int) (*TableMetrics, error)
	RunCheck(check *Check, dataset string, defaultWhere string) (bool, string, error)
}

const (
	CheckTypeRawQuery = "raw_query"
)

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

type TableMetrics struct {
	ProfiledAt          int64                     `json:"profiled_at"`
	TableName           string                    `json:"table_name"`
	DatabaseName        string                    `json:"database_name"`
	TotalRows           uint64                    `json:"total_rows"`
	ColumnsMetrics      map[string]*ColumnMetrics `json:"columns_metrics"`
	RowsSample          []map[string]interface{}  `json:"rows_sample"`
	ProfilingDurationMs int64                     `json:"profiling_duration_ms"`
	DbqErrors           []error                   `json:"_dbq_errors"`
}

type ColumnInfo struct {
	Name     string
	Type     string
	Comment  string
	Position uint
}

type ValidationResult struct {
	CheckID      string `json:"check_id"`
	Pass         bool   `json:"pass"`
	ActualResult string `json:"actual_result,omitempty"`
	Message      string `json:"message,omitempty"`
}
