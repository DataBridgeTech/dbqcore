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

import (
	"context"
	"fmt"
	"log/slog"
	"time"
)

// DataQualityCheckType represents the type of data quality check.
type DataQualityCheckType string

// ValidationResult represents the result of a data quality check.
type ValidationResult struct {
	CheckID          string `json:"check_id"`
	Pass             bool   `json:"pass"`
	QueryResultValue string `json:"query_result_value,omitempty"`
	Error            string `json:"error,omitempty"`
}

const (
	// CheckTypeRawQuery is a data quality check that uses a raw SQL query.
	CheckTypeRawQuery = "raw_query"
)

// DbqDataValidator is the interface that wraps the basic data validation methods.
type DbqDataValidator interface {
	// RunCheck runs a data quality check and returns the result.
	RunCheck(ctx context.Context, adapter DbqDataSourceAdapter, check *DataQualityCheck, dataset string, defaultWhere string) *ValidationResult
}

type DbqDataSourceAdapter interface {
	// InterpretDataQualityCheck generates a SQL query specific for datasource for a data quality check
	InterpretDataQualityCheck(check *DataQualityCheck, dataset string, defaultWhere string) (string, error)

	// ExecuteQuery executes the SQL query and returns the query result and flag if check passed or not
	ExecuteQuery(ctx context.Context, query string) (string, bool, error)
}

//

func NewDbqDataValidator(logger *slog.Logger) DbqDataValidator {
	return &DbqDataValidatorImpl{logger: logger}
}

type DbqDataValidatorImpl struct {
	logger *slog.Logger
}

func (d DbqDataValidatorImpl) RunCheck(ctx context.Context, adapter DbqDataSourceAdapter, check *DataQualityCheck, dataset string, defaultWhere string) *ValidationResult {
	result := &ValidationResult{
		CheckID: check.Expression,
		Pass:    false,
	}

	if adapter == nil {
		result.Error = "adapter is not provided"
		return result
	}

	checkQuery, err := adapter.InterpretDataQualityCheck(check, dataset, defaultWhere)
	if err != nil {
		result.Error = fmt.Sprintf("failed to generate query for check (%s)/(%s): %v", check.Expression, dataset, err)
		return result
	}

	d.logger.Debug("executing query for check",
		"check_expression", check.Expression,
		"check_query", checkQuery)

	startTime := time.Now()
	queryResult, pass, err := adapter.ExecuteQuery(ctx, checkQuery)
	elapsed := time.Since(startTime).Milliseconds()

	if err != nil {
		result.Error = fmt.Sprintf("failed to execute query for check (%s): %v", check.Expression, err)
		return result
	}

	d.logger.Debug("query completed in time",
		"check_expression", check.Expression,
		"duration_ms", elapsed)

	// todo: do actual check validation based on the query result
	result.QueryResultValue = queryResult
	result.Pass = pass

	return result
}
