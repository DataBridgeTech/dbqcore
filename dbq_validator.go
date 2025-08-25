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
	"strconv"
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
	CheckTypeSchemaCheck = "schema_check"
	CheckTypeRawQuery    = "raw_query"
)

// DbqDataValidator is the interface that wraps the basic data validation methods.
type DbqDataValidator interface {
	// RunCheck runs a data quality check and returns the result.
	RunCheck(ctx context.Context, adapter DbqDataSourceAdapter, check *DataQualityCheck, dataset string, defaultWhere string) *ValidationResult
}

type DbqDataSourceAdapter interface {
	// InterpretDataQualityCheck generates a SQL query specific for datasource for a data quality check
	InterpretDataQualityCheck(check *DataQualityCheck, dataset string, defaultWhere string) (string, error)

	// ExecuteQuery executes the SQL query and returns the query result
	ExecuteQuery(ctx context.Context, query string) (interface{}, error)
}

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
	queryResult, err := adapter.ExecuteQuery(ctx, checkQuery)
	elapsed := time.Since(startTime).Milliseconds()

	if err != nil {
		result.Error = fmt.Sprintf("failed to execute query for check (%s): %v", check.Expression, err)
		return result
	}

	d.logger.Debug("query completed in time",
		"check_expression", check.Expression,
		"duration_ms", elapsed)

	// convert queryResult to string for display
	switch v := queryResult.(type) {
	case []byte:
		result.QueryResultValue = string(v)
	default:
		result.QueryResultValue = fmt.Sprintf("%v", queryResult)
	}

	// Handle schema checks specially
	if check.SchemaCheck != nil {
		// For schema checks, we expect the count to match the expected value
		if check.SchemaCheck.ExpectColumnsOrdered != nil {
			// For expect_columns_ordered, the count should match the number of expected columns
			expectedCount := len(check.SchemaCheck.ExpectColumnsOrdered.ColumnsOrder)
			actualCount, err := d.convertToInt(queryResult)
			if err != nil || actualCount != expectedCount {
				result.Pass = false
				result.Error = fmt.Sprintf("Check failed: %s == %d (got: %v)", check.Expression, expectedCount, queryResult)
			} else {
				result.Pass = true
			}
		} else if check.SchemaCheck.ExpectColumns != nil {
			// For expect_columns, the count should match the number of expected columns
			expectedCount := len(check.SchemaCheck.ExpectColumns.Columns)
			actualCount, err := d.convertToInt(queryResult)
			if err != nil || actualCount != expectedCount {
				result.Pass = false
				result.Error = fmt.Sprintf("Check failed: %s == %d (got: %v)", check.Expression, expectedCount, queryResult)
			} else {
				result.Pass = true
			}
		} else if check.SchemaCheck.ColumnsNotPresent != nil {
			// For columns_not_present, the count should be 0 (no unwanted columns should exist)
			actualCount, err := d.convertToInt(queryResult)
			if err != nil {
				result.Pass = false
				result.Error = fmt.Sprintf("Check failed: %s invalid result: %v", check.Expression, queryResult)
			} else if actualCount > 0 {
				result.Pass = false
				result.Error = fmt.Sprintf("Check failed: %s found %d unwanted columns", check.Expression, actualCount)
			} else {
				result.Pass = true
			}
		} else {
			// Unknown schema check type, default to pass
			result.Pass = true
		}
	} else {
		// Regular checks use the existing validation logic
		result.Pass = d.validateResult(queryResult, check.ParsedCheck)
	}

	return result
}

// validateResult checks if the query result meets the check criteria
func (d DbqDataValidatorImpl) validateResult(queryResult interface{}, parsedCheck *CheckExpression) bool {
	if parsedCheck == nil {
		// If there's no parsed check, consider it a pass (raw queries without validation)
		return true
	}

	// If there's no operator, just check if we got a result (for functions like raw_query)
	if parsedCheck.Operator == "" {
		return queryResult != nil && fmt.Sprintf("%v", queryResult) != ""
	}

	// Convert query result to float64 for numeric comparisons
	actualValue, err := d.convertToFloat64(queryResult)
	if err != nil {
		d.logger.Warn("Failed to parse query result as number, treating as string comparison",
			"result", queryResult,
			"error", err)
		return d.validateStringResult(queryResult, parsedCheck)
	}

	switch parsedCheck.Operator {
	case "between":
		return d.validateBetweenRange(actualValue, parsedCheck.ThresholdValue)
	case ">":
		return d.validateGreaterThan(actualValue, parsedCheck.ThresholdValue)
	case ">=":
		return d.validateGreaterThanOrEqual(actualValue, parsedCheck.ThresholdValue)
	case "<":
		return d.validateLessThan(actualValue, parsedCheck.ThresholdValue)
	case "<=":
		return d.validateLessThanOrEqual(actualValue, parsedCheck.ThresholdValue)
	case "==", "=":
		return d.validateEqual(actualValue, parsedCheck.ThresholdValue)
	case "!=", "<>":
		return d.validateNotEqual(actualValue, parsedCheck.ThresholdValue)
	default:
		d.logger.Warn("Unknown operator, defaulting to true",
			"operator", parsedCheck.Operator)
		return true
	}
}

// validateStringResult handles string-based comparisons when numeric parsing fails
func (d DbqDataValidatorImpl) validateStringResult(queryResult interface{}, parsedCheck *CheckExpression) bool {
	var queryResultStr string
	switch v := queryResult.(type) {
	case []byte:
		queryResultStr = string(v)
	default:
		queryResultStr = fmt.Sprintf("%v", queryResult)
	}

	switch parsedCheck.Operator {
	case "==", "=":
		if thresholdStr, ok := parsedCheck.ThresholdValue.(string); ok {
			return queryResultStr == thresholdStr
		}
		return queryResultStr == fmt.Sprintf("%v", parsedCheck.ThresholdValue)
	case "!=", "<>":
		if thresholdStr, ok := parsedCheck.ThresholdValue.(string); ok {
			return queryResultStr != thresholdStr
		}
		return queryResultStr != fmt.Sprintf("%v", parsedCheck.ThresholdValue)
	default:
		d.logger.Warn("String comparison not supported for operator, defaulting to false",
			"operator", parsedCheck.Operator,
			"result", queryResultStr)
		return false
	}
}

// validateBetweenRange checks if value is within the specified range
func (d DbqDataValidatorImpl) validateBetweenRange(actualValue float64, thresholdValue interface{}) bool {
	betweenRange, ok := thresholdValue.(BetweenRange)
	if !ok {
		d.logger.Warn("Invalid threshold value for between operator",
			"value", thresholdValue)
		return false
	}

	minVal, err := d.convertToFloat64(betweenRange.Min)
	if err != nil {
		d.logger.Warn("Failed to convert min value to float64",
			"min", betweenRange.Min,
			"error", err)
		return false
	}

	maxVal, err := d.convertToFloat64(betweenRange.Max)
	if err != nil {
		d.logger.Warn("Failed to convert max value to float64",
			"max", betweenRange.Max,
			"error", err)
		return false
	}

	return actualValue >= minVal && actualValue <= maxVal
}

// validateGreaterThan checks if actual > threshold
func (d DbqDataValidatorImpl) validateGreaterThan(actualValue float64, thresholdValue interface{}) bool {
	threshold, err := d.convertToFloat64(thresholdValue)
	if err != nil {
		d.logger.Warn("Failed to convert threshold to float64 for > comparison",
			"threshold", thresholdValue,
			"error", err)
		return false
	}
	return actualValue > threshold
}

// validateGreaterThanOrEqual checks if actual >= threshold
func (d DbqDataValidatorImpl) validateGreaterThanOrEqual(actualValue float64, thresholdValue interface{}) bool {
	threshold, err := d.convertToFloat64(thresholdValue)
	if err != nil {
		d.logger.Warn("Failed to convert threshold to float64 for >= comparison",
			"threshold", thresholdValue,
			"error", err)
		return false
	}
	return actualValue >= threshold
}

// validateLessThan checks if actual < threshold
func (d DbqDataValidatorImpl) validateLessThan(actualValue float64, thresholdValue interface{}) bool {
	threshold, err := d.convertToFloat64(thresholdValue)
	if err != nil {
		d.logger.Warn("Failed to convert threshold to float64 for < comparison",
			"threshold", thresholdValue,
			"error", err)
		return false
	}
	return actualValue < threshold
}

// validateLessThanOrEqual checks if actual <= threshold
func (d DbqDataValidatorImpl) validateLessThanOrEqual(actualValue float64, thresholdValue interface{}) bool {
	threshold, err := d.convertToFloat64(thresholdValue)
	if err != nil {
		d.logger.Warn("Failed to convert threshold to float64 for <= comparison",
			"threshold", thresholdValue,
			"error", err)
		return false
	}
	return actualValue <= threshold
}

// validateEqual checks if actual == threshold
func (d DbqDataValidatorImpl) validateEqual(actualValue float64, thresholdValue interface{}) bool {
	threshold, err := d.convertToFloat64(thresholdValue)
	if err != nil {
		d.logger.Warn("Failed to convert threshold to float64 for == comparison",
			"threshold", thresholdValue,
			"error", err)
		return false
	}
	return actualValue == threshold
}

// validateNotEqual checks if actual != threshold
func (d DbqDataValidatorImpl) validateNotEqual(actualValue float64, thresholdValue interface{}) bool {
	threshold, err := d.convertToFloat64(thresholdValue)
	if err != nil {
		d.logger.Warn("Failed to convert threshold to float64 for != comparison",
			"threshold", thresholdValue,
			"error", err)
		return false
	}
	return actualValue != threshold
}

// convertToFloat64 converts various types to float64
func (d DbqDataValidatorImpl) convertToFloat64(value interface{}) (float64, error) {
	switch v := value.(type) {
	case float64:
		return v, nil
	case float32:
		return float64(v), nil
	case int:
		return float64(v), nil
	case int8:
		return float64(v), nil
	case int16:
		return float64(v), nil
	case int32:
		return float64(v), nil
	case int64:
		return float64(v), nil
	case uint:
		return float64(v), nil
	case uint8:
		return float64(v), nil
	case uint16:
		return float64(v), nil
	case uint32:
		return float64(v), nil
	case uint64:
		return float64(v), nil
	case string:
		return strconv.ParseFloat(v, 64)
	case []byte:
		// Handle byte arrays from PostgreSQL/MySQL drivers
		return strconv.ParseFloat(string(v), 64)
	default:
		return 0, fmt.Errorf("unsupported type: %T", value)
	}
}

// convertToInt converts various types to int
func (d DbqDataValidatorImpl) convertToInt(value interface{}) (int, error) {
	switch v := value.(type) {
	case int:
		return v, nil
	case int8:
		return int(v), nil
	case int16:
		return int(v), nil
	case int32:
		return int(v), nil
	case int64:
		return int(v), nil
	case uint:
		return int(v), nil
	case uint8:
		return int(v), nil
	case uint16:
		return int(v), nil
	case uint32:
		return int(v), nil
	case uint64:
		return int(v), nil
	case float32:
		return int(v), nil
	case float64:
		return int(v), nil
	case string:
		return strconv.Atoi(v)
	case []byte:
		// Handle byte arrays from PostgreSQL/MySQL drivers
		return strconv.Atoi(string(v))
	default:
		return 0, fmt.Errorf("unsupported type: %T", value)
	}
}
