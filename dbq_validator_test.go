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
	"io"
	"log/slog"
	"testing"
)

func TestDbqDataValidatorImpl_validateResult(t *testing.T) {
	validator := DbqDataValidatorImpl{
		logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	}

	tests := []struct {
		name         string
		queryResult  string
		parsedCheck  *CheckExpression
		expectedPass bool
	}{
		{
			name:         "nil parsed check should pass",
			queryResult:  "100",
			parsedCheck:  nil,
			expectedPass: true,
		},
		{
			name:        "no operator with result should pass",
			queryResult: "100",
			parsedCheck: &CheckExpression{
				FunctionName: "row_count",
				Operator:     "",
			},
			expectedPass: true,
		},
		{
			name:        "no operator with empty result should fail",
			queryResult: "",
			parsedCheck: &CheckExpression{
				FunctionName: "row_count",
				Operator:     "",
			},
			expectedPass: false,
		},
		{
			name:        "greater than - pass",
			queryResult: "150",
			parsedCheck: &CheckExpression{
				FunctionName:   "row_count",
				Operator:       ">",
				ThresholdValue: 100,
			},
			expectedPass: true,
		},
		{
			name:        "greater than - fail",
			queryResult: "50",
			parsedCheck: &CheckExpression{
				FunctionName:   "row_count",
				Operator:       ">",
				ThresholdValue: 100,
			},
			expectedPass: false,
		},
		{
			name:        "greater than or equal - pass equal",
			queryResult: "100",
			parsedCheck: &CheckExpression{
				FunctionName:   "avg",
				Operator:       ">=",
				ThresholdValue: 100,
			},
			expectedPass: true,
		},
		{
			name:        "greater than or equal - pass greater",
			queryResult: "150",
			parsedCheck: &CheckExpression{
				FunctionName:   "avg",
				Operator:       ">=",
				ThresholdValue: 100,
			},
			expectedPass: true,
		},
		{
			name:        "less than - pass",
			queryResult: "50",
			parsedCheck: &CheckExpression{
				FunctionName:   "freshness",
				Operator:       "<",
				ThresholdValue: 100,
			},
			expectedPass: true,
		},
		{
			name:        "less than - fail",
			queryResult: "150",
			parsedCheck: &CheckExpression{
				FunctionName:   "freshness",
				Operator:       "<",
				ThresholdValue: 100,
			},
			expectedPass: false,
		},
		{
			name:        "less than or equal - pass equal",
			queryResult: "100",
			parsedCheck: &CheckExpression{
				FunctionName:   "max",
				Operator:       "<=",
				ThresholdValue: 100,
			},
			expectedPass: true,
		},
		{
			name:        "less than or equal - pass less",
			queryResult: "50",
			parsedCheck: &CheckExpression{
				FunctionName:   "max",
				Operator:       "<=",
				ThresholdValue: 100,
			},
			expectedPass: true,
		},
		{
			name:        "equal - pass",
			queryResult: "100",
			parsedCheck: &CheckExpression{
				FunctionName:   "sum",
				Operator:       "==",
				ThresholdValue: 100,
			},
			expectedPass: true,
		},
		{
			name:        "equal - fail",
			queryResult: "150",
			parsedCheck: &CheckExpression{
				FunctionName:   "sum",
				Operator:       "==",
				ThresholdValue: 100,
			},
			expectedPass: false,
		},
		{
			name:        "not equal - pass",
			queryResult: "150",
			parsedCheck: &CheckExpression{
				FunctionName:   "stddev",
				Operator:       "!=",
				ThresholdValue: 100,
			},
			expectedPass: true,
		},
		{
			name:        "not equal - fail",
			queryResult: "100",
			parsedCheck: &CheckExpression{
				FunctionName:   "stddev",
				Operator:       "!=",
				ThresholdValue: 100,
			},
			expectedPass: false,
		},
		{
			name:        "between - pass within range",
			queryResult: "75",
			parsedCheck: &CheckExpression{
				FunctionName:   "avg",
				Operator:       "between",
				ThresholdValue: BetweenRange{Min: 50, Max: 100},
			},
			expectedPass: true,
		},
		{
			name:        "between - pass at min boundary",
			queryResult: "50",
			parsedCheck: &CheckExpression{
				FunctionName:   "avg",
				Operator:       "between",
				ThresholdValue: BetweenRange{Min: 50, Max: 100},
			},
			expectedPass: true,
		},
		{
			name:        "between - pass at max boundary",
			queryResult: "100",
			parsedCheck: &CheckExpression{
				FunctionName:   "avg",
				Operator:       "between",
				ThresholdValue: BetweenRange{Min: 50, Max: 100},
			},
			expectedPass: true,
		},
		{
			name:        "between - fail below range",
			queryResult: "25",
			parsedCheck: &CheckExpression{
				FunctionName:   "avg",
				Operator:       "between",
				ThresholdValue: BetweenRange{Min: 50, Max: 100},
			},
			expectedPass: false,
		},
		{
			name:        "between - fail above range",
			queryResult: "150",
			parsedCheck: &CheckExpression{
				FunctionName:   "avg",
				Operator:       "between",
				ThresholdValue: BetweenRange{Min: 50, Max: 100},
			},
			expectedPass: false,
		},
		{
			name:        "between with float values - pass",
			queryResult: "3.5",
			parsedCheck: &CheckExpression{
				FunctionName:   "avg",
				Operator:       "between",
				ThresholdValue: BetweenRange{Min: 3.0, Max: 5.0},
			},
			expectedPass: true,
		},
		{
			name:        "string comparison equal - pass",
			queryResult: "active",
			parsedCheck: &CheckExpression{
				FunctionName:   "status_check",
				Operator:       "==",
				ThresholdValue: "active",
			},
			expectedPass: true,
		},
		{
			name:        "string comparison equal - fail",
			queryResult: "inactive",
			parsedCheck: &CheckExpression{
				FunctionName:   "status_check",
				Operator:       "==",
				ThresholdValue: "active",
			},
			expectedPass: false,
		},
		{
			name:        "string comparison not equal - pass",
			queryResult: "inactive",
			parsedCheck: &CheckExpression{
				FunctionName:   "status_check",
				Operator:       "!=",
				ThresholdValue: "active",
			},
			expectedPass: true,
		},
		{
			name:        "unknown operator - default to true",
			queryResult: "100",
			parsedCheck: &CheckExpression{
				FunctionName:   "custom",
				Operator:       "~=",
				ThresholdValue: 100,
			},
			expectedPass: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := validator.validateResult(tt.queryResult, tt.parsedCheck)
			if result != tt.expectedPass {
				t.Errorf("validateResult() = %v, expected %v", result, tt.expectedPass)
			}
		})
	}
}

func TestDbqDataValidatorImpl_convertToFloat64(t *testing.T) {
	validator := DbqDataValidatorImpl{
		logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	}

	tests := []struct {
		name        string
		input       interface{}
		expected    float64
		expectError bool
	}{
		{"float64", float64(123.45), 123.45, false},
		{"float32", float32(123.45), float64(float32(123.45)), false},
		{"int", int(123), 123.0, false},
		{"int8", int8(123), 123.0, false},
		{"int16", int16(123), 123.0, false},
		{"int32", int32(123), 123.0, false},
		{"int64", int64(123), 123.0, false},
		{"uint", uint(123), 123.0, false},
		{"uint8", uint8(123), 123.0, false},
		{"uint16", uint16(123), 123.0, false},
		{"uint32", uint32(123), 123.0, false},
		{"uint64", uint64(123), 123.0, false},
		{"string valid", "123.45", 123.45, false},
		{"string invalid", "not-a-number", 0, true},
		{"unsupported type", []int{1, 2, 3}, 0, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := validator.convertToFloat64(tt.input)

			if tt.expectError {
				if err == nil {
					t.Errorf("convertToFloat64() expected error but got none")
				}
			} else {
				if err != nil {
					t.Errorf("convertToFloat64() unexpected error: %v", err)
				}
				if result != tt.expected {
					t.Errorf("convertToFloat64() = %v, expected %v", result, tt.expected)
				}
			}
		})
	}
}
