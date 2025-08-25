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
	"io"
	"log/slog"
	"testing"
)

// MockAdapter for testing the validation logic
type MockAdapter struct {
	queryResult interface{}
	queryError  error
}

func (m *MockAdapter) InterpretDataQualityCheck(check *DataQualityCheck, dataset string, defaultWhere string) (string, error) {
	return "SELECT COUNT(*) FROM " + dataset, nil
}

func (m *MockAdapter) ExecuteQuery(ctx context.Context, query string) (interface{}, error) {
	return m.queryResult, m.queryError
}

func TestDbqDataValidator_RunCheck_Integration(t *testing.T) {
	validator := NewDbqDataValidator(slog.New(slog.NewTextHandler(io.Discard, nil)))

	tests := []struct {
		name         string
		check        *DataQualityCheck
		queryResult  interface{}
		expectedPass bool
	}{
		{
			name: "row_count > 100 - pass",
			check: &DataQualityCheck{
				Expression: "row_count > 100",
				ParsedCheck: &CheckExpression{
					FunctionName:   "row_count",
					Operator:       ">",
					ThresholdValue: 100,
				},
			},
			queryResult:  150,
			expectedPass: true,
		},
		{
			name: "row_count > 100 - fail",
			check: &DataQualityCheck{
				Expression: "row_count > 100",
				ParsedCheck: &CheckExpression{
					FunctionName:   "row_count",
					Operator:       ">",
					ThresholdValue: 100,
				},
			},
			queryResult:  50,
			expectedPass: false,
		},
		{
			name: "avg between 3.0 and 5.0 - pass",
			check: &DataQualityCheck{
				Expression: "avg(rating) between 3.0 and 5.0",
				ParsedCheck: &CheckExpression{
					FunctionName:   "avg",
					Operator:       "between",
					ThresholdValue: BetweenRange{Min: 3.0, Max: 5.0},
				},
			},
			queryResult:  4.2,
			expectedPass: true,
		},
		{
			name: "avg between 3.0 and 5.0 - fail",
			check: &DataQualityCheck{
				Expression: "avg(rating) between 3.0 and 5.0",
				ParsedCheck: &CheckExpression{
					FunctionName:   "avg",
					Operator:       "between",
					ThresholdValue: BetweenRange{Min: 3.0, Max: 5.0},
				},
			},
			queryResult:  6.0,
			expectedPass: false,
		},
		{
			name: "freshness < 3600 - pass",
			check: &DataQualityCheck{
				Expression: "freshness(last_updated) < 3600",
				ParsedCheck: &CheckExpression{
					FunctionName:   "freshness",
					Operator:       "<",
					ThresholdValue: 3600,
				},
			},
			queryResult:  1800,
			expectedPass: true,
		},
		{
			name: "freshness < 3600 - fail",
			check: &DataQualityCheck{
				Expression: "freshness(last_updated) < 3600",
				ParsedCheck: &CheckExpression{
					FunctionName:   "freshness",
					Operator:       "<",
					ThresholdValue: 3600,
				},
			},
			queryResult:  7200,
			expectedPass: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			adapter := &MockAdapter{
				queryResult: tt.queryResult,
				queryError:  nil,
			}

			result := validator.RunCheck(context.Background(), adapter, tt.check, "test_table", "")

			if result.Error != "" {
				t.Errorf("Unexpected error: %s", result.Error)
			}

			if result.Pass != tt.expectedPass {
				t.Errorf("Expected Pass = %v, got %v", tt.expectedPass, result.Pass)
			}

			expectedStr := fmt.Sprintf("%v", tt.queryResult)
			if result.QueryResultValue != expectedStr {
				t.Errorf("Expected QueryResultValue = %s, got %s", expectedStr, result.QueryResultValue)
			}

			if result.CheckID != tt.check.Expression {
				t.Errorf("Expected CheckID = %s, got %s", tt.check.Expression, result.CheckID)
			}
		})
	}
}
