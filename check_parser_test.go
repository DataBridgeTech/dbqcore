package dbqcore

import (
	"reflect"
	"testing"
)

func TestParseCheckExpression(t *testing.T) {
	tests := []struct {
		name        string
		expression  string
		expected    *CheckExpression
		expectError bool
	}{
		{
			name:       "row_count between (no parentheses)",
			expression: "row_count between 1 and 100",
			expected: &CheckExpression{
				FunctionName:       "row_count",
				FunctionParameters: []string{},
				Scope:              ScopeTable,
				Operator:           "between",
				ThresholdValue:     BetweenRange{Min: 1, Max: 100},
			},
		},
		{
			name:       "row_count() between (empty parentheses)",
			expression: "row_count() between 1 and 100",
			expected: &CheckExpression{
				FunctionName:       "row_count",
				FunctionParameters: []string{},
				Scope:              ScopeTable,
				Operator:           "between",
				ThresholdValue:     BetweenRange{Min: 1, Max: 100},
			},
		},
		{
			name:       "not_null function only",
			expression: "not_null(col_name)",
			expected: &CheckExpression{
				FunctionName:       "not_null",
				FunctionParameters: []string{"col_name"},
				Scope:              ScopeColumn,
				Operator:           "",
				ThresholdValue:     nil,
			},
		},
		{
			name:       "freshness with time duration",
			expression: "freshness(pickup_datetime) < 3d",
			expected: &CheckExpression{
				FunctionName:       "freshness",
				FunctionParameters: []string{"pickup_datetime"},
				Scope:              ScopeColumn,
				Operator:           "<",
				ThresholdValue:     "3d",
			},
		},
		{
			name:       "uniqueness function only",
			expression: "uniqueness(col_2)",
			expected: &CheckExpression{
				FunctionName:       "uniqueness",
				FunctionParameters: []string{"col_2"},
				Scope:              ScopeColumn,
				Operator:           "",
				ThresholdValue:     nil,
			},
		},
		{
			name:       "stddev with large number",
			expression: "stddev(trip_distance) < 100000",
			expected: &CheckExpression{
				FunctionName:       "stddev",
				FunctionParameters: []string{"trip_distance"},
				Scope:              ScopeColumn,
				Operator:           "<",
				ThresholdValue:     100000,
			},
		},
		{
			name:       "sum with <= operator",
			expression: "sum(fare_amount) <= 10000000",
			expected: &CheckExpression{
				FunctionName:       "sum",
				FunctionParameters: []string{"fare_amount"},
				Scope:              ScopeColumn,
				Operator:           "<=",
				ThresholdValue:     10000000,
			},
		},
		{
			name:       "min with > operator",
			expression: "min(price) > 0",
			expected: &CheckExpression{
				FunctionName:       "min",
				FunctionParameters: []string{"price"},
				Scope:              ScopeColumn,
				Operator:           ">",
				ThresholdValue:     0,
			},
		},
		{
			name:       "max with large number",
			expression: "max(price) < 100000000",
			expected: &CheckExpression{
				FunctionName:       "max",
				FunctionParameters: []string{"price"},
				Scope:              ScopeColumn,
				Operator:           "<",
				ThresholdValue:     100000000,
			},
		},
		{
			name:       "stddev with specific threshold",
			expression: "stddev(price) < 150000",
			expected: &CheckExpression{
				FunctionName:       "stddev",
				FunctionParameters: []string{"price"},
				Scope:              ScopeColumn,
				Operator:           "<",
				ThresholdValue:     150000,
			},
		},
		{
			name:       "float threshold value",
			expression: "avg(price) > 1000.50",
			expected: &CheckExpression{
				FunctionName:       "avg",
				FunctionParameters: []string{"price"},
				Scope:              ScopeColumn,
				Operator:           ">",
				ThresholdValue:     1000.50,
			},
		},
		{
			name:       "between with float values",
			expression: "avg(price) between 100.5 and 200.75",
			expected: &CheckExpression{
				FunctionName:       "avg",
				FunctionParameters: []string{"price"},
				Scope:              ScopeColumn,
				Operator:           "between",
				ThresholdValue:     BetweenRange{Min: 100.5, Max: 200.75},
			},
		},
		{
			name:       ">= operator",
			expression: "count(id) >= 5",
			expected: &CheckExpression{
				FunctionName:       "count",
				FunctionParameters: []string{"id"},
				Scope:              ScopeColumn,
				Operator:           ">=",
				ThresholdValue:     5,
			},
		},
		{
			name:       "== operator",
			expression: "status(active) == 1",
			expected: &CheckExpression{
				FunctionName:       "status",
				FunctionParameters: []string{"active"},
				Scope:              ScopeColumn,
				Operator:           "==",
				ThresholdValue:     1,
			},
		},
		{
			name:       "string threshold value",
			expression: "status(state) == active",
			expected: &CheckExpression{
				FunctionName:       "status",
				FunctionParameters: []string{"state"},
				Scope:              ScopeColumn,
				Operator:           "==",
				ThresholdValue:     "active",
			},
		},
		{
			name:       "function with three parameters",
			expression: "custom_func(col1, col2, val) < 100",
			expected: &CheckExpression{
				FunctionName:       "custom_func",
				FunctionParameters: []string{"col1", "col2", "val"},
				Scope:              ScopeColumn,
				Operator:           "<",
				ThresholdValue:     100,
			},
		},
		{
			name:        "empty expression",
			expression:  "",
			expected:    nil,
			expectError: true,
		},
		{
			name:        "invalid format",
			expression:  "invalid expression format",
			expected:    nil,
			expectError: true,
		},
		{
			name:        "between with missing 'and'",
			expression:  "row_count between 1 100",
			expected:    nil,
			expectError: true,
		},
		{
			name:        "malformed operator",
			expression:  "count(id) ??? 5",
			expected:    nil,
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := ParseCheckExpression(tt.expression)

			if tt.expectError {
				if err == nil {
					t.Errorf("ParseCheckExpression() expected error but got none")
				}
				return
			}

			if err != nil {
				t.Errorf("ParseCheckExpression() unexpected error: %v", err)
				return
			}

			// Check each field individually
			if result.FunctionName != tt.expected.FunctionName {
				t.Errorf("FunctionName = %s, expected %s", result.FunctionName, tt.expected.FunctionName)
			}
			if result.Scope != tt.expected.Scope {
				t.Errorf("Scope = %s, expected %s", result.Scope, tt.expected.Scope)
			}
			if result.Operator != tt.expected.Operator {
				t.Errorf("Operator = %s, expected %s", result.Operator, tt.expected.Operator)
			}
			if !reflect.DeepEqual(result.FunctionParameters, tt.expected.FunctionParameters) {
				t.Errorf("FunctionParameters = %v, expected %v", result.FunctionParameters, tt.expected.FunctionParameters)
			}

			// Special handling for BetweenRange comparison
			if expectedRange, ok := tt.expected.ThresholdValue.(BetweenRange); ok {
				if resultRange, ok := result.ThresholdValue.(BetweenRange); ok {
					if !reflect.DeepEqual(resultRange.Min, expectedRange.Min) || !reflect.DeepEqual(resultRange.Max, expectedRange.Max) {
						t.Errorf("BetweenRange = {Min:%v, Max:%v}, expected {Min:%v, Max:%v}",
							resultRange.Min, resultRange.Max, expectedRange.Min, expectedRange.Max)
					}
				} else {
					t.Errorf("ThresholdValue = %T, expected BetweenRange", result.ThresholdValue)
				}
			} else if !reflect.DeepEqual(result.ThresholdValue, tt.expected.ThresholdValue) {
				t.Errorf("ThresholdValue = %v, expected %v", result.ThresholdValue, tt.expected.ThresholdValue)
			}
		})
	}
}

func TestParseParameters(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected []string
	}{
		{
			name:     "empty parameters",
			input:    "",
			expected: []string{},
		},
		{
			name:     "single parameter",
			input:    "col_name",
			expected: []string{"col_name"},
		},
		{
			name:     "multiple parameters",
			input:    "col1, col2, val",
			expected: []string{"col1", "col2", "val"},
		},
		{
			name:     "parameters with extra spaces",
			input:    " col1 ,  col2  , val ",
			expected: []string{"col1", "col2", "val"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := parseParameters(tt.input)
			if !reflect.DeepEqual(result, tt.expected) {
				t.Errorf("parseParameters() = %v, expected %v", result, tt.expected)
			}
		})
	}
}

func TestParseValue(t *testing.T) {
	tests := []struct {
		name        string
		input       string
		expected    interface{}
		expectError bool
	}{
		{
			name:     "integer value",
			input:    "100",
			expected: 100,
		},
		{
			name:     "float value",
			input:    "100.50",
			expected: 100.50,
		},
		{
			name:     "time duration",
			input:    "3d",
			expected: "3d",
		},
		{
			name:     "string value",
			input:    "active",
			expected: "active",
		},
		{
			name:     "zero value",
			input:    "0",
			expected: 0,
		},
		{
			name:        "empty value",
			input:       "",
			expected:    nil,
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := parseValue(tt.input)

			if tt.expectError {
				if err == nil {
					t.Errorf("parseValue() expected error but got none")
				}
				return
			}

			if err != nil {
				t.Errorf("parseValue() unexpected error: %v", err)
				return
			}

			if !reflect.DeepEqual(result, tt.expected) {
				t.Errorf("parseValue() = %v (%T), expected %v (%T)", result, result, tt.expected, tt.expected)
			}
		})
	}
}

func TestInferScope(t *testing.T) {
	tests := []struct {
		name         string
		functionName string
		expected     CheckScope
	}{
		{
			name:         "table scope function",
			functionName: "row_count",
			expected:     ScopeTable,
		},
		{
			name:         "column scope function - not_null",
			functionName: "not_null",
			expected:     ScopeColumn,
		},
		{
			name:         "column scope function - uniqueness",
			functionName: "uniqueness",
			expected:     ScopeColumn,
		},
		{
			name:         "column scope function - freshness",
			functionName: "freshness",
			expected:     ScopeColumn,
		},
		{
			name:         "column scope function - min",
			functionName: "min",
			expected:     ScopeColumn,
		},
		{
			name:         "column scope function - max",
			functionName: "max",
			expected:     ScopeColumn,
		},
		{
			name:         "column scope function - sum",
			functionName: "sum",
			expected:     ScopeColumn,
		},
		{
			name:         "column scope function - stddev",
			functionName: "stddev",
			expected:     ScopeColumn,
		},
		{
			name:         "schema scope function",
			functionName: "raw_query",
			expected:     ScopeTable,
		},
		{
			name:         "unknown function defaults to column",
			functionName: "unknown_func",
			expected:     ScopeColumn,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := inferScope(tt.functionName)
			if result != tt.expected {
				t.Errorf("inferScope() = %v, expected %v", result, tt.expected)
			}
		})
	}
}
