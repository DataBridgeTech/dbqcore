package dbqcore

import (
	"os"
	"reflect"
	"testing"

	"gopkg.in/yaml.v3"
)

func TestLoadChecksFileConfig(t *testing.T) {
	tests := []struct {
		name     string
		yamlData string
		expected *ChecksFileConfig
		wantErr  bool
	}{
		{
			name: "basic config with simple checks",
			yamlData: `
version: 1
rules:
  - dataset: ch@[nyc_taxi.trips_small]
    where: "pickup_datetime > '2014-01-01'"
    checks:
      - not_null(col_name)
      - uniqueness(col_2)
      - freshness(pickup_datetime) < 3d
`,
			expected: &ChecksFileConfig{
				Version: "1",
				Rules: []ValidationRule{
					{
						Dataset: "ch@[nyc_taxi.trips_small]",
						Where:   "pickup_datetime > '2014-01-01'",
						Checks: []DataQualityCheck{
							{
								Expression: "not_null(col_name)",
								ParsedCheck: &CheckExpression{
									FunctionName:       "not_null",
									FunctionParameters: []string{"col_name"},
									Scope:              ScopeColumn,
									Operator:           "",
									ThresholdValue:     nil,
								},
							},
							{
								Expression: "uniqueness(col_2)",
								ParsedCheck: &CheckExpression{
									FunctionName:       "uniqueness",
									FunctionParameters: []string{"col_2"},
									Scope:              ScopeColumn,
									Operator:           "",
									ThresholdValue:     nil,
								},
							},
							{
								Expression: "freshness(pickup_datetime) < 3d",
								ParsedCheck: &CheckExpression{
									FunctionName:       "freshness",
									FunctionParameters: []string{"pickup_datetime"},
									Scope:              ScopeColumn,
									Operator:           "<",
									ThresholdValue:     "3d",
								},
							},
						},
					},
				},
			},
		},
		{
			name: "config with between checks and descriptions",
			yamlData: `
version: 1
rules:
  - dataset: pg@[public.land_registry_price_paid_uk]
    where: "transfer_date >= '2025-02-01'"
    checks:
      - row_count() between 1 and 100:
          desc: "some explanation goes here"
      - sum(col) < 1000:
          desc: "some explanation goes here"
          on_fail: warn
      - stddev(price) < 150000:
          desc: "Price standard deviation should be reasonable."
`,
			expected: &ChecksFileConfig{
				Version: "1",
				Rules: []ValidationRule{
					{
						Dataset: "pg@[public.land_registry_price_paid_uk]",
						Where:   "transfer_date >= '2025-02-01'",
						Checks: []DataQualityCheck{
							{
								Expression:  "row_count() between 1 and 100",
								Description: "some explanation goes here",
								ParsedCheck: &CheckExpression{
									FunctionName:       "row_count",
									FunctionParameters: []string{},
									Scope:              ScopeTable,
									Operator:           "between",
									ThresholdValue:     BetweenRange{Min: 1, Max: 100},
								},
							},
							{
								Expression:  "sum(col) < 1000",
								Description: "some explanation goes here",
								OnFail:      OnFailActionWarn,
								ParsedCheck: &CheckExpression{
									FunctionName:       "sum",
									FunctionParameters: []string{"col"},
									Scope:              ScopeColumn,
									Operator:           "<",
									ThresholdValue:     1000,
								},
							},
							{
								Expression:  "stddev(price) < 150000",
								Description: "Price standard deviation should be reasonable.",
								ParsedCheck: &CheckExpression{
									FunctionName:       "stddev",
									FunctionParameters: []string{"price"},
									Scope:              ScopeColumn,
									Operator:           "<",
									ThresholdValue:     150000,
								},
							},
						},
					},
				},
			},
		},
		{
			name: "config with raw query",
			yamlData: `
version: 1
rules:
  - dataset: ch@[nyc_taxi.trips_small]
    checks:
      - raw_query:
          desc: "special kind of query"
          query: "select countIf(trip_distance == 0) from {{dataset}}"
          on_fail: warn
`,
			expected: &ChecksFileConfig{
				Version: "1",
				Rules: []ValidationRule{
					{
						Dataset: "ch@[nyc_taxi.trips_small]",
						Checks: []DataQualityCheck{
							{
								Expression:  "raw_query",
								Description: "special kind of query",
								Query:       "select countIf(trip_distance == 0) from {{dataset}}",
								OnFail:      OnFailActionWarn,
								ParsedCheck: &CheckExpression{
									FunctionName:       "raw_query",
									FunctionParameters: []string{},
									Scope:              ScopeTable,
									Operator:           "",
									ThresholdValue:     nil,
								},
							},
						},
					},
				},
			},
		},
		{
			name: "mixed check types",
			yamlData: `
version: 1
rules:
  - dataset: mysql@[ecommerce.orders]
    where: "created_at >= CURRENT_DATE - INTERVAL 30 DAY"
    checks:
      - row_count between 100 and 10000
      - not_null(customer_id)
      - sum(total_amount) > 0
      - max(total_amount) < 50000:
          desc: "No single order should exceed 50k"
          on_fail: error
      - avg(item_count) between 1.0 and 10.5:
          desc: "Average items per order should be reasonable"
      - raw_query:
          desc: "Check for duplicate order IDs"
          query: "SELECT COUNT(*) - COUNT(DISTINCT order_id) FROM {{dataset}}"
`,
			expected: &ChecksFileConfig{
				Version: "1",
				Rules: []ValidationRule{
					{
						Dataset: "mysql@[ecommerce.orders]",
						Where:   "created_at >= CURRENT_DATE - INTERVAL 30 DAY",
						Checks: []DataQualityCheck{
							{
								Expression: "row_count between 100 and 10000",
								ParsedCheck: &CheckExpression{
									FunctionName:       "row_count",
									FunctionParameters: []string{},
									Scope:              ScopeTable,
									Operator:           "between",
									ThresholdValue:     BetweenRange{Min: 100, Max: 10000},
								},
							},
							{
								Expression: "not_null(customer_id)",
								ParsedCheck: &CheckExpression{
									FunctionName:       "not_null",
									FunctionParameters: []string{"customer_id"},
									Scope:              ScopeColumn,
									Operator:           "",
									ThresholdValue:     nil,
								},
							},
							{
								Expression: "sum(total_amount) > 0",
								ParsedCheck: &CheckExpression{
									FunctionName:       "sum",
									FunctionParameters: []string{"total_amount"},
									Scope:              ScopeColumn,
									Operator:           ">",
									ThresholdValue:     0,
								},
							},
							{
								Expression:  "max(total_amount) < 50000",
								Description: "No single order should exceed 50k",
								OnFail:      OnFailActionError,
								ParsedCheck: &CheckExpression{
									FunctionName:       "max",
									FunctionParameters: []string{"total_amount"},
									Scope:              ScopeColumn,
									Operator:           "<",
									ThresholdValue:     50000,
								},
							},
							{
								Expression:  "avg(item_count) between 1.0 and 10.5",
								Description: "Average items per order should be reasonable",
								ParsedCheck: &CheckExpression{
									FunctionName:       "avg",
									FunctionParameters: []string{"item_count"},
									Scope:              ScopeColumn,
									Operator:           "between",
									ThresholdValue:     BetweenRange{Min: 1.0, Max: 10.5},
								},
							},
							{
								Expression:  "raw_query",
								Description: "Check for duplicate order IDs",
								Query:       "SELECT COUNT(*) - COUNT(DISTINCT order_id) FROM {{dataset}}",
								ParsedCheck: &CheckExpression{
									FunctionName:       "raw_query",
									FunctionParameters: []string{},
									Scope:              ScopeTable,
									Operator:           "",
									ThresholdValue:     nil,
								},
							},
						},
					},
				},
			},
		},
		{
			name: "invalid expression format",
			yamlData: `
version: 1
rules:
  - dataset: invalid
    checks:
      - "invalid expression ??? format"
`,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tmpFile, err := os.CreateTemp("", "dbqcore-test-config-*.yml")
			if err != nil {
				t.Fatalf("Failed to create temp file: %v", err)
			}
			defer os.Remove(tmpFile.Name())
			defer tmpFile.Close()

			if _, err := tmpFile.WriteString(tt.yamlData); err != nil {
				t.Fatalf("Failed to write test data: %v", err)
			}
			tmpFile.Close()

			result, err := LoadChecksFileConfig(tmpFile.Name())

			if tt.wantErr {
				if err == nil {
					t.Error("LoadChecksFileConfig() expected error but got none")
				}
				return
			}

			if err != nil {
				t.Errorf("LoadChecksFileConfig() unexpected error: %v", err)
				return
			}

			if !configsEqual(result, tt.expected) {
				t.Errorf("LoadChecksFileConfig() = %+v, expected %+v", result, tt.expected)
			}
		})
	}
}

func configsEqual(a, b *ChecksFileConfig) bool {
	if a.Version != b.Version || len(a.Rules) != len(b.Rules) {
		return false
	}

	for i, ruleA := range a.Rules {
		ruleB := b.Rules[i]
		if ruleA.Dataset != ruleB.Dataset || ruleA.Where != ruleB.Where ||
			len(ruleA.Checks) != len(ruleB.Checks) {
			return false
		}

		for j, checkA := range ruleA.Checks {
			checkB := ruleB.Checks[j]
			if !checksEqual(&checkA, &checkB) {
				return false
			}
		}
	}

	return true
}

func checksEqual(a, b *DataQualityCheck) bool {
	if a.Expression != b.Expression ||
		a.Description != b.Description ||
		a.OnFail != b.OnFail ||
		a.Query != b.Query {
		return false
	}

	if a.ParsedCheck == nil && b.ParsedCheck == nil {
		return true
	}
	if a.ParsedCheck == nil || b.ParsedCheck == nil {
		return false
	}

	return checkExpressionsEqual(a.ParsedCheck, b.ParsedCheck)
}

func checkExpressionsEqual(a, b *CheckExpression) bool {
	if a.FunctionName != b.FunctionName ||
		a.Scope != b.Scope ||
		a.Operator != b.Operator {
		return false
	}

	if !reflect.DeepEqual(a.FunctionParameters, b.FunctionParameters) {
		return false
	}

	if aRange, aOk := a.ThresholdValue.(BetweenRange); aOk {
		if bRange, bOk := b.ThresholdValue.(BetweenRange); bOk {
			return reflect.DeepEqual(aRange.Min, bRange.Min) &&
				reflect.DeepEqual(aRange.Max, bRange.Max)
		}
		return false
	}

	return reflect.DeepEqual(a.ThresholdValue, b.ThresholdValue)
}

func TestDataQualityCheckUnmarshalYAML_EdgeCases(t *testing.T) {
	tests := []struct {
		name     string
		yamlData string
		expected []DataQualityCheck
		wantErr  bool
	}{
		{
			name: "mixed check formats",
			yamlData: `
checks:
  - not_null(id)
  - row_count > 0:
      desc: "Table should not be empty"
  - max(price) < 1000000:
      on_fail: error
`,
			expected: []DataQualityCheck{
				{
					Expression: "not_null(id)",
					ParsedCheck: &CheckExpression{
						FunctionName:       "not_null",
						FunctionParameters: []string{"id"},
						Scope:              ScopeColumn,
						Operator:           "",
						ThresholdValue:     nil,
					},
				},
				{
					Expression:  "row_count > 0",
					Description: "Table should not be empty",
					ParsedCheck: &CheckExpression{
						FunctionName:       "row_count",
						FunctionParameters: []string{},
						Scope:              ScopeTable,
						Operator:           ">",
						ThresholdValue:     0,
					},
				},
				{
					Expression: "max(price) < 1000000",
					OnFail:     OnFailActionError,
					ParsedCheck: &CheckExpression{
						FunctionName:       "max",
						FunctionParameters: []string{"price"},
						Scope:              ScopeColumn,
						Operator:           "<",
						ThresholdValue:     1000000,
					},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var config struct {
				Checks []DataQualityCheck `yaml:"checks"`
			}

			if err := yaml.Unmarshal([]byte(tt.yamlData), &config); err != nil {
				if tt.wantErr {
					return
				}
				t.Fatalf("Failed to unmarshal: %v", err)
			}

			if tt.wantErr {
				t.Error("Expected error but got none")
				return
			}

			if len(config.Checks) != len(tt.expected) {
				t.Errorf("Got %d checks, expected %d", len(config.Checks), len(tt.expected))
				return
			}

			for i, check := range config.Checks {
				if !checksEqual(&check, &tt.expected[i]) {
					t.Errorf("Check %d: got %+v, expected %+v", i, check, tt.expected[i])
				}
			}
		})
	}
}
