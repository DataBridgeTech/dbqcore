package dbqcore

import (
	"testing"

	"gopkg.in/yaml.v3"
)

func TestSchemaCheckConfigUnmarshal(t *testing.T) {
	tests := []struct {
		name          string
		yamlContent   string
		expectedCheck DataQualityCheck
		expectError   bool
	}{
		{
			name: "schema_check with expect_columns_ordered",
			yamlContent: `
schema_check:
  expect_columns_ordered:
    columns_order: [id, name, email, created_at]
desc: "Validate column order"
on_fail: error
`,
			expectedCheck: DataQualityCheck{
				Expression:  "expect_columns_ordered",
				Description: "Validate column order",
				OnFail:      OnFailActionError,
				SchemaCheck: &SchemaCheckConfig{
					ExpectColumnsOrdered: &ExpectColumnsOrderedConfig{
						ColumnsOrder: []string{"id", "name", "email", "created_at"},
					},
				},
				ParsedCheck: nil,
			},
		},
		{
			name: "schema_check with single column",
			yamlContent: `
schema_check:
  expect_columns_ordered:
    columns_order: [id]
`,
			expectedCheck: DataQualityCheck{
				Expression: "expect_columns_ordered",
				SchemaCheck: &SchemaCheckConfig{
					ExpectColumnsOrdered: &ExpectColumnsOrderedConfig{
						ColumnsOrder: []string{"id"},
					},
				},
				ParsedCheck: nil,
			},
		},
		{
			name: "schema_check with empty columns should parse",
			yamlContent: `
schema_check:
  expect_columns_ordered:
    columns_order: []
`,
			expectedCheck: DataQualityCheck{
				Expression: "expect_columns_ordered",
				SchemaCheck: &SchemaCheckConfig{
					ExpectColumnsOrdered: &ExpectColumnsOrderedConfig{
						ColumnsOrder: []string{},
					},
				},
				ParsedCheck: nil,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var check DataQualityCheck
			err := yaml.Unmarshal([]byte(tt.yamlContent), &check)

			if tt.expectError {
				if err == nil {
					t.Errorf("Expected error but got none")
				}
				return
			}

			if err != nil {
				t.Errorf("Unexpected error: %v", err)
				return
			}

			// Validate Expression
			if check.Expression != tt.expectedCheck.Expression {
				t.Errorf("Expression mismatch: got %s, want %s",
					check.Expression, tt.expectedCheck.Expression)
			}

			// Validate Description
			if check.Description != tt.expectedCheck.Description {
				t.Errorf("Description mismatch: got %s, want %s",
					check.Description, tt.expectedCheck.Description)
			}

			// Validate OnFail
			if check.OnFail != tt.expectedCheck.OnFail {
				t.Errorf("OnFail mismatch: got %s, want %s",
					check.OnFail, tt.expectedCheck.OnFail)
			}

			// Validate SchemaCheck
			if tt.expectedCheck.SchemaCheck != nil {
				if check.SchemaCheck == nil {
					t.Errorf("SchemaCheck is nil, expected non-nil")
				} else if check.SchemaCheck.ExpectColumnsOrdered != nil {
					if len(check.SchemaCheck.ExpectColumnsOrdered.ColumnsOrder) !=
						len(tt.expectedCheck.SchemaCheck.ExpectColumnsOrdered.ColumnsOrder) {
						t.Errorf("ColumnsOrder length mismatch: got %d, want %d",
							len(check.SchemaCheck.ExpectColumnsOrdered.ColumnsOrder),
							len(tt.expectedCheck.SchemaCheck.ExpectColumnsOrdered.ColumnsOrder))
					}
					for i, col := range check.SchemaCheck.ExpectColumnsOrdered.ColumnsOrder {
						if col != tt.expectedCheck.SchemaCheck.ExpectColumnsOrdered.ColumnsOrder[i] {
							t.Errorf("Column[%d] mismatch: got %s, want %s",
								i, col, tt.expectedCheck.SchemaCheck.ExpectColumnsOrdered.ColumnsOrder[i])
						}
					}
				}
			}

			// Validate ParsedCheck
			if tt.expectedCheck.ParsedCheck != nil {
				t.Errorf("ParsedCheck is non-nil, expected nil for schema checks")

				//if check.ParsedCheck == nil {
				//	t.Errorf("ParsedCheck is nil, expected non-nil")
				//} else {
				//	if check.ParsedCheck.FunctionName != tt.expectedCheck.ParsedCheck.FunctionName {
				//		t.Errorf("ParsedCheck.FunctionName mismatch: got %s, want %s",
				//			check.ParsedCheck.FunctionName, tt.expectedCheck.ParsedCheck.FunctionName)
				//	}
				//	if check.ParsedCheck.Scope != tt.expectedCheck.ParsedCheck.Scope {
				//		t.Errorf("ParsedCheck.Scope mismatch: got %s, want %s",
				//			check.ParsedCheck.Scope, tt.expectedCheck.ParsedCheck.Scope)
				//	}
				//	if check.ParsedCheck.Operator != tt.expectedCheck.ParsedCheck.Operator {
				//		t.Errorf("ParsedCheck.Operator mismatch: got %s, want %s",
				//			check.ParsedCheck.Operator, tt.expectedCheck.ParsedCheck.Operator)
				//	}
				//	if check.ParsedCheck.ThresholdValue != tt.expectedCheck.ParsedCheck.ThresholdValue {
				//		t.Errorf("ParsedCheck.ThresholdValue mismatch: got %v, want %v",
				//			check.ParsedCheck.ThresholdValue, tt.expectedCheck.ParsedCheck.ThresholdValue)
				//	}
				//}
			}
		})
	}
}

func TestSchemaCheckIntegrationWithConfig(t *testing.T) {
	yamlContent := `
version: 1
rules:
  - dataset: pg@[public.users]
    checks:
      - schema_check:
          expect_columns_ordered:
            columns_order: [id, name, email]
        desc: "Ensure columns are ordered correctly"
        on_fail: warn
      - row_count > 100
      - not_null(id)
`

	var config ChecksFileConfig
	err := yaml.Unmarshal([]byte(yamlContent), &config)
	if err != nil {
		t.Fatalf("Failed to unmarshal config: %v", err)
	}

	if len(config.Rules) != 1 {
		t.Errorf("Expected 1 rule, got %d", len(config.Rules))
	}

	rule := config.Rules[0]
	if len(rule.Checks) != 3 {
		t.Errorf("Expected 3 checks, got %d", len(rule.Checks))
	}

	// Check the schema_check
	schemaCheck := rule.Checks[0]
	if schemaCheck.SchemaCheck == nil {
		t.Errorf("First check should be a schema_check")
	} else if schemaCheck.SchemaCheck.ExpectColumnsOrdered == nil {
		t.Errorf("Schema check should have ExpectColumnsOrdered")
	} else {
		expectedColumns := []string{"id", "name", "email"}
		if len(schemaCheck.SchemaCheck.ExpectColumnsOrdered.ColumnsOrder) != len(expectedColumns) {
			t.Errorf("Expected %d columns, got %d",
				len(expectedColumns),
				len(schemaCheck.SchemaCheck.ExpectColumnsOrdered.ColumnsOrder))
		}
		for i, col := range schemaCheck.SchemaCheck.ExpectColumnsOrdered.ColumnsOrder {
			if col != expectedColumns[i] {
				t.Errorf("Column[%d] mismatch: got %s, want %s",
					i, col, expectedColumns[i])
			}
		}
	}

	if schemaCheck.Description != "Ensure columns are ordered correctly" {
		t.Errorf("Description mismatch: got %s", schemaCheck.Description)
	}

	if schemaCheck.OnFail != OnFailActionWarn {
		t.Errorf("OnFail mismatch: got %s, want warn", schemaCheck.OnFail)
	}

	// verify ParsedCheck is nil
	if schemaCheck.ParsedCheck != nil {
		t.Errorf("ParsedCheck should not be present")
	}

	//else {
	//	if schemaCheck.ParsedCheck.FunctionName != "expect_columns_ordered" {
	//		t.Errorf("ParsedCheck FunctionName mismatch: got %s",
	//			schemaCheck.ParsedCheck.FunctionName)
	//	}
	//	if schemaCheck.ParsedCheck.Scope != ScopeSchema {
	//		t.Errorf("ParsedCheck Scope mismatch: got %s",
	//			schemaCheck.ParsedCheck.Scope)
	//	}
	//}
}
