package dbqcore

import (
	"testing"

	"gopkg.in/yaml.v3"
)

func TestColumnsNotPresentConfig(t *testing.T) {
	tests := []struct {
		name           string
		yamlContent    string
		expectedConfig ColumnsNotPresentConfig
		expectError    bool
	}{
		{
			name: "columns_not_present with column list only",
			yamlContent: `
version: 1
rules:
  - dataset: test_table
    checks:
      - schema_check:
          columns_not_present:
            columns: [credit_card_number, ssn, password]
        desc: "Ensure no sensitive columns exist"
        on_fail: error
`,
			expectedConfig: ColumnsNotPresentConfig{
				Columns: []string{"credit_card_number", "ssn", "password"},
				Pattern: "",
			},
			expectError: false,
		},
		{
			name: "columns_not_present with pattern only",
			yamlContent: `
version: 1
rules:
  - dataset: test_table
    checks:
      - schema_check:
          columns_not_present:
            pattern: "pii_*"
        desc: "Ensure no PII columns exist"
        on_fail: warn
`,
			expectedConfig: ColumnsNotPresentConfig{
				Columns: nil,
				Pattern: "pii_*",
			},
			expectError: false,
		},
		{
			name: "columns_not_present with both columns and pattern",
			yamlContent: `
version: 1
rules:
  - dataset: test_table
    checks:
      - schema_check:
          columns_not_present:
            columns: [credit_card, cvv]
            pattern: "sensitive_*"
        desc: "Ensure no sensitive data columns"
        on_fail: error
`,
			expectedConfig: ColumnsNotPresentConfig{
				Columns: []string{"credit_card", "cvv"},
				Pattern: "sensitive_*",
			},
			expectError: false,
		},
		{
			name: "columns_not_present with single column",
			yamlContent: `
version: 1
rules:
  - dataset: test_table
    checks:
      - schema_check:
          columns_not_present:
            columns: [password]
        desc: "Ensure password column doesn't exist"
`,
			expectedConfig: ColumnsNotPresentConfig{
				Columns: []string{"password"},
				Pattern: "",
			},
			expectError: false,
		},
		{
			name: "columns_not_present with wildcard pattern",
			yamlContent: `
version: 1
rules:
  - dataset: test_table
    checks:
      - schema_check:
          columns_not_present:
            pattern: "*_temp"
        desc: "Ensure no temporary columns"
`,
			expectedConfig: ColumnsNotPresentConfig{
				Columns: nil,
				Pattern: "*_temp",
			},
			expectError: false,
		},
		{
			name: "columns_not_present with complex pattern",
			yamlContent: `
version: 1
rules:
  - dataset: test_table
    checks:
      - schema_check:
          columns_not_present:
            pattern: "test_*_backup"
        desc: "Ensure no backup columns"
`,
			expectedConfig: ColumnsNotPresentConfig{
				Columns: nil,
				Pattern: "test_*_backup",
			},
			expectError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var config ChecksFileConfig
			err := yaml.Unmarshal([]byte(tt.yamlContent), &config)

			if tt.expectError {
				if err == nil {
					t.Errorf("Expected error but got none")
				}
				return
			}

			if err != nil {
				t.Fatalf("Failed to unmarshal config: %v", err)
			}

			if len(config.Rules) != 1 {
				t.Fatalf("Expected 1 rule, got %d", len(config.Rules))
			}

			if len(config.Rules[0].Checks) != 1 {
				t.Fatalf("Expected 1 check, got %d", len(config.Rules[0].Checks))
			}

			check := config.Rules[0].Checks[0]
			if check.SchemaCheck == nil {
				t.Fatal("Expected SchemaCheck to be set")
			}

			if check.SchemaCheck.ColumnsNotPresent == nil {
				t.Fatal("Expected ColumnsNotPresent to be set")
			}

			// Verify Expression is set correctly
			if check.Expression != "columns_not_present" {
				t.Errorf("Expected Expression to be 'columns_not_present', got %s", check.Expression)
			}

			// Verify ParsedCheck is nil for schema checks
			if check.ParsedCheck != nil {
				t.Error("Expected ParsedCheck to be nil for schema checks")
			}

			// Compare columns
			actualConfig := check.SchemaCheck.ColumnsNotPresent
			if len(actualConfig.Columns) != len(tt.expectedConfig.Columns) {
				t.Errorf("Expected %d columns, got %d", len(tt.expectedConfig.Columns), len(actualConfig.Columns))
			} else {
				for i, col := range actualConfig.Columns {
					if col != tt.expectedConfig.Columns[i] {
						t.Errorf("Column[%d] mismatch: expected %s, got %s", i, tt.expectedConfig.Columns[i], col)
					}
				}
			}

			// Compare pattern
			if actualConfig.Pattern != tt.expectedConfig.Pattern {
				t.Errorf("Pattern mismatch: expected %s, got %s", tt.expectedConfig.Pattern, actualConfig.Pattern)
			}
		})
	}
}

func TestColumnsNotPresentWithOtherChecks(t *testing.T) {
	yamlContent := `
version: 1
rules:
  - dataset: test_table
    checks:
      - schema_check:
          expect_columns:
            columns: [id, name, email]
        desc: "Ensure required columns exist"
      - schema_check:
          columns_not_present:
            columns: [password, ssn]
        desc: "Ensure sensitive columns don't exist"
      - row_count > 100:
          desc: "Ensure table has data"
`

	var config ChecksFileConfig
	err := yaml.Unmarshal([]byte(yamlContent), &config)
	if err != nil {
		t.Fatalf("Failed to unmarshal config: %v", err)
	}

	if len(config.Rules) != 1 {
		t.Fatalf("Expected 1 rule, got %d", len(config.Rules))
	}

	if len(config.Rules[0].Checks) != 3 {
		t.Fatalf("Expected 3 checks, got %d", len(config.Rules[0].Checks))
	}

	// Check the first check (expect_columns)
	check1 := config.Rules[0].Checks[0]
	if check1.Expression != "expect_columns" {
		t.Errorf("First check: expected Expression 'expect_columns', got %s", check1.Expression)
	}
	if check1.SchemaCheck == nil || check1.SchemaCheck.ExpectColumns == nil {
		t.Error("First check should have ExpectColumns")
	}

	// Check the second check (columns_not_present)
	check2 := config.Rules[0].Checks[1]
	if check2.Expression != "columns_not_present" {
		t.Errorf("Second check: expected Expression 'columns_not_present', got %s", check2.Expression)
	}
	if check2.SchemaCheck == nil || check2.SchemaCheck.ColumnsNotPresent == nil {
		t.Error("Second check should have ColumnsNotPresent")
	}
	if len(check2.SchemaCheck.ColumnsNotPresent.Columns) != 2 {
		t.Errorf("Expected 2 columns to not be present, got %d", len(check2.SchemaCheck.ColumnsNotPresent.Columns))
	}

	// Check the third check (row_count)
	check3 := config.Rules[0].Checks[2]
	if check3.Expression != "row_count > 100" {
		t.Errorf("Third check: expected Expression 'row_count > 100', got %s", check3.Expression)
	}
	if check3.ParsedCheck == nil {
		t.Error("Third check should have ParsedCheck")
	}
}

func TestColumnsNotPresentValidationEdgeCases(t *testing.T) {
	tests := []struct {
		name        string
		yamlContent string
		expectValid bool
		description string
	}{
		{
			name: "empty columns list with pattern",
			yamlContent: `
version: 1
rules:
  - dataset: test_table
    checks:
      - schema_check:
          columns_not_present:
            columns: []
            pattern: "temp_*"
`,
			expectValid: true,
			description: "Empty columns list with pattern should be valid",
		},
		{
			name: "empty pattern with columns",
			yamlContent: `
version: 1
rules:
  - dataset: test_table
    checks:
      - schema_check:
          columns_not_present:
            columns: [temp_col]
            pattern: ""
`,
			expectValid: true,
			description: "Empty pattern with columns should be valid",
		},
		{
			name: "neither columns nor pattern",
			yamlContent: `
version: 1
rules:
  - dataset: test_table
    checks:
      - schema_check:
          columns_not_present:
            columns: []
            pattern: ""
`,
			expectValid: true, // Will be validated at runtime in adapter
			description: "Neither columns nor pattern will be caught during SQL generation",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var config ChecksFileConfig
			err := yaml.Unmarshal([]byte(tt.yamlContent), &config)

			if !tt.expectValid {
				if err == nil {
					t.Errorf("%s: Expected error but got none", tt.description)
				}
			} else {
				if err != nil {
					t.Errorf("%s: Unexpected error: %v", tt.description, err)
				}
			}
		})
	}
}
