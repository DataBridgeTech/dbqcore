package dbqcore

import (
	"testing"

	"gopkg.in/yaml.v3"
)

func TestColumnsNotPresentIntegration(t *testing.T) {
	// Test complete YAML configuration with all schema checks
	yamlContent := `
version: 1
rules:
  - dataset: pg@[public.users]
    checks:
      # Ensure required columns exist
      - schema_check:
          expect_columns:
            columns: [user_id, email, created_at]
        desc: "Ensure all required columns exist"
        on_fail: error
      
      # Ensure columns are in expected order
      - schema_check:
          expect_columns_ordered:
            columns_order: [user_id, email, created_at, updated_at]
        desc: "Ensure columns are in correct order"
        on_fail: warn
      
      # Ensure no PII columns exist
      - schema_check:
          columns_not_present:
            columns: [ssn, credit_card_number, cvv]
        desc: "Ensure no credit card data exists"
        on_fail: error
      
      # Ensure no temporary or backup columns
      - schema_check:
          columns_not_present:
            pattern: "temp_*"
        desc: "Ensure no temporary columns exist"
        on_fail: warn
      
      # Ensure no sensitive data columns
      - schema_check:
          columns_not_present:
            columns: [password_plain, api_secret]
            pattern: "pii_*"
        desc: "Ensure no sensitive data columns exist"
        on_fail: error
`

	var config ChecksFileConfig
	err := yaml.Unmarshal([]byte(yamlContent), &config)
	if err != nil {
		t.Fatalf("Failed to unmarshal config: %v", err)
	}

	// Verify we have the expected structure
	if len(config.Rules) != 1 {
		t.Fatalf("Expected 1 rule, got %d", len(config.Rules))
	}

	rule := config.Rules[0]
	if len(rule.Checks) != 5 {
		t.Fatalf("Expected 5 checks, got %d", len(rule.Checks))
	}

	// Verify expect_columns check
	check1 := rule.Checks[0]
	if check1.Expression != "expect_columns" {
		t.Errorf("Check 1: Expected Expression 'expect_columns', got %s", check1.Expression)
	}
	if check1.SchemaCheck == nil || check1.SchemaCheck.ExpectColumns == nil {
		t.Error("Check 1: Should have ExpectColumns")
	}
	if len(check1.SchemaCheck.ExpectColumns.Columns) != 3 {
		t.Errorf("Check 1: Expected 3 columns, got %d", len(check1.SchemaCheck.ExpectColumns.Columns))
	}

	// Verify expect_columns_ordered check
	check2 := rule.Checks[1]
	if check2.Expression != "expect_columns_ordered" {
		t.Errorf("Check 2: Expected Expression 'expect_columns_ordered', got %s", check2.Expression)
	}
	if check2.SchemaCheck == nil || check2.SchemaCheck.ExpectColumnsOrdered == nil {
		t.Error("Check 2: Should have ExpectColumnsOrdered")
	}
	if len(check2.SchemaCheck.ExpectColumnsOrdered.ColumnsOrder) != 4 {
		t.Errorf("Check 2: Expected 4 columns in order, got %d", len(check2.SchemaCheck.ExpectColumnsOrdered.ColumnsOrder))
	}

	// Verify first columns_not_present check (columns only)
	check3 := rule.Checks[2]
	if check3.Expression != "columns_not_present" {
		t.Errorf("Check 3: Expected Expression 'columns_not_present', got %s", check3.Expression)
	}
	if check3.SchemaCheck == nil || check3.SchemaCheck.ColumnsNotPresent == nil {
		t.Error("Check 3: Should have ColumnsNotPresent")
	}
	if len(check3.SchemaCheck.ColumnsNotPresent.Columns) != 3 {
		t.Errorf("Check 3: Expected 3 columns to not be present, got %d", len(check3.SchemaCheck.ColumnsNotPresent.Columns))
	}
	if check3.SchemaCheck.ColumnsNotPresent.Pattern != "" {
		t.Errorf("Check 3: Expected no pattern, got %s", check3.SchemaCheck.ColumnsNotPresent.Pattern)
	}
	if check3.OnFail != OnFailActionError {
		t.Errorf("Check 3: Expected on_fail 'error', got %s", check3.OnFail)
	}

	// Verify second columns_not_present check (pattern only)
	check4 := rule.Checks[3]
	if check4.Expression != "columns_not_present" {
		t.Errorf("Check 4: Expected Expression 'columns_not_present', got %s", check4.Expression)
	}
	if check4.SchemaCheck == nil || check4.SchemaCheck.ColumnsNotPresent == nil {
		t.Error("Check 4: Should have ColumnsNotPresent")
	}
	if len(check4.SchemaCheck.ColumnsNotPresent.Columns) != 0 {
		t.Errorf("Check 4: Expected no columns list, got %d", len(check4.SchemaCheck.ColumnsNotPresent.Columns))
	}
	if check4.SchemaCheck.ColumnsNotPresent.Pattern != "temp_*" {
		t.Errorf("Check 4: Expected pattern 'temp_*', got %s", check4.SchemaCheck.ColumnsNotPresent.Pattern)
	}
	if check4.OnFail != OnFailActionWarn {
		t.Errorf("Check 4: Expected on_fail 'warn', got %s", check4.OnFail)
	}

	// Verify third columns_not_present check (both columns and pattern)
	check5 := rule.Checks[4]
	if check5.Expression != "columns_not_present" {
		t.Errorf("Check 5: Expected Expression 'columns_not_present', got %s", check5.Expression)
	}
	if check5.SchemaCheck == nil || check5.SchemaCheck.ColumnsNotPresent == nil {
		t.Error("Check 5: Should have ColumnsNotPresent")
	}
	if len(check5.SchemaCheck.ColumnsNotPresent.Columns) != 2 {
		t.Errorf("Check 5: Expected 2 columns, got %d", len(check5.SchemaCheck.ColumnsNotPresent.Columns))
	}
	if check5.SchemaCheck.ColumnsNotPresent.Pattern != "pii_*" {
		t.Errorf("Check 5: Expected pattern 'pii_*', got %s", check5.SchemaCheck.ColumnsNotPresent.Pattern)
	}
	if check5.Description != "Ensure no sensitive data columns exist" {
		t.Errorf("Check 5: Expected description 'Ensure no sensitive data columns exist', got %s", check5.Description)
	}

	// Verify all checks have ParsedCheck as nil (schema checks don't use ParsedCheck)
	for i, check := range rule.Checks {
		if check.ParsedCheck != nil {
			t.Errorf("Check %d: Expected ParsedCheck to be nil for schema checks, but it wasn't", i+1)
		}
	}
}
