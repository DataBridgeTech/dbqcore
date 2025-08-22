package dbqcore

import (
	"context"
	"io"
	"log/slog"
	"testing"
)

// MockAdapterForExpectColumns is a test adapter for expect_columns checks
type MockAdapterForExpectColumns struct {
	expectedQuery string
	returnValue   string
	returnError   error
}

func (m *MockAdapterForExpectColumns) InterpretDataQualityCheck(check *DataQualityCheck, dataset string, where string) (string, error) {
	if check.SchemaCheck != nil && check.SchemaCheck.ExpectColumns != nil {
		// Simulate SQL generation for expect_columns check
		return "SELECT COUNT(*) FROM information_schema.columns WHERE column_name IN ('col1', 'col2')", nil
	}
	return "", nil
}

func (m *MockAdapterForExpectColumns) ExecuteQuery(ctx context.Context, query string) (string, error) {
	m.expectedQuery = query
	return m.returnValue, m.returnError
}

func TestDbqDataValidator_ExpectColumns(t *testing.T) {
	tests := []struct {
		name           string
		check          DataQualityCheck
		queryResult    string
		expectedPassed bool
		expectedReason string
	}{
		{
			name: "expect_columns all exist",
			check: DataQualityCheck{
				Expression: "expect_columns",
				SchemaCheck: &SchemaCheckConfig{
					ExpectColumns: &ExpectColumnsConfig{
						Columns: []string{"user_id", "email", "created_at"},
					},
				},
			},
			queryResult:    "3",
			expectedPassed: true,
			expectedReason: "",
		},
		{
			name: "expect_columns some missing",
			check: DataQualityCheck{
				Expression:  "expect_columns",
				Description: "Check required columns exist",
				SchemaCheck: &SchemaCheckConfig{
					ExpectColumns: &ExpectColumnsConfig{
						Columns: []string{"user_id", "email", "created_at"},
					},
				},
			},
			queryResult:    "2",
			expectedPassed: false,
			expectedReason: "Check failed: expect_columns == 3 (got: 2)",
		},
		{
			name: "expect_columns none exist",
			check: DataQualityCheck{
				Expression: "expect_columns",
				OnFail:     OnFailActionError,
				SchemaCheck: &SchemaCheckConfig{
					ExpectColumns: &ExpectColumnsConfig{
						Columns: []string{"col1", "col2"},
					},
				},
			},
			queryResult:    "0",
			expectedPassed: false,
			expectedReason: "Check failed: expect_columns == 2 (got: 0)",
		},
		{
			name: "expect_columns single column",
			check: DataQualityCheck{
				Expression: "expect_columns",
				SchemaCheck: &SchemaCheckConfig{
					ExpectColumns: &ExpectColumnsConfig{
						Columns: []string{"id"},
					},
				},
			},
			queryResult:    "1",
			expectedPassed: true,
			expectedReason: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			adapter := &MockAdapterForExpectColumns{
				returnValue: tt.queryResult,
			}

			logger := slog.New(slog.NewTextHandler(io.Discard, nil))
			validator := NewDbqDataValidator(logger)
			ctx := context.Background()

			result := validator.RunCheck(ctx, adapter, &tt.check, "test.table", "")

			if result.Pass != tt.expectedPassed {
				t.Errorf("Expected passed=%v, got %v", tt.expectedPassed, result.Pass)
			}

			if !tt.expectedPassed && result.Error != tt.expectedReason {
				t.Errorf("Expected reason '%s', got '%s'", tt.expectedReason, result.Error)
			}

			if result.CheckID != tt.check.Expression {
				t.Errorf("Expected expression '%s', got '%s'", tt.check.Expression, result.CheckID)
			}
		})
	}
}
