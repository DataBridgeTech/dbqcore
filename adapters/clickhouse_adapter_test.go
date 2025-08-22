package adapters

import (
	"io"
	"log/slog"
	"testing"

	"github.com/DataBridgeTech/dbqcore"
)

func createMockClickHouseAdapter() *ClickhouseDbqDataSourceAdapter {
	return &ClickhouseDbqDataSourceAdapter{
		cnn:    nil, // We don't need actual connection for InterpretDataQualityCheck tests
		logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
}

func TestClickhouseAdapter_InterpretDataQualityCheck(t *testing.T) {
	adapter := createMockClickHouseAdapter()

	tests := []struct {
		name         string
		check        *dbqcore.DataQualityCheck
		dataset      string
		whereClause  string
		expectedSQL  string
		expectError  bool
		errorMessage string
	}{
		{
			name: "row_count check",
			check: &dbqcore.DataQualityCheck{
				Expression:  "row_count > 1000",
				ParsedCheck: createMockParsedCheck("row_count", []string{}, ">", 1000),
			},
			dataset:     "my_table",
			whereClause: "",
			expectedSQL: "select count() from my_table",
		},
		{
			name: "row_count check with where clause",
			check: &dbqcore.DataQualityCheck{
				Expression:  "row_count between 100 and 1000",
				ParsedCheck: createMockParsedCheck("row_count", []string{}, "between", dbqcore.BetweenRange{Min: 100, Max: 1000}),
			},
			dataset:     "orders",
			whereClause: "created_at > '2024-01-01'",
			expectedSQL: "select count() from orders where created_at > '2024-01-01'",
		},
		{
			name: "not_null check",
			check: &dbqcore.DataQualityCheck{
				Expression:  "not_null(customer_id)",
				ParsedCheck: createMockParsedCheck("not_null", []string{"customer_id"}, "", nil),
			},
			dataset:     "customers",
			whereClause: "",
			expectedSQL: "select countIf(isNotNull(customer_id)) from customers",
		},
		{
			name: "not_null check missing parameter",
			check: &dbqcore.DataQualityCheck{
				Expression:  "not_null()",
				ParsedCheck: createMockParsedCheck("not_null", []string{}, "", nil),
			},
			dataset:      "customers",
			whereClause:  "",
			expectError:  true,
			errorMessage: "not_null check requires a column parameter",
		},
		{
			name: "uniqueness check",
			check: &dbqcore.DataQualityCheck{
				Expression:  "uniqueness(email)",
				ParsedCheck: createMockParsedCheck("uniqueness", []string{"email"}, "", nil),
			},
			dataset:     "users",
			whereClause: "",
			expectedSQL: "select uniqExact(email) from users",
		},
		{
			name: "freshness check",
			check: &dbqcore.DataQualityCheck{
				Expression:  "freshness(last_updated) < 3600",
				ParsedCheck: createMockParsedCheck("freshness", []string{"last_updated"}, "<", 3600),
			},
			dataset:     "products",
			whereClause: "",
			expectedSQL: "select dateDiff('second', max(last_updated), now()) from products",
		},
		{
			name: "min check",
			check: &dbqcore.DataQualityCheck{
				Expression:  "min(price) > 0",
				ParsedCheck: createMockParsedCheck("min", []string{"price"}, ">", 0),
			},
			dataset:     "items",
			whereClause: "",
			expectedSQL: "select min(price) from items",
		},
		{
			name: "max check with where clause",
			check: &dbqcore.DataQualityCheck{
				Expression:  "max(quantity) < 1000",
				ParsedCheck: createMockParsedCheck("max", []string{"quantity"}, "<", 1000),
			},
			dataset:     "inventory",
			whereClause: "active = 1",
			expectedSQL: "select max(quantity) from inventory where active = 1",
		},
		{
			name: "avg check",
			check: &dbqcore.DataQualityCheck{
				Expression:  "avg(rating) between 3.0 and 5.0",
				ParsedCheck: createMockParsedCheck("avg", []string{"rating"}, "between", dbqcore.BetweenRange{Min: 3.0, Max: 5.0}),
			},
			dataset:     "reviews",
			whereClause: "",
			expectedSQL: "select avg(rating) from reviews",
		},
		{
			name: "sum check",
			check: &dbqcore.DataQualityCheck{
				Expression:  "sum(total_amount) > 10000",
				ParsedCheck: createMockParsedCheck("sum", []string{"total_amount"}, ">", 10000),
			},
			dataset:     "transactions",
			whereClause: "",
			expectedSQL: "select sum(total_amount) from transactions",
		},
		{
			name: "stddev check",
			check: &dbqcore.DataQualityCheck{
				Expression:  "stddev(response_time) < 100",
				ParsedCheck: createMockParsedCheck("stddev", []string{"response_time"}, "<", 100),
			},
			dataset:     "api_calls",
			whereClause: "",
			expectedSQL: "select stddevPop(response_time) from api_calls",
		},
		{
			name: "raw_query check",
			check: &dbqcore.DataQualityCheck{
				Expression:  "raw_query",
				Query:       "select count(*) from {{dataset}} where status = 'active'",
				ParsedCheck: createMockParsedCheck("raw_query", []string{}, "", nil),
			},
			dataset:     "users",
			whereClause: "",
			expectedSQL: "select count(*) from users where status = 'active'",
		},
		{
			name: "raw_query check with where clause",
			check: &dbqcore.DataQualityCheck{
				Expression:  "raw_query",
				Query:       "select count(*) from {{dataset}}",
				ParsedCheck: createMockParsedCheck("raw_query", []string{}, "", nil),
			},
			dataset:     "products",
			whereClause: "category = 'electronics'",
			expectedSQL: "select count(*) from products where category = 'electronics'",
		},
		{
			name: "raw_query check with existing where clause",
			check: &dbqcore.DataQualityCheck{
				Expression:  "raw_query",
				Query:       "select avg(price) from {{dataset}} where category = 'books'",
				ParsedCheck: createMockParsedCheck("raw_query", []string{}, "", nil),
			},
			dataset:     "products",
			whereClause: "active = 1",
			expectedSQL: "select avg(price) from products where category = 'books' and (active = 1)",
		},
		{
			name: "raw_query check with multiline query",
			check: &dbqcore.DataQualityCheck{
				Expression:  "raw_query",
				Query:       "select count(*)\nfrom {{dataset}}\nwhere active = 1",
				ParsedCheck: createMockParsedCheck("raw_query", []string{}, "", nil),
			},
			dataset:     "orders",
			whereClause: "",
			expectedSQL: "select count(*) from orders where active = 1",
		},
		{
			name: "raw_query check missing query field",
			check: &dbqcore.DataQualityCheck{
				Expression:  "raw_query",
				Query:       "", // Missing query
				ParsedCheck: createMockParsedCheck("raw_query", []string{}, "", nil),
			},
			dataset:      "users",
			whereClause:  "",
			expectError:  true,
			errorMessage: "raw_query check requires a 'query' field",
		},
		{
			name: "expect_columns_ordered check",
			check: &dbqcore.DataQualityCheck{
				Expression: "expect_columns_ordered",
				SchemaCheck: &dbqcore.SchemaCheckConfig{
					ExpectColumnsOrdered: &dbqcore.ExpectColumnsOrderedConfig{
						ColumnsOrder: []string{"id", "name", "email"},
					},
				},
			},
			dataset:     "default.users",
			whereClause: "",
			expectedSQL: `select count()
				from system.columns
				where database = 'default'
				and table = 'users'
				and ((name = 'id' and position = 1) or (name = 'name' and position = 2) or (name = 'email' and position = 3))`,
		},
		{
			name: "expect_columns_ordered check with single column",
			check: &dbqcore.DataQualityCheck{
				Expression: "expect_columns_ordered",
				SchemaCheck: &dbqcore.SchemaCheckConfig{
					ExpectColumnsOrdered: &dbqcore.ExpectColumnsOrderedConfig{
						ColumnsOrder: []string{"id"},
					},
				},
			},
			dataset:     "analytics.events",
			whereClause: "",
			expectedSQL: `select count()
				from system.columns
				where database = 'analytics'
				and table = 'events'
				and ((name = 'id' and position = 1))`,
		},
		{
			name: "expect_columns_ordered check invalid dataset format",
			check: &dbqcore.DataQualityCheck{
				Expression: "expect_columns_ordered",
				SchemaCheck: &dbqcore.SchemaCheckConfig{
					ExpectColumnsOrdered: &dbqcore.ExpectColumnsOrderedConfig{
						ColumnsOrder: []string{"id", "name"},
					},
				},
			},
			dataset:      "invalid_dataset",
			whereClause:  "",
			expectError:  true,
			errorMessage: "dataset must be in format database.table",
		},
		{
			name: "expect_columns check with multiple columns",
			check: &dbqcore.DataQualityCheck{
				Expression: "expect_columns",
				SchemaCheck: &dbqcore.SchemaCheckConfig{
					ExpectColumns: &dbqcore.ExpectColumnsConfig{
						Columns: []string{"event_id", "user_id", "timestamp"},
					},
				},
			},
			dataset:     "analytics.events",
			whereClause: "",
			expectedSQL: `select count()
				from system.columns
				where database = 'analytics'
				and table = 'events'
				and (name = 'event_id' or name = 'user_id' or name = 'timestamp')`,
		},
		{
			name: "expect_columns check with two columns",
			check: &dbqcore.DataQualityCheck{
				Expression: "expect_columns",
				SchemaCheck: &dbqcore.SchemaCheckConfig{
					ExpectColumns: &dbqcore.ExpectColumnsConfig{
						Columns: []string{"metric_name", "value"},
					},
				},
			},
			dataset:     "metrics.timeseries",
			whereClause: "",
			expectedSQL: `select count()
				from system.columns
				where database = 'metrics'
				and table = 'timeseries'
				and (name = 'metric_name' or name = 'value')`,
		},
		{
			name: "expect_columns check invalid dataset format",
			check: &dbqcore.DataQualityCheck{
				Expression: "expect_columns",
				SchemaCheck: &dbqcore.SchemaCheckConfig{
					ExpectColumns: &dbqcore.ExpectColumnsConfig{
						Columns: []string{"id", "name"},
					},
				},
			},
			dataset:      "invalid_dataset",
			whereClause:  "",
			expectError:  true,
			errorMessage: "dataset must be in format database.table",
		},
		{
			name: "unknown function fallback",
			check: &dbqcore.DataQualityCheck{
				Expression:  "custom_function(col1, col2) > 100",
				ParsedCheck: createMockParsedCheck("custom_function", []string{"col1", "col2"}, ">", 100),
			},
			dataset:     "custom_table",
			whereClause: "",
			expectedSQL: "select custom_function(col1, col2) > 100 from custom_table",
		},
		{
			name: "check without parsed structure",
			check: &dbqcore.DataQualityCheck{
				Expression:  "row_count > 100",
				ParsedCheck: nil, // Missing parsed structure
			},
			dataset:      "test_table",
			whereClause:  "",
			expectError:  true,
			errorMessage: "check does not have parsed structure",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := adapter.InterpretDataQualityCheck(tt.check, tt.dataset, tt.whereClause)

			if tt.expectError {
				if err == nil {
					t.Errorf("Expected error but got none")
					return
				}
				if tt.errorMessage != "" && err.Error() != tt.errorMessage {
					t.Errorf("Expected error message '%s', got '%s'", tt.errorMessage, err.Error())
				}
				return
			}

			if err != nil {
				t.Errorf("Unexpected error: %v", err)
				return
			}

			if result != tt.expectedSQL {
				t.Errorf("Expected SQL:\n%s\nGot:\n%s", tt.expectedSQL, result)
			}
		})
	}
}

func TestClickhouseAdapter_InterpretDataQualityCheck_EdgeCases(t *testing.T) {
	adapter := createMockClickHouseAdapter()

	t.Run("all aggregation functions without parameters should fail", func(t *testing.T) {
		functions := []string{"min", "max", "avg", "sum", "stddev", "not_null", "uniqueness", "freshness"}

		for _, fn := range functions {
			check := &dbqcore.DataQualityCheck{
				Expression:  fn + "()",
				ParsedCheck: createMockParsedCheck(fn, []string{}, "", nil),
			}

			_, err := adapter.InterpretDataQualityCheck(check, "test_table", "")
			if err == nil {
				t.Errorf("Function %s should require parameters but didn't fail", fn)
			}
		}
	})

	t.Run("case sensitivity in raw queries", func(t *testing.T) {
		check := &dbqcore.DataQualityCheck{
			Expression:  "raw_query",
			Query:       "SELECT COUNT(*) FROM {{dataset}} WHERE status = 'ACTIVE'",
			ParsedCheck: createMockParsedCheck("raw_query", []string{}, "", nil),
		}

		result, err := adapter.InterpretDataQualityCheck(check, "Users", "Category = 'ELECTRONICS'")
		if err != nil {
			t.Errorf("Unexpected error: %v", err)
		}

		expected := "SELECT COUNT(*) FROM Users WHERE status = 'ACTIVE' and (Category = 'ELECTRONICS')"
		if result != expected {
			t.Errorf("Expected: %s, Got: %s", expected, result)
		}
	})
}
