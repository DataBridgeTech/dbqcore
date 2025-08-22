package adapters

import (
	"io"
	"log/slog"
	"testing"

	"github.com/DataBridgeTech/dbqcore"
)

func createMockPostgreSQLAdapter() *PostgresqlDbqDataSourceAdapter {
	return &PostgresqlDbqDataSourceAdapter{
		db:     nil, // We don't need actual connection for InterpretDataQualityCheck tests
		logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
}

func TestPostgreSQLAdapter_InterpretDataQualityCheck(t *testing.T) {
	adapter := createMockPostgreSQLAdapter()

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
			expectedSQL: "SELECT COUNT(*) FROM my_table",
		},
		{
			name: "row_count check with where clause",
			check: &dbqcore.DataQualityCheck{
				Expression:  "row_count between 100 and 1000",
				ParsedCheck: createMockParsedCheck("row_count", []string{}, "between", dbqcore.BetweenRange{Min: 100, Max: 1000}),
			},
			dataset:     "orders",
			whereClause: "created_at > '2024-01-01'",
			expectedSQL: "SELECT COUNT(*) FROM orders WHERE created_at > '2024-01-01'",
		},
		{
			name: "not_null check with PostgreSQL-specific CASE syntax",
			check: &dbqcore.DataQualityCheck{
				Expression:  "not_null(customer_id)",
				ParsedCheck: createMockParsedCheck("not_null", []string{"customer_id"}, "", nil),
			},
			dataset:     "customers",
			whereClause: "",
			expectedSQL: "SELECT COUNT(CASE WHEN \"customer_id\" IS NOT NULL THEN 1 END) FROM customers",
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
			name: "uniqueness check with PostgreSQL double quotes",
			check: &dbqcore.DataQualityCheck{
				Expression:  "uniqueness(email)",
				ParsedCheck: createMockParsedCheck("uniqueness", []string{"email"}, "", nil),
			},
			dataset:     "users",
			whereClause: "",
			expectedSQL: "SELECT COUNT(DISTINCT \"email\") FROM users",
		},
		{
			name: "freshness check with EXTRACT and EPOCH",
			check: &dbqcore.DataQualityCheck{
				Expression:  "freshness(last_updated) < 3600",
				ParsedCheck: createMockParsedCheck("freshness", []string{"last_updated"}, "<", 3600),
			},
			dataset:     "products",
			whereClause: "",
			expectedSQL: "SELECT EXTRACT(EPOCH FROM (NOW() - MAX(\"last_updated\"))) FROM products",
		},
		{
			name: "min check",
			check: &dbqcore.DataQualityCheck{
				Expression:  "min(price) > 0",
				ParsedCheck: createMockParsedCheck("min", []string{"price"}, ">", 0),
			},
			dataset:     "items",
			whereClause: "",
			expectedSQL: "SELECT MIN(\"price\") FROM items",
		},
		{
			name: "max check with where clause",
			check: &dbqcore.DataQualityCheck{
				Expression:  "max(quantity) < 1000",
				ParsedCheck: createMockParsedCheck("max", []string{"quantity"}, "<", 1000),
			},
			dataset:     "inventory",
			whereClause: "active = true",
			expectedSQL: "SELECT MAX(\"quantity\") FROM inventory WHERE active = true",
		},
		{
			name: "avg check",
			check: &dbqcore.DataQualityCheck{
				Expression:  "avg(rating) between 3.0 and 5.0",
				ParsedCheck: createMockParsedCheck("avg", []string{"rating"}, "between", dbqcore.BetweenRange{Min: 3.0, Max: 5.0}),
			},
			dataset:     "reviews",
			whereClause: "",
			expectedSQL: "SELECT AVG(\"rating\") FROM reviews",
		},
		{
			name: "sum check",
			check: &dbqcore.DataQualityCheck{
				Expression:  "sum(total_amount) > 10000",
				ParsedCheck: createMockParsedCheck("sum", []string{"total_amount"}, ">", 10000),
			},
			dataset:     "transactions",
			whereClause: "",
			expectedSQL: "SELECT SUM(\"total_amount\") FROM transactions",
		},
		{
			name: "stddev check with STDDEV_POP",
			check: &dbqcore.DataQualityCheck{
				Expression:  "stddev(response_time) < 100",
				ParsedCheck: createMockParsedCheck("stddev", []string{"response_time"}, "<", 100),
			},
			dataset:     "api_calls",
			whereClause: "",
			expectedSQL: "SELECT STDDEV_POP(\"response_time\") FROM api_calls",
		},
		{
			name: "raw_query check",
			check: &dbqcore.DataQualityCheck{
				Expression:  "raw_query",
				Query:       "SELECT COUNT(*) FROM {{dataset}} WHERE status = 'active'",
				ParsedCheck: createMockParsedCheck("raw_query", []string{}, "", nil),
			},
			dataset:     "users",
			whereClause: "",
			expectedSQL: "SELECT COUNT(*) FROM users WHERE status = 'active'",
		},
		{
			name: "raw_query check with where clause",
			check: &dbqcore.DataQualityCheck{
				Expression:  "raw_query",
				Query:       "SELECT COUNT(*) FROM {{dataset}}",
				ParsedCheck: createMockParsedCheck("raw_query", []string{}, "", nil),
			},
			dataset:     "products",
			whereClause: "category = 'electronics'",
			expectedSQL: "SELECT COUNT(*) FROM products WHERE category = 'electronics'",
		},
		{
			name: "raw_query check with existing where clause",
			check: &dbqcore.DataQualityCheck{
				Expression:  "raw_query",
				Query:       "SELECT AVG(price) FROM {{dataset}} WHERE category = 'books'",
				ParsedCheck: createMockParsedCheck("raw_query", []string{}, "", nil),
			},
			dataset:     "products",
			whereClause: "active = true",
			expectedSQL: "SELECT AVG(price) FROM products WHERE category = 'books' AND (active = true)",
		},
		{
			name: "raw_query check with multiline query",
			check: &dbqcore.DataQualityCheck{
				Expression:  "raw_query",
				Query:       "SELECT COUNT(*)\nFROM {{dataset}}\nWHERE active = true",
				ParsedCheck: createMockParsedCheck("raw_query", []string{}, "", nil),
			},
			dataset:     "orders",
			whereClause: "",
			expectedSQL: "SELECT COUNT(*) FROM orders WHERE active = true",
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
			dataset:     "public.users",
			whereClause: "",
			expectedSQL: `select count(*)
			from information_schema.columns
			where table_schema = 'public'
			and table_name = 'users'
			and ((column_name = 'id' and ordinal_position = 1) or (column_name = 'name' and ordinal_position = 2) or (column_name = 'email' and ordinal_position = 3))`,
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
			dataset:     "mydb.products",
			whereClause: "",
			expectedSQL: `select count(*)
			from information_schema.columns
			where table_schema = 'mydb'
			and table_name = 'products'
			and ((column_name = 'id' and ordinal_position = 1))`,
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
			name: "unknown function fallback",
			check: &dbqcore.DataQualityCheck{
				Expression:  "custom_function(col1, col2) > 100",
				ParsedCheck: createMockParsedCheck("custom_function", []string{"col1", "col2"}, ">", 100),
			},
			dataset:     "custom_table",
			whereClause: "",
			expectedSQL: "SELECT custom_function(col1, col2) > 100 FROM custom_table",
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

func TestPostgreSQLAdapter_DatabaseSpecificFeatures(t *testing.T) {
	adapter := createMockPostgreSQLAdapter()

	t.Run("PostgreSQL double quote identifier quoting", func(t *testing.T) {
		check := &dbqcore.DataQualityCheck{
			Expression:  "min(order)",
			ParsedCheck: createMockParsedCheck("min", []string{"order"}, ">", 0),
		}

		result, err := adapter.InterpretDataQualityCheck(check, "sales", "")
		if err != nil {
			t.Errorf("Unexpected error: %v", err)
		}

		expected := "SELECT MIN(\"order\") FROM sales"
		if result != expected {
			t.Errorf("Expected: %s, Got: %s", expected, result)
		}
	})

	t.Run("PostgreSQL CASE WHEN for not_null", func(t *testing.T) {
		check := &dbqcore.DataQualityCheck{
			Expression:  "not_null(description)",
			ParsedCheck: createMockParsedCheck("not_null", []string{"description"}, "", nil),
		}

		result, err := adapter.InterpretDataQualityCheck(check, "products", "active = true")
		if err != nil {
			t.Errorf("Unexpected error: %v", err)
		}

		expected := "SELECT COUNT(CASE WHEN \"description\" IS NOT NULL THEN 1 END) FROM products WHERE active = true"
		if result != expected {
			t.Errorf("Expected: %s, Got: %s", expected, result)
		}
	})

	t.Run("PostgreSQL EXTRACT with EPOCH for freshness", func(t *testing.T) {
		check := &dbqcore.DataQualityCheck{
			Expression:  "freshness(timestamp_col)",
			ParsedCheck: createMockParsedCheck("freshness", []string{"timestamp_col"}, "", nil),
		}

		result, err := adapter.InterpretDataQualityCheck(check, "events", "")
		if err != nil {
			t.Errorf("Unexpected error: %v", err)
		}

		expected := "SELECT EXTRACT(EPOCH FROM (NOW() - MAX(\"timestamp_col\"))) FROM events"
		if result != expected {
			t.Errorf("Expected: %s, Got: %s", expected, result)
		}
	})

	t.Run("PostgreSQL raw query with AND conjunction", func(t *testing.T) {
		check := &dbqcore.DataQualityCheck{
			Expression:  "raw_query",
			Query:       "SELECT COUNT(*) FROM {{dataset}} WHERE status = 'active'",
			ParsedCheck: createMockParsedCheck("raw_query", []string{}, "", nil),
		}

		result, err := adapter.InterpretDataQualityCheck(check, "users", "created_at > '2024-01-01'")
		if err != nil {
			t.Errorf("Unexpected error: %v", err)
		}

		expected := "SELECT COUNT(*) FROM users WHERE status = 'active' AND (created_at > '2024-01-01')"
		if result != expected {
			t.Errorf("Expected: %s, Got: %s", expected, result)
		}
	})
}

func TestPostgreSQLAdapter_EdgeCases(t *testing.T) {
	adapter := createMockPostgreSQLAdapter()

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

	t.Run("boolean where clauses with PostgreSQL syntax", func(t *testing.T) {
		check := &dbqcore.DataQualityCheck{
			Expression:  "row_count",
			ParsedCheck: createMockParsedCheck("row_count", []string{}, "", nil),
		}

		result, err := adapter.InterpretDataQualityCheck(check, "users", "active = true AND verified = false")
		if err != nil {
			t.Errorf("Unexpected error: %v", err)
		}

		expected := "SELECT COUNT(*) FROM users WHERE active = true AND verified = false"
		if result != expected {
			t.Errorf("Expected: %s, Got: %s", expected, result)
		}
	})

	t.Run("case sensitivity in column names", func(t *testing.T) {
		check := &dbqcore.DataQualityCheck{
			Expression:  "not_null(CustomerID)",
			ParsedCheck: createMockParsedCheck("not_null", []string{"CustomerID"}, "", nil),
		}

		result, err := adapter.InterpretDataQualityCheck(check, "Orders", "")
		if err != nil {
			t.Errorf("Unexpected error: %v", err)
		}

		expected := "SELECT COUNT(CASE WHEN \"CustomerID\" IS NOT NULL THEN 1 END) FROM Orders"
		if result != expected {
			t.Errorf("Expected: %s, Got: %s", expected, result)
		}
	})
}
