package adapters

import (
	"io"
	"log/slog"
	"testing"

	"github.com/DataBridgeTech/dbqcore"
)

func createMockMySQLAdapter() *MysqlDbqDataSourceAdapter {
	return &MysqlDbqDataSourceAdapter{
		db:     nil, // We don't need actual connection for InterpretDataQualityCheck tests
		logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
}

func TestMySQLAdapter_InterpretDataQualityCheck(t *testing.T) {
	adapter := createMockMySQLAdapter()

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
			name: "not_null check with MySQL-specific syntax",
			check: &dbqcore.DataQualityCheck{
				Expression:  "not_null(customer_id)",
				ParsedCheck: createMockParsedCheck("not_null", []string{"customer_id"}, "", nil),
			},
			dataset:     "customers",
			whereClause: "",
			expectedSQL: "SELECT SUM(CASE WHEN `customer_id` IS NOT NULL THEN 1 ELSE 0 END) FROM customers",
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
			name: "uniqueness check with MySQL backticks",
			check: &dbqcore.DataQualityCheck{
				Expression:  "uniqueness(email)",
				ParsedCheck: createMockParsedCheck("uniqueness", []string{"email"}, "", nil),
			},
			dataset:     "users",
			whereClause: "",
			expectedSQL: "SELECT COUNT(DISTINCT `email`) FROM users",
		},
		{
			name: "freshness check with TIMESTAMPDIFF",
			check: &dbqcore.DataQualityCheck{
				Expression:  "freshness(last_updated) < 3600",
				ParsedCheck: createMockParsedCheck("freshness", []string{"last_updated"}, "<", 3600),
			},
			dataset:     "products",
			whereClause: "",
			expectedSQL: "SELECT TIMESTAMPDIFF(SECOND, MAX(`last_updated`), NOW()) FROM products",
		},
		{
			name: "min check",
			check: &dbqcore.DataQualityCheck{
				Expression:  "min(price) > 0",
				ParsedCheck: createMockParsedCheck("min", []string{"price"}, ">", 0),
			},
			dataset:     "items",
			whereClause: "",
			expectedSQL: "SELECT MIN(`price`) FROM items",
		},
		{
			name: "max check with where clause",
			check: &dbqcore.DataQualityCheck{
				Expression:  "max(quantity) < 1000",
				ParsedCheck: createMockParsedCheck("max", []string{"quantity"}, "<", 1000),
			},
			dataset:     "inventory",
			whereClause: "active = 1",
			expectedSQL: "SELECT MAX(`quantity`) FROM inventory WHERE active = 1",
		},
		{
			name: "avg check",
			check: &dbqcore.DataQualityCheck{
				Expression:  "avg(rating) between 3.0 and 5.0",
				ParsedCheck: createMockParsedCheck("avg", []string{"rating"}, "between", dbqcore.BetweenRange{Min: 3.0, Max: 5.0}),
			},
			dataset:     "reviews",
			whereClause: "",
			expectedSQL: "SELECT AVG(`rating`) FROM reviews",
		},
		{
			name: "sum check",
			check: &dbqcore.DataQualityCheck{
				Expression:  "sum(total_amount) > 10000",
				ParsedCheck: createMockParsedCheck("sum", []string{"total_amount"}, ">", 10000),
			},
			dataset:     "transactions",
			whereClause: "",
			expectedSQL: "SELECT SUM(`total_amount`) FROM transactions",
		},
		{
			name: "stddev check with STDDEV_POP",
			check: &dbqcore.DataQualityCheck{
				Expression:  "stddev(response_time) < 100",
				ParsedCheck: createMockParsedCheck("stddev", []string{"response_time"}, "<", 100),
			},
			dataset:     "api_calls",
			whereClause: "",
			expectedSQL: "SELECT STDDEV_POP(`response_time`) FROM api_calls",
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
			expectedSQL: "SELECT COUNT(*) FROM products where category = 'electronics'",
		},
		{
			name: "raw_query check with existing where clause",
			check: &dbqcore.DataQualityCheck{
				Expression:  "raw_query",
				Query:       "SELECT AVG(price) FROM {{dataset}} WHERE category = 'books'",
				ParsedCheck: createMockParsedCheck("raw_query", []string{}, "", nil),
			},
			dataset:     "products",
			whereClause: "active = 1",
			expectedSQL: "SELECT AVG(price) FROM products WHERE category = 'books' and (active = 1)",
		},
		{
			name: "raw_query check with multiline query",
			check: &dbqcore.DataQualityCheck{
				Expression:  "raw_query",
				Query:       "SELECT COUNT(*)\nFROM {{dataset}}\nWHERE active = 1",
				ParsedCheck: createMockParsedCheck("raw_query", []string{}, "", nil),
			},
			dataset:     "orders",
			whereClause: "",
			expectedSQL: "SELECT COUNT(*) FROM orders WHERE active = 1",
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
			dataset:     "mydb.users",
			whereClause: "",
			expectedSQL: `select count(*)
				from information_schema.columns
				where table_schema = 'mydb'
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
			dataset:     "testdb.products",
			whereClause: "",
			expectedSQL: `select count(*)
				from information_schema.columns
				where table_schema = 'testdb'
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
			name: "expect_columns check with multiple columns",
			check: &dbqcore.DataQualityCheck{
				Expression: "expect_columns",
				SchemaCheck: &dbqcore.SchemaCheckConfig{
					ExpectColumns: &dbqcore.ExpectColumnsConfig{
						Columns: []string{"order_id", "customer_id", "total_amount"},
					},
				},
			},
			dataset:     "ecommerce.orders",
			whereClause: "",
			expectedSQL: `select count(*)
				from information_schema.columns
				where table_schema = 'ecommerce'
				and table_name = 'orders'
				and (column_name = 'order_id' or column_name = 'customer_id' or column_name = 'total_amount')`,
		},
		{
			name: "expect_columns check with two columns",
			check: &dbqcore.DataQualityCheck{
				Expression: "expect_columns",
				SchemaCheck: &dbqcore.SchemaCheckConfig{
					ExpectColumns: &dbqcore.ExpectColumnsConfig{
						Columns: []string{"product_id", "price"},
					},
				},
			},
			dataset:     "shop.products",
			whereClause: "",
			expectedSQL: `select count(*)
				from information_schema.columns
				where table_schema = 'shop'
				and table_name = 'products'
				and (column_name = 'product_id' or column_name = 'price')`,
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
			name: "columns_not_present check with column list",
			check: &dbqcore.DataQualityCheck{
				Expression: "columns_not_present",
				SchemaCheck: &dbqcore.SchemaCheckConfig{
					ColumnsNotPresent: &dbqcore.ColumnsNotPresentConfig{
						Columns: []string{"credit_card_number", "ssn", "password"},
					},
				},
			},
			dataset:     "mydb.users",
			whereClause: "",
			expectedSQL: `select count(*)
				from information_schema.columns
				where table_schema = 'mydb'
				and table_name = 'users'
				and (column_name = 'credit_card_number' or column_name = 'ssn' or column_name = 'password')`,
		},
		{
			name: "columns_not_present check with pattern",
			check: &dbqcore.DataQualityCheck{
				Expression: "columns_not_present",
				SchemaCheck: &dbqcore.SchemaCheckConfig{
					ColumnsNotPresent: &dbqcore.ColumnsNotPresentConfig{
						Pattern: "temp_*",
					},
				},
			},
			dataset:     "shop.products",
			whereClause: "",
			expectedSQL: `select count(*)
				from information_schema.columns
				where table_schema = 'shop'
				and table_name = 'products'
				and (column_name LIKE 'temp_%')`,
		},
		{
			name: "columns_not_present check with both columns and pattern",
			check: &dbqcore.DataQualityCheck{
				Expression: "columns_not_present",
				SchemaCheck: &dbqcore.SchemaCheckConfig{
					ColumnsNotPresent: &dbqcore.ColumnsNotPresentConfig{
						Columns: []string{"pan", "cvv"},
						Pattern: "card_*",
					},
				},
			},
			dataset:     "ecommerce.orders",
			whereClause: "",
			expectedSQL: `select count(*)
				from information_schema.columns
				where table_schema = 'ecommerce'
				and table_name = 'orders'
				and (column_name = 'pan' or column_name = 'cvv' or column_name LIKE 'card_%')`,
		},
		{
			name: "columns_not_present check with neither columns nor pattern",
			check: &dbqcore.DataQualityCheck{
				Expression: "columns_not_present",
				SchemaCheck: &dbqcore.SchemaCheckConfig{
					ColumnsNotPresent: &dbqcore.ColumnsNotPresentConfig{},
				},
			},
			dataset:      "test.table",
			whereClause:  "",
			expectError:  true,
			errorMessage: "columns_not_present check requires either 'columns' list or 'pattern'",
		},
		{
			name: "columns_not_present check invalid dataset format",
			check: &dbqcore.DataQualityCheck{
				Expression: "columns_not_present",
				SchemaCheck: &dbqcore.SchemaCheckConfig{
					ColumnsNotPresent: &dbqcore.ColumnsNotPresentConfig{
						Pattern: "debug_*",
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

func TestMySQLAdapter_DatabaseSpecificFeatures(t *testing.T) {
	adapter := createMockMySQLAdapter()

	t.Run("MySQL backtick quoting for reserved words", func(t *testing.T) {
		check := &dbqcore.DataQualityCheck{
			Expression:  "min(order)",
			ParsedCheck: createMockParsedCheck("min", []string{"order"}, ">", 0),
		}

		result, err := adapter.InterpretDataQualityCheck(check, "sales", "")
		if err != nil {
			t.Errorf("Unexpected error: %v", err)
		}

		expected := "SELECT MIN(`order`) FROM sales"
		if result != expected {
			t.Errorf("Expected: %s, Got: %s", expected, result)
		}
	})

	t.Run("MySQL CASE WHEN for not_null", func(t *testing.T) {
		check := &dbqcore.DataQualityCheck{
			Expression:  "not_null(description)",
			ParsedCheck: createMockParsedCheck("not_null", []string{"description"}, "", nil),
		}

		result, err := adapter.InterpretDataQualityCheck(check, "products", "active = 1")
		if err != nil {
			t.Errorf("Unexpected error: %v", err)
		}

		expected := "SELECT SUM(CASE WHEN `description` IS NOT NULL THEN 1 ELSE 0 END) FROM products WHERE active = 1"
		if result != expected {
			t.Errorf("Expected: %s, Got: %s", expected, result)
		}
	})
}
