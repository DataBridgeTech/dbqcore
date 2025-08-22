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

package adapters

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"strings"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/DataBridgeTech/dbqcore"
)

type ClickhouseDbqDataSourceAdapter struct {
	cnn    driver.Conn
	logger *slog.Logger
}

func NewClickhouseDbqDataSourceAdapter(cnn driver.Conn, logger *slog.Logger) dbqcore.DbqDataSourceAdapter {
	if logger == nil {
		logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}

	return &ClickhouseDbqDataSourceAdapter{
		cnn:    cnn,
		logger: logger,
	}
}

func (a *ClickhouseDbqDataSourceAdapter) InterpretDataQualityCheck(check *dbqcore.DataQualityCheck, dataset string, whereClause string) (string, error) {
	// handle schema checks first
	if check.SchemaCheck != nil {
		database, table, err := extractDatabaseAndTableFromDataset(dataset)
		if err != nil {
			return "", err
		}

		if check.SchemaCheck.ExpectColumnsOrdered != nil {
			expectedColumns := check.SchemaCheck.ExpectColumnsOrdered.ColumnsOrder
			columnChecks := make([]string, len(expectedColumns))
			for i, col := range expectedColumns {
				columnChecks[i] = fmt.Sprintf("(name = '%s' and position = %d)", col, i+1)
			}

			// count of matching columns in correct positions
			sqlQuery := fmt.Sprintf(`select count()
				from system.columns
				where database = '%s'
				and table = '%s'
				and (%s)`, database, table, strings.Join(columnChecks, " or "))

			return sqlQuery, nil
		}

		if check.SchemaCheck.ExpectColumns != nil {
			// Query returns count of matching columns
			expectedColumns := check.SchemaCheck.ExpectColumns.Columns
			columnChecks := make([]string, len(expectedColumns))
			for i, col := range expectedColumns {
				columnChecks[i] = fmt.Sprintf("name = '%s'", col)
			}

			sqlQuery := fmt.Sprintf(`select count()
				from system.columns
				where database = '%s'
				and table = '%s'
				and (%s)`, database, table, strings.Join(columnChecks, " or "))

			return sqlQuery, nil
		}
	}

	if check.ParsedCheck == nil {
		return "", fmt.Errorf("check does not have parsed structure")
	}

	parsed := check.ParsedCheck

	// handle raw_query checks
	if parsed.FunctionName == "raw_query" {
		if check.Query == "" {
			return "", fmt.Errorf("raw_query check requires a 'query' field")
		}

		sqlQuery := strings.ReplaceAll(check.Query, "{{dataset}}", dataset)
		sqlQuery = strings.ReplaceAll(sqlQuery, "\n", " ")

		if whereClause != "" {
			if strings.Contains(strings.ToLower(sqlQuery), " where ") {
				sqlQuery = fmt.Sprintf("%s and (%s)", sqlQuery, whereClause)
			} else {
				sqlQuery = fmt.Sprintf("%s where %s", sqlQuery, whereClause)
			}
		}

		return sqlQuery, nil
	}

	// build SQL query based on parsed check structure
	var sqlQuery string
	var selectExpression string

	switch parsed.FunctionName {
	case "row_count":
		selectExpression = "count()"

	case "not_null":
		if err := requireParameter("not_null", parsed.FunctionParameters); err != nil {
			return "", err
		}
		column := parsed.FunctionParameters[0]
		selectExpression = fmt.Sprintf("countIf(isNotNull(%s))", column)

	case "uniqueness":
		if err := requireParameter("uniqueness", parsed.FunctionParameters); err != nil {
			return "", err
		}
		column := parsed.FunctionParameters[0]
		selectExpression = fmt.Sprintf("uniqExact(%s)", column)

	case "freshness":
		if err := requireParameter("freshness", parsed.FunctionParameters); err != nil {
			return "", err
		}
		column := parsed.FunctionParameters[0]
		selectExpression = fmt.Sprintf("dateDiff('second', max(%s), now())", column)

	case "min":
		if err := requireParameter("min", parsed.FunctionParameters); err != nil {
			return "", err
		}
		column := parsed.FunctionParameters[0]
		selectExpression = fmt.Sprintf("min(%s)", column)

	case "max":
		if err := requireParameter("max", parsed.FunctionParameters); err != nil {
			return "", err
		}
		column := parsed.FunctionParameters[0]
		selectExpression = fmt.Sprintf("max(%s)", column)

	case "avg":
		if err := requireParameter("avg", parsed.FunctionParameters); err != nil {
			return "", err
		}
		column := parsed.FunctionParameters[0]
		selectExpression = fmt.Sprintf("avg(%s)", column)

	case "sum":
		if err := requireParameter("sum", parsed.FunctionParameters); err != nil {
			return "", err
		}
		column := parsed.FunctionParameters[0]
		selectExpression = fmt.Sprintf("sum(%s)", column)

	case "stddev":
		if err := requireParameter("stddev", parsed.FunctionParameters); err != nil {
			return "", err
		}
		column := parsed.FunctionParameters[0]
		selectExpression = fmt.Sprintf("stddevPop(%s)", column)

	default:
		a.logger.Warn("Unknown function name in parsed check, using original expression",
			"function_name", parsed.FunctionName,
			"expression", check.Expression)
		selectExpression = check.Expression
	}

	// build the complete query
	sqlQuery = fmt.Sprintf("select %s from %s", selectExpression, dataset)

	if whereClause != "" {
		sqlQuery = fmt.Sprintf("%s where %s", sqlQuery, whereClause)
	}

	a.logger.Debug("Generated ClickHouse query",
		"function", parsed.FunctionName,
		"operator", parsed.Operator,
		"query", sqlQuery)

	return sqlQuery, nil
}

func (a *ClickhouseDbqDataSourceAdapter) ExecuteQuery(ctx context.Context, query string) (string, error) {
	rows, err := a.cnn.Query(ctx, query)
	if err != nil {
		return "", fmt.Errorf("failed to execute query for check: %v", err)
	}
	defer rows.Close()

	var queryResult string
	for rows.Next() {
		if err := rows.Scan(&queryResult); err != nil {
			return "", fmt.Errorf("failed to scan result for check: %v", err)
		}
	}

	if err = rows.Err(); err != nil {
		return "", fmt.Errorf("failed to scan result for check: %v", err)
	}

	return queryResult, nil
}
