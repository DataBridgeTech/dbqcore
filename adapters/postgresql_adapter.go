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
	"database/sql"
	"fmt"
	"io"
	"log/slog"
	"strings"

	"github.com/DataBridgeTech/dbqcore"
)

type PostgresqlDbqDataSourceAdapter struct {
	db     *sql.DB
	logger *slog.Logger
}

func NewPostgresqlDbqDataSourceAdapter(db *sql.DB, logger *slog.Logger) dbqcore.DbqDataSourceAdapter {
	if logger == nil {
		logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}

	return &PostgresqlDbqDataSourceAdapter{
		db:     db,
		logger: logger,
	}
}

func (a *PostgresqlDbqDataSourceAdapter) InterpretDataQualityCheck(check *dbqcore.DataQualityCheck, dataset string, whereClause string) (string, error) {
	if check.ParsedCheck == nil {
		return "", fmt.Errorf("check does not have parsed structure")
	}

	parsed := check.ParsedCheck

	// Handle raw_query checks
	if parsed.FunctionName == "raw_query" {
		if check.Query == "" {
			return "", fmt.Errorf("raw_query check requires a 'query' field")
		}

		sqlQuery := strings.ReplaceAll(check.Query, "{{dataset}}", dataset)
		sqlQuery = strings.ReplaceAll(sqlQuery, "\n", " ")

		if whereClause != "" {
			if strings.Contains(strings.ToLower(sqlQuery), " where ") {
				sqlQuery = fmt.Sprintf("%s AND (%s)", sqlQuery, whereClause)
			} else {
				sqlQuery = fmt.Sprintf("%s WHERE %s", sqlQuery, whereClause)
			}
		}

		return sqlQuery, nil
	}

	// build SQL query based on parsed check structure
	var sqlQuery string
	var selectExpression string

	switch parsed.FunctionName {
	case "row_count":
		selectExpression = "COUNT(*)"

	case "not_null":
		if len(parsed.FunctionParameters) == 0 {
			return "", fmt.Errorf("not_null check requires a column parameter")
		}
		column := parsed.FunctionParameters[0]
		selectExpression = fmt.Sprintf("COUNT(CASE WHEN \"%s\" IS NOT NULL THEN 1 END)", column)

	case "uniqueness":
		if len(parsed.FunctionParameters) == 0 {
			return "", fmt.Errorf("uniqueness check requires a column parameter")
		}
		column := parsed.FunctionParameters[0]
		selectExpression = fmt.Sprintf("COUNT(DISTINCT \"%s\")", column)

	case "freshness":
		if len(parsed.FunctionParameters) == 0 {
			return "", fmt.Errorf("freshness check requires a column parameter")
		}
		column := parsed.FunctionParameters[0]
		selectExpression = fmt.Sprintf("EXTRACT(EPOCH FROM (NOW() - MAX(\"%s\")))", column)

	case "min":
		if len(parsed.FunctionParameters) == 0 {
			return "", fmt.Errorf("min check requires a column parameter")
		}
		column := parsed.FunctionParameters[0]
		selectExpression = fmt.Sprintf("MIN(\"%s\")", column)

	case "max":
		if len(parsed.FunctionParameters) == 0 {
			return "", fmt.Errorf("max check requires a column parameter")
		}
		column := parsed.FunctionParameters[0]
		selectExpression = fmt.Sprintf("MAX(\"%s\")", column)

	case "avg":
		if len(parsed.FunctionParameters) == 0 {
			return "", fmt.Errorf("avg check requires a column parameter")
		}
		column := parsed.FunctionParameters[0]
		selectExpression = fmt.Sprintf("AVG(\"%s\")", column)

	case "sum":
		if len(parsed.FunctionParameters) == 0 {
			return "", fmt.Errorf("sum check requires a column parameter")
		}
		column := parsed.FunctionParameters[0]
		selectExpression = fmt.Sprintf("SUM(\"%s\")", column)

	case "stddev":
		if len(parsed.FunctionParameters) == 0 {
			return "", fmt.Errorf("stddev check requires a column parameter")
		}
		column := parsed.FunctionParameters[0]
		selectExpression = fmt.Sprintf("STDDEV_POP(\"%s\")", column)

	case "avgWeighted":
		if len(parsed.FunctionParameters) < 2 {
			return "", fmt.Errorf("avgWeighted check requires two parameters: column and weight")
		}
		column := parsed.FunctionParameters[0]
		weight := parsed.FunctionParameters[1]
		// PostgreSQL doesn't have built-in weighted average, so we calculate it manually
		selectExpression = fmt.Sprintf("SUM(\"%s\" * \"%s\") / SUM(\"%s\")", column, weight, weight)

	default:
		a.logger.Warn("Unknown function name in parsed check, using original expression",
			"function_name", parsed.FunctionName,
			"expression", check.Expression)
		selectExpression = check.Expression
	}

	// build the complete query
	sqlQuery = fmt.Sprintf("SELECT %s FROM %s", selectExpression, dataset)

	if whereClause != "" {
		sqlQuery = fmt.Sprintf("%s WHERE %s", sqlQuery, whereClause)
	}

	a.logger.Debug("Generated PostgreSQL query",
		"function", parsed.FunctionName,
		"operator", parsed.Operator,
		"query", sqlQuery)

	return sqlQuery, nil
}

func (a *PostgresqlDbqDataSourceAdapter) ExecuteQuery(ctx context.Context, query string) (string, bool, error) {
	rows, err := a.db.QueryContext(ctx, query)
	if err != nil {
		return "", false, fmt.Errorf("failed to execute query for check: %v", err)
	}
	defer rows.Close()

	var queryResult string
	var pass bool
	for rows.Next() {
		if err := rows.Scan(&queryResult, &pass); err != nil {
			return "", false, fmt.Errorf("failed to scan result for check: %v", err)
		}
	}

	if err = rows.Err(); err != nil {
		return "", false, fmt.Errorf("failed to scan result for check: %v", err)
	}

	return queryResult, pass, nil
}
