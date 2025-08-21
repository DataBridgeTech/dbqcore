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

type MysqlDbqDataSourceAdapter struct {
	db     *sql.DB
	logger *slog.Logger
}

func NewMysqlDbqDataSourceAdapter(db *sql.DB, logger *slog.Logger) dbqcore.DbqDataSourceAdapter {
	if logger == nil {
		logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}

	return &MysqlDbqDataSourceAdapter{
		db:     db,
		logger: logger,
	}
}

func (a *MysqlDbqDataSourceAdapter) InterpretDataQualityCheck(check *dbqcore.DataQualityCheck, dataset string, whereClause string) (string, error) {
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

	// Build SQL query based on parsed check structure
	var sqlQuery string
	var selectExpression string

	switch parsed.FunctionName {
	case "row_count":
		selectExpression = "COUNT(*)"

	case "not_null":
		if err := requireParameter("not_null", parsed.FunctionParameters); err != nil {
			return "", err
		}
		column := parsed.FunctionParameters[0]
		selectExpression = fmt.Sprintf("SUM(CASE WHEN `%s` IS NOT NULL THEN 1 ELSE 0 END)", column)

	case "uniqueness":
		if err := requireParameter("uniqueness", parsed.FunctionParameters); err != nil {
			return "", err
		}
		column := parsed.FunctionParameters[0]
		selectExpression = fmt.Sprintf("COUNT(DISTINCT `%s`)", column)

	case "freshness":
		if err := requireParameter("freshness", parsed.FunctionParameters); err != nil {
			return "", err
		}
		column := parsed.FunctionParameters[0]
		selectExpression = fmt.Sprintf("TIMESTAMPDIFF(SECOND, MAX(`%s`), NOW())", column)

	case "min":
		if err := requireParameter("min", parsed.FunctionParameters); err != nil {
			return "", err
		}
		column := parsed.FunctionParameters[0]
		selectExpression = fmt.Sprintf("MIN(`%s`)", column)

	case "max":
		if err := requireParameter("max", parsed.FunctionParameters); err != nil {
			return "", err
		}
		column := parsed.FunctionParameters[0]
		selectExpression = fmt.Sprintf("MAX(`%s`)", column)

	case "avg":
		if err := requireParameter("avg", parsed.FunctionParameters); err != nil {
			return "", err
		}
		column := parsed.FunctionParameters[0]
		selectExpression = fmt.Sprintf("AVG(`%s`)", column)

	case "sum":
		if err := requireParameter("sum", parsed.FunctionParameters); err != nil {
			return "", err
		}
		column := parsed.FunctionParameters[0]
		selectExpression = fmt.Sprintf("SUM(`%s`)", column)

	case "stddev":
		if err := requireParameter("stddev", parsed.FunctionParameters); err != nil {
			return "", err
		}
		column := parsed.FunctionParameters[0]
		selectExpression = fmt.Sprintf("STDDEV_POP(`%s`)", column)

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

	a.logger.Debug("Generated MySQL query",
		"function", parsed.FunctionName,
		"operator", parsed.Operator,
		"query", sqlQuery)

	return sqlQuery, nil
}

func (a *MysqlDbqDataSourceAdapter) ExecuteQuery(ctx context.Context, query string) (string, bool, error) {
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
