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

package validators

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"regexp"
	"strings"

	"github.com/DataBridgeTech/dbqcore"
)

type MysqlDbqDataValidator struct {
	db     *sql.DB
	logger *slog.Logger
}

func NewMysqlDbqDataValidator(db *sql.DB, logger *slog.Logger) dbqcore.DbqDataValidator {
	return &MysqlDbqDataValidator{db: db, logger: logger}
}

func (v *MysqlDbqDataValidator) RunCheck(ctx context.Context, check *dbqcore.DataQualityCheck, dataset string, defaultWhere string) (bool, string, error) {
	query, err := v.generateDataCheckQuery(check, dataset, defaultWhere)
	if err != nil {
		return false, "", fmt.Errorf("failed to generate SQL for check (%s)/(%s): %w", check.ID, dataset, err)
	}

	v.logger.Debug("executing query for check",
		"check_id", check.ID,
		"query", query)

	rows, err := v.db.QueryContext(ctx, query)
	if err != nil {
		return false, "", fmt.Errorf("failed to query database: %w", err)
	}
	defer rows.Close()

	var checkPassed bool
	for rows.Next() {
		if err := rows.Scan(&checkPassed); err != nil {
			return false, "", fmt.Errorf("failed to scan row: %w", err)
		}
	}

	if err = rows.Err(); err != nil {
		return false, "", fmt.Errorf("error occurred during row iteration: %w", err)
	}

	return checkPassed, "", nil
}

func (v *MysqlDbqDataValidator) generateDataCheckQuery(check *dbqcore.DataQualityCheck, dataset string, whereClause string) (string, error) {
	var sqlQuery string

	if check.ID == dbqcore.CheckTypeRawQuery {
		if check.Query == "" {
			return "", fmt.Errorf("check with id 'raw_query' requires a 'query' field")
		}

		sqlQuery = strings.ReplaceAll(check.Query, "{{table}}", dataset)
		if whereClause != "" {
			if strings.Contains(strings.ToLower(sqlQuery), " where ") {
				sqlQuery = fmt.Sprintf("%s AND (%s)", sqlQuery, whereClause)
			} else {
				sqlQuery = fmt.Sprintf("%s WHERE %s", sqlQuery, whereClause)
			}
		}

		return sqlQuery, nil
	}

	isAggFunction := v.startWithAnyOf([]string{
		"min", "max", "avg", "stddev_pop", "sum",
	}, strings.ToLower(check.ID))

	var checkExpression string
	parts := strings.Fields(check.ID)
	if len(parts) < 3 {
		return "", fmt.Errorf("invalid format for check: %s", check.ID)
	}

	switch {
	case strings.HasPrefix(check.ID, "row_count"):
		checkExpression = strings.Replace(check.ID, "row_count", "count(*)", 1)

	case strings.HasPrefix(check.ID, "null_count"):
		re := regexp.MustCompile(`^null_count\((.*?)\)(.*)`)
		matches := re.FindStringSubmatch(check.ID)
		if len(matches) < 3 {
			return "", fmt.Errorf("invalid format for null_count check: %s", check.ID)
		}

		column := matches[1]
		remainder := matches[2]
		checkExpression = fmt.Sprintf("count_if(`%s` is null)%s", column, remainder)

	case isAggFunction:
		re := regexp.MustCompile(`^(min|max|avg|stddev_pop|sum)\((.*?)\)(.*)`)
		matches := re.FindStringSubmatch(check.ID)
		if len(matches) < 3 {
			return "", fmt.Errorf("invalid format for aggregation function check: %s", check.ID)
		}

		checkExpression = matches[0]

	default:
		v.logger.Warn("DataQualityCheck did not match known check patterns. Assuming it's a direct SQL boolean expression",
			"check_id", check.ID)
		checkExpression = check.ID
	}

	sqlQuery = fmt.Sprintf("SELECT %s FROM %s", checkExpression, dataset)
	if whereClause != "" {
		sqlQuery = fmt.Sprintf("%s WHERE %s", sqlQuery, whereClause)
	}

	return sqlQuery, nil
}

func (v *MysqlDbqDataValidator) startWithAnyOf(prefixes []string, s string) bool {
	for _, prefix := range prefixes {
		if strings.HasPrefix(s, strings.ToLower(prefix)) {
			return true
		}
	}
	return false
}
