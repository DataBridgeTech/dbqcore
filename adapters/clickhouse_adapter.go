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
	"regexp"
	"strings"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/DataBridgeTech/dbqcore"
	"github.com/DataBridgeTech/dbqcore/utils"
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
	var sqlQuery string

	if check.ID == dbqcore.CheckTypeRawQuery {
		if check.Query == "" {
			return "", fmt.Errorf("check with id 'raw_query' requires a 'query' field")
		}

		sqlQuery = strings.ReplaceAll(check.Query, "{{dataset}}", dataset)
		sqlQuery = strings.ReplaceAll(sqlQuery, "\n", " ")
		sqlQuery = strings.ToLower(sqlQuery)

		if whereClause != "" {
			if strings.Contains(sqlQuery, " where ") {
				sqlQuery = fmt.Sprintf("%s and (%s)", sqlQuery, whereClause)
			} else {
				sqlQuery = fmt.Sprintf("%s where %s", sqlQuery, whereClause)
			}
		}

		return sqlQuery, nil
	}

	isAggFunction := utils.StartWithAnyOf([]string{
		"min", "max", "avg", "stddevPop", "sum",
	}, strings.ToLower(check.ID))

	var checkExpression string
	parts := strings.Fields(check.ID)
	if len(parts) < 3 {
		return "", fmt.Errorf("invalid format for check: %s", check.ID)
	}

	switch {
	case strings.HasPrefix(check.ID, "row_count"):
		checkExpression = strings.Replace(check.ID, "row_count", "count()", 1)

	case strings.HasPrefix(check.ID, "null_count"):
		re := regexp.MustCompile(`^null_count\((.*?)\)(.*)`)
		matches := re.FindStringSubmatch(check.ID)
		if len(matches) < 3 {
			return "", fmt.Errorf("invalid format for null_count check: %s", check.ID)
		}

		column := matches[1]
		remainder := matches[2]
		checkExpression = fmt.Sprintf("countIf(isNull(%s))%s", column, remainder)

	case isAggFunction:
		re := regexp.MustCompile(`^(min|max|avg|stddevPop|sum)\((.*?)\)(.*)`)
		matches := re.FindStringSubmatch(check.ID)
		if len(matches) < 3 {
			fmt.Println(matches, " --- ", len(matches))
			return "", fmt.Errorf("invalid format for aggregation function check: %s", check.ID)
		}

		checkExpression = matches[0]

	default:
		// assume the ID itself is a valid boolean expression if no specific pattern matches
		// this is less robust but covers simple cases
		a.logger.Warn("DataQualityCheck did not match known check patterns. Assuming it's a direct SQL boolean expression",
			"check_id", check.ID)
		checkExpression = check.ID
	}

	sqlQuery = fmt.Sprintf("select %s from %s", checkExpression, dataset)
	if whereClause != "" {
		sqlQuery = fmt.Sprintf("%s where %s", sqlQuery, whereClause)
	}

	return sqlQuery, nil
}

func (a *ClickhouseDbqDataSourceAdapter) ExecuteQuery(ctx context.Context, query string) (string, bool, error) {
	rows, err := a.cnn.Query(ctx, query)
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
