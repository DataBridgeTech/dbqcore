package validators

import (
	"context"
	"fmt"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/DataBridgeTech/dbqcore"
	"io"
	"log/slog"
	"regexp"
	"strings"
	"time"
)

type ClickhouseDbqDataValidator struct {
	cnn    driver.Conn
	logger *slog.Logger
}

func NewClickhouseDbqDataValidator(cnn driver.Conn, logger *slog.Logger) dbqcore.DbqDataValidator {
	if logger == nil {
		logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}

	return &ClickhouseDbqDataValidator{
		cnn:    cnn,
		logger: logger,
	}
}

func (c *ClickhouseDbqDataValidator) RunCheck(check *dbqcore.DataQualityCheck, dataset string, defaultWhere string) (bool, string, error) {
	if c.cnn == nil {
		return false, "", fmt.Errorf("database connection is not initialized")
	}

	query, err := generateDataCheckQuery(check, dataset, defaultWhere, c.logger)
	if err != nil {
		return false, "", fmt.Errorf("failed to generate SQL for check (%s)/(%s): %s", check.ID, dataset, err.Error())
	}

	c.logger.Debug("executing query for check",
		"check_id", check.ID,
		"query", query)

	startTime := time.Now()
	rows, err := c.cnn.Query(context.Background(), query)
	if err != nil {
		return false, "", fmt.Errorf("failed to query database: %w", err)
	}
	defer rows.Close()
	elapsed := time.Since(startTime).Milliseconds()

	c.logger.Debug("query completed in time",
		"check_id", check.ID,
		"duration_ms", elapsed)

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

func generateDataCheckQuery(check *dbqcore.DataQualityCheck, dataset string, whereClause string, logger *slog.Logger) (string, error) {
	var sqlQuery string

	// handle raw_query first
	if check.ID == dbqcore.CheckTypeRawQuery {
		if check.Query == "" {
			return "", fmt.Errorf("check with id 'raw_query' requires a 'query' field")
		}

		sqlQuery = strings.ReplaceAll(check.Query, "{{table}}", dataset)
		if whereClause != "" {
			// todo: more sophisticated check is needed
			if strings.Contains(strings.ToLower(sqlQuery), " where ") {
				sqlQuery = fmt.Sprintf("%s and (%s)", sqlQuery, whereClause)
			} else {
				sqlQuery = fmt.Sprintf("%s where %s", sqlQuery, whereClause)
			}
		}

		return sqlQuery, nil
	}

	isAggFunction := startWithAnyOf([]string{
		"min", "max", "avg", "stddevPop", "sum",
	}, strings.ToLower(check.ID))

	var checkExpression string
	parts := strings.Fields(check.ID)
	if len(parts) < 3 {
		return "", fmt.Errorf("invalid format for check: %s", check.ID)
	}

	// todo: extract matcher to make it DataSource agnostic
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
		logger.Warn("DataQualityCheck did not match known check patterns. Assuming it's a direct SQL boolean expression",
			"check_id", check.ID)
		checkExpression = check.ID
	}

	sqlQuery = fmt.Sprintf("select %s from %s", checkExpression, dataset)
	if whereClause != "" {
		sqlQuery = fmt.Sprintf("%s where %s", sqlQuery, whereClause)
	}

	return sqlQuery, nil
}

func startWithAnyOf(prefixes []string, s string) bool {
	for _, prefix := range prefixes {
		if strings.HasPrefix(s, strings.ToLower(prefix)) {
			return true
		}
	}
	return false
}
