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

package dbqcore

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"log/slog"
	"reflect"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

type ClickhouseDbqConnector struct {
	cnn    driver.Conn
	logger *slog.Logger
}

func NewClickhouseDbqConnector(dataSource DataSource, logger *slog.Logger) (DbqConnector, error) {
	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{dataSource.Configuration.Host},
		Auth: clickhouse.Auth{
			Database: dataSource.Configuration.Database,
			Username: dataSource.Configuration.Username,
			Password: dataSource.Configuration.Password,
		},
		MaxOpenConns: 32,
		MaxIdleConns: 32,
		//TLS: &tls.Config{
		//	InsecureSkipVerify: true,
		//},
	})

	if logger == nil {
		// noop logger by default
		logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}

	return &ClickhouseDbqConnector{
		cnn:    conn,
		logger: logger,
	}, err
}

func (c *ClickhouseDbqConnector) Ping() (string, error) {
	info, err := c.cnn.ServerVersion()
	if err != nil {
		return "", err
	}

	return info.String(), nil
}

func (c *ClickhouseDbqConnector) ImportDatasets(filter string) ([]string, error) {
	if c.cnn == nil {
		return nil, fmt.Errorf("database connection is not initialized")
	}

	query := `
        select database, name
        from system.tables
        where 
            database not in ('system', 'INFORMATION_SCHEMA', 'information_schema')
			and not startsWith(name, '.')
			and is_temporary = 0`

	filter = fmt.Sprintf("%%%s%%", strings.TrimSpace(filter))
	if filter != "" {
		query += fmt.Sprintf(` and (database like '%s' or name like '%s')`, filter, filter)
	}
	query += ` order by database, name;`

	rows, err := c.cnn.Query(context.Background(), query)
	if err != nil {
		return nil, fmt.Errorf("failed to query system.tables: %w", err)
	}
	defer rows.Close()

	var datasets []string
	for rows.Next() {
		var databaseName, tableName string
		if err := rows.Scan(&databaseName, &tableName); err != nil {
			return nil, fmt.Errorf("failed to scan row: %w", err)
		}
		datasets = append(datasets, fmt.Sprintf("%s.%s", databaseName, tableName))
	}

	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("error occurred during row iteration: %w", err)
	}

	return datasets, nil
}

func (c *ClickhouseDbqConnector) ProfileDataset(dataset string, sample bool, maxConcurrent int) (*TableMetrics, error) {
	startTime := time.Now()
	ctx := context.Background()
	taskPool := NewTaskPool(maxConcurrent, c.logger)

	var databaseName, tableName string
	parts := strings.SplitN(dataset, ".", 2)
	if len(parts) == 2 {
		databaseName = strings.TrimSpace(parts[0])
		tableName = strings.TrimSpace(parts[1])
	}

	metrics := &TableMetrics{
		ProfiledAt:     time.Now().Unix(),
		TableName:      tableName,
		DatabaseName:   databaseName,
		ColumnsMetrics: make(map[string]*ColumnMetrics),
	}

	// Total Row Count
	err := c.cnn.QueryRow(ctx, fmt.Sprintf("select count() from %s", dataset)).Scan(&metrics.TotalRows)
	if err != nil {
		return nil, fmt.Errorf("failed to get total row count for %s: %w", dataset, err)
	}

	// Get Column Information (Name and Type)
	columnsToProcess, err := fetchColumns(c.cnn, ctx, databaseName, tableName)
	if err != nil {
		return metrics, err
	}

	if len(columnsToProcess) == 0 {
		c.logger.Warn("no columns found for table", dataset, ", returning basic info")
		metrics.ProfilingDurationMs = time.Since(startTime).Milliseconds()
		return metrics, nil
	}

	//

	c.logger.Debug(fmt.Sprintf("found %d columns to process", len(columnsToProcess)))

	// sample data if enabled
	if sample {
		taskPool.Enqueue("task:sampling", func() error {
			sampleQuery := fmt.Sprintf("select * from %s.%s order by rand() limit 100", databaseName, tableName)

			toCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()

			rows, err := c.cnn.Query(toCtx, sampleQuery)
			if err != nil {
				c.logger.Warn("failed to sample data",
					"dataset", tableName,
					"error", err.Error())
			}
			defer rows.Close()

			var allRows []map[string]interface{}
			for rows.Next() {
				scanArgs := make([]interface{}, len(rows.Columns()))
				for i, colType := range rows.ColumnTypes() {
					scanType := colType.ScanType()
					valuePtr := reflect.New(scanType).Interface()
					scanArgs[i] = valuePtr
				}

				err = rows.Scan(scanArgs...)
				if err != nil {
					c.logger.Warn("failed to scan row", "error", err)
					continue
				}

				rowData := make(map[string]interface{})
				for i, colName := range rows.Columns() {
					scannedValue := reflect.ValueOf(scanArgs[i]).Elem().Interface()
					rowData[colName] = scannedValue
				}

				allRows = append(allRows, rowData)
			}

			metrics.RowsSample = allRows
			return err
		})
	}

	// Calculate Metrics per Column
	for _, col := range columnsToProcess {
		var colWg sync.WaitGroup
		colStartTime := time.Now()

		c.logger.Debug("start column processing",
			"col_name", col.Name,
			"col_type", col.Type)

		colMetrics := &ColumnMetrics{
			ColumnName:     col.Name,
			DataType:       col.Type,
			ColumnComment:  col.Comment,
			ColumnPosition: col.Position,
		}

		taskIdPrefix := fmt.Sprintf("task:%s:", col.Name)

		// Null Count (all types)
		enqueueColumnTask(taskPool, &colWg, taskIdPrefix+"null_count", func() error {
			nullQuery := fmt.Sprintf("select count() from %s where isNull(%s)", dataset, col.Name)
			err := c.cnn.QueryRow(ctx, nullQuery).Scan(&colMetrics.NullCount)
			if err != nil {
				c.logger.Warn("failed to get NULL count",
					"error", err.Error(),
					"col_name", col.Name,
					"col_type", col.Type)
			}
			return err
		})

		// Blank Count (String types only)
		if isStringCHType(col.Type) {
			enqueueColumnTask(taskPool, &colWg, taskIdPrefix+"blank_count", func() error {
				blankQuery := fmt.Sprintf("select count() from %s where empty(%s)", dataset, col.Name)
				var blankCount uint64
				err := c.cnn.QueryRow(ctx, blankQuery).Scan(&blankCount)
				if err != nil {
					c.logger.Warn("failed to get blank count for column",
						"error", err.Error(),
						"col_name", col.Name,
						"col_type", col.Type)
					colMetrics.BlankCount = nil
				} else {
					val := int64(blankCount)
					colMetrics.BlankCount = &val
				}
				return err
			})
		}

		// Numeric Metrics (Numeric types only)
		if isNumericCHType(col.Type) {
			enqueueColumnTask(taskPool, &colWg, taskIdPrefix+"num_stats", func() error {
				// todo: check null handling
				numericQuery := fmt.Sprintf(`
                select
                    min(%s),
                    max(%s),
                    avg(%s),
                    stddevPop(%s)
                from %s`, col.Name, col.Name, col.Name, col.Name, dataset)

				var minValue sql.NullFloat64
				var maxValue sql.NullFloat64
				var avgValue sql.NullFloat64
				var stddevValue sql.NullFloat64

				err := c.cnn.QueryRow(ctx, numericQuery).Scan(
					&minValue,
					&maxValue,
					&avgValue,
					&stddevValue,
				)

				if err != nil {
					c.logger.Warn("failed to get numeric aggregates for column",
						"error", err.Error(),
						"col_name", col.Name,
						"col_type", col.Type)
				} else {
					if minValue.Valid {
						colMetrics.MinValue = &minValue.Float64
					}
					if maxValue.Valid {
						colMetrics.MaxValue = &maxValue.Float64
					}
					if avgValue.Valid {
						colMetrics.AvgValue = &avgValue.Float64
					}
					if stddevValue.Valid {
						colMetrics.StddevValue = &stddevValue.Float64
					}
				}

				return err
			})
		}

		// Most Frequent Value (all types - using topK)
		// topK(1) returns an array, we need to extract the first element if it exists
		// It handles NULL correctly. CAST to String for consistent retrieval.
		// Note: If the most frequent value is NULL, it should be represented correctly by sql.NullString
		enqueueColumnTask(taskPool, &colWg, taskIdPrefix+"mfv", func() error {
			mfvQuery := fmt.Sprintf("SELECT CAST(arrayElement(topK(1)(%s), 1), 'Nullable(String)') FROM %s", col.Name, dataset)
			err := c.cnn.QueryRow(ctx, mfvQuery).Scan(&colMetrics.MostFrequentValue)
			if err != nil {
				c.logger.Warn("failed to get most frequent value for column",
					"error", err.Error(),
					"col_name", col.Name,
					"col_type", col.Type)
				colMetrics.MostFrequentValue = nil
			}
			return err
		})

		go func() {
			colWg.Wait()
			elapsed := time.Since(colStartTime).Milliseconds()
			colMetrics.ProfilingDurationMs = elapsed
			metrics.ColumnsMetrics[col.Name] = colMetrics
			c.logger.Debug("finished processing column",
				"col_name", col.Name,
				"proc_duration_ms", elapsed)
		}()
	}

	// todo: add timeout, errors collection & cancellation
	taskPool.Join()

	metrics.ProfilingDurationMs = time.Since(startTime).Milliseconds()
	c.logger.Debug("finished data profiling for table",
		"dataset", dataset,
		"profile_duration_ms", metrics.ProfilingDurationMs)

	return metrics, nil
}

func enqueueColumnTask(taskPool *TaskPool, subsetWg *sync.WaitGroup, taskId string, task func() error) {
	subsetWg.Add(1)
	taskPool.Enqueue(taskId, func() error {
		defer subsetWg.Done()
		return task()
	})
}

func (c *ClickhouseDbqConnector) RunCheck(check *Check, dataset string, defaultWhere string) (bool, string, error) {
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

func fetchColumns(cnn driver.Conn, ctx context.Context, databaseName string, tableName string) ([]ColumnInfo, error) {
	columnQuery := `
        SELECT name, type, comment, position
        FROM system.columns
        WHERE database = ? AND table = ?
        ORDER BY position`

	rows, err := cnn.Query(ctx, columnQuery, databaseName, tableName)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch columns info for %s.%s: %w", databaseName, tableName, err)
	}
	defer rows.Close()

	var cols []ColumnInfo
	for rows.Next() {
		var colName, colType, comment string
		var pos uint64
		if err := rows.Scan(&colName, &colType, &comment, &pos); err != nil {
			return nil, fmt.Errorf("failed to scan column info: %w", err)
		}
		cols = append(cols, ColumnInfo{Name: colName, Type: colType, Comment: comment, Position: uint(pos)})
	}

	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating column info rows: %w", err)
	}
	rows.Close()

	return cols, nil
}

func generateDataCheckQuery(check *Check, dataset string, whereClause string, logger *slog.Logger) (string, error) {
	var sqlQuery string

	// handle raw_query first
	if check.ID == CheckTypeRawQuery {
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
		logger.Warn("Check did not match known check patterns. Assuming it's a direct SQL boolean expression",
			"check_id", check.ID)
		checkExpression = check.ID
	}

	sqlQuery = fmt.Sprintf("select %s from %s", checkExpression, dataset)
	if whereClause != "" {
		sqlQuery = fmt.Sprintf("%s where %s", sqlQuery, whereClause)
	}

	return sqlQuery, nil
}

// isNumericCHType checks if a ClickHouse data type string represents a numeric type
// that supports standard aggregate functions like min, max, avg, stddev
func isNumericCHType(dataType string) bool {
	// Basic check, might need additional refinement
	dataType = strings.ToLower(dataType)
	return strings.HasPrefix(dataType, "int") ||
		strings.HasPrefix(dataType, "uint") ||
		strings.HasPrefix(dataType, "float") ||
		strings.HasPrefix(dataType, "decimal")
}

// isStringCHType checks if a ClickHouse data type is a string type
func isStringCHType(dataType string) bool {
	dataType = strings.ToLower(dataType)
	return strings.HasPrefix(dataType, "string") ||
		strings.HasPrefix(dataType, "fixedstring")
}

func startWithAnyOf(prefixes []string, s string) bool {
	for _, prefix := range prefixes {
		if strings.HasPrefix(s, strings.ToLower(prefix)) {
			return true
		}
	}
	return false
}
