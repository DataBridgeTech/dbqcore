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

package profilers

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"log/slog"
	"reflect"
	"strings"
	"sync"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/DataBridgeTech/dbqcore"
)

type ClickhouseDbqDataProfiler struct {
	cnn    driver.Conn
	logger *slog.Logger
}

func NewClickhouseDbqDataProfiler(cnn driver.Conn, logger *slog.Logger) dbqcore.DbqDataProfiler {
	if logger == nil {
		logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}

	return &ClickhouseDbqDataProfiler{
		cnn:    cnn,
		logger: logger,
	}
}

// ProfileDataset analyzes a given dataset (table) and returns detailed metrics about its columns.
func (c *ClickhouseDbqDataProfiler) ProfileDataset(ctx context.Context, dataset string, sample bool, maxConcurrent int) (*dbqcore.TableMetrics, error) {
	startTime := time.Now()
	taskPool := dbqcore.NewTaskPool(maxConcurrent, c.logger)

	var databaseName, tableName string
	parts := strings.SplitN(dataset, ".", 2)
	if len(parts) == 2 {
		databaseName = strings.TrimSpace(parts[0])
		tableName = strings.TrimSpace(parts[1])
	}

	var metricsLock sync.Mutex
	metrics := &dbqcore.TableMetrics{
		ProfiledAt:     time.Now().Unix(),
		TableName:      tableName,
		DatabaseName:   databaseName,
		ColumnsMetrics: make(map[string]*dbqcore.ColumnMetrics),
	}

	// Total Row Count
	err := c.cnn.QueryRow(ctx, fmt.Sprintf("select count() as row_count from %s", dataset)).Scan(&metrics.TotalRows)
	if err != nil {
		return nil, fmt.Errorf("failed to get total row count for %s: %w", dataset, err)
	}

	// Get Column Information (Name and Type)
	columnsToProcess, err := fetchColumns(ctx, c.cnn, c.logger, databaseName, tableName)
	if err != nil {
		return metrics, err
	}

	if len(columnsToProcess) == 0 {
		c.logger.Warn("no columns found for table", "dataset", dataset, ", returning basic info")
		metrics.ProfilingDurationMs = time.Since(startTime).Milliseconds()
		return metrics, nil
	}

	c.logger.Debug(fmt.Sprintf("found %d columns to process", len(columnsToProcess)))

	// sample data if enabled
	if sample {
		taskPool.Enqueue("task:sampling", func() error {
			sampleQuery := fmt.Sprintf("select * from %s.%s order by rand() limit 100", databaseName, tableName)

			toCtx, cancel := context.WithTimeout(ctx, 60*time.Second)
			defer cancel()

			rows, err := c.cnn.Query(toCtx, sampleQuery)
			if err != nil {
				c.logger.Warn("failed to sample data",
					"dataset", tableName,
					"error", err.Error())
				return nil
			}

			defer func() {
				if closeErr := rows.Close(); closeErr != nil {
					c.logger.Warn("failed to close rows", "error", closeErr)
				}
			}()

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

			if err := rows.Err(); err != nil {
				c.logger.Warn("error iterating sample rows", "error", err)
				return err
			}

			metricsLock.Lock()
			metrics.RowsSample = allRows
			metricsLock.Unlock()

			return nil
		})
	}

	// Calculate Metrics per Column
	for _, col := range columnsToProcess {
		column := col
		var colWg sync.WaitGroup
		var colMetricsLock sync.Mutex
		colStartTime := time.Now()

		c.logger.Debug("start column processing",
			"col_name", column.Name,
			"col_type", column.Type)

		colMetrics := &dbqcore.ColumnMetrics{
			ColumnName:     column.Name,
			DataType:       column.Type,
			ColumnComment:  column.Comment,
			ColumnPosition: column.Position,
		}

		taskIdPrefix := fmt.Sprintf("task:%s:", column.Name)

		// Null Count (all types)
		enqueueColumnTask(taskPool, &colWg, taskIdPrefix+"null_count", func() error {
			nullQuery := fmt.Sprintf("select count() as null_count from %s where isNull(%s)", dataset, column.Name)

			rows, err := c.cnn.Query(ctx, nullQuery)
			if err != nil {
				c.logger.Warn("failed to get NULL count", "error", err.Error(), "col_name", column.Name)
				return err
			}

			defer func() {
				if err := rows.Close(); err != nil {
					c.logger.Warn("failed to close rows", "error", err)
				}
			}()

			if rows.Next() {
				var nullCount uint64
				if err = rows.Scan(&nullCount); err != nil {
					c.logger.Warn("failed to scan nulls count", "error", err.Error(), "col_name", column.Name)
				} else {
					colMetricsLock.Lock()
					colMetrics.NullCount = nullCount
					colMetricsLock.Unlock()
				}
			}

			return rows.Err()
		})

		// Blank Count (String types only)
		if isStringCHType(column.Type) {
			enqueueColumnTask(taskPool, &colWg, taskIdPrefix+"blank_count", func() error {
				blankQuery := fmt.Sprintf("select count() as blank_count from %s where empty(%s)", dataset, column.Name)

				rows, err := c.cnn.Query(ctx, blankQuery)
				if err != nil {
					c.logger.Warn("failed to get blank count", "error", err.Error(), "col_name", column.Name)
					return err
				}

				defer func() {
					if err := rows.Close(); err != nil {
						c.logger.Warn("failed to close rows", "error", err)
					}
				}()

				if rows.Next() {
					var blankCount uint64
					if err = rows.Scan(&blankCount); err != nil {
						c.logger.Warn("failed to scan blank count", "error", err.Error(), "col_name", column.Name)
					} else {
						val := int64(blankCount)
						colMetricsLock.Lock()
						colMetrics.BlankCount = &val
						colMetricsLock.Unlock()
					}
				}

				return rows.Err()
			})
		}

		// Numeric Metrics (Numeric types only)
		if isNumericCHType(column.Type) {
			enqueueColumnTask(taskPool, &colWg, taskIdPrefix+"num_stats", func() error {
				numericQuery := fmt.Sprintf(`
                select min(%s), max(%s), avg(%s), stddevPop(%s) from %s`,
					column.Name, column.Name, column.Name, column.Name, dataset)

				rows, err := c.cnn.Query(ctx, numericQuery)
				if err != nil {
					c.logger.Warn("failed to get numeric aggregates", "error", err.Error(), "col_name", column.Name)
					return err
				}

				defer func() {
					if err := rows.Close(); err != nil {
						c.logger.Warn("failed to close rows", "error", err)
					}
				}()

				if rows.Next() {
					var minValue, maxValue, avgValue, stddevValue sql.NullFloat64
					if err = rows.Scan(&minValue, &maxValue, &avgValue, &stddevValue); err != nil {
						c.logger.Warn("failed to scan numeric aggregates", "error", err.Error(), "col_name", column.Name)
					} else {
						colMetricsLock.Lock()
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
						colMetricsLock.Unlock()
					}
				}

				return rows.Err()
			})
		}

		// Most Frequent Value
		enqueueColumnTask(taskPool, &colWg, taskIdPrefix+"mfv", func() error {
			mfvQuery := fmt.Sprintf("select cast(arrayElement(topK(1)(%s), 1), 'Nullable(String)') as mfv from %s", column.Name, dataset)

			rows, err := c.cnn.Query(ctx, mfvQuery)
			if err != nil {
				c.logger.Warn("failed to get most frequent value", "error", err.Error(), "col_name", column.Name)
				return err
			}

			defer func() {
				if err := rows.Close(); err != nil {
					c.logger.Warn("failed to close rows", "error", err)
				}
			}()

			if rows.Next() {
				var mfv sql.NullString
				if err := rows.Scan(&mfv); err != nil {
					c.logger.Warn("failed to scan most frequent value", "error", err.Error(), "col_name", column.Name)
				} else {
					colMetricsLock.Lock()
					if mfv.Valid {
						colMetrics.MostFrequentValue = &mfv.String
					} else {
						colMetrics.MostFrequentValue = nil
					}
					colMetricsLock.Unlock()
				}
			}

			return rows.Err()
		})

		go func() {
			// Wait for all metric-gathering tasks for the current column to complete
			colWg.Wait()
			colMetrics.ProfilingDurationMs = time.Since(colStartTime).Milliseconds()

			metricsLock.Lock()
			metrics.ColumnsMetrics[column.Name] = colMetrics
			metricsLock.Unlock()

			c.logger.Debug("finished processing column",
				"col_name", column.Name,
				"proc_duration_ms", colMetrics.ProfilingDurationMs)
		}()
	}

	// Wait for all tasks (sampling and all column metrics) to finish.
	taskPool.Join()

	metrics.ProfilingDurationMs = time.Since(startTime).Milliseconds()
	metrics.DbqErrors = taskPool.Errors()

	c.logger.Debug("finished data profiling for table",
		"dataset", dataset,
		"profile_duration_ms", metrics.ProfilingDurationMs)

	return metrics, nil
}

func fetchColumns(ctx context.Context, cnn driver.Conn, logger *slog.Logger, databaseName string, tableName string) ([]dbqcore.ColumnInfo, error) {
	columnQuery := `
        SELECT name, type, comment, position
        FROM system.columns
        WHERE database = ? AND table = ?
        ORDER BY position`

	rows, err := cnn.Query(ctx, columnQuery, databaseName, tableName)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch columns info for %s.%s: %w", databaseName, tableName, err)
	}

	defer func() {
		if err := rows.Close(); err != nil {
			logger.Warn("failed to close rows", "error", err)
		}
	}()

	var cols []dbqcore.ColumnInfo
	for rows.Next() {
		var colName, colType, comment string
		var pos uint64
		if err := rows.Scan(&colName, &colType, &comment, &pos); err != nil {
			return nil, fmt.Errorf("failed to scan column info: %w", err)
		}
		cols = append(cols, dbqcore.ColumnInfo{Name: colName, Type: colType, Comment: comment, Position: uint(pos)})
	}

	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating column info rows: %w", err)
	}
	rows.Close()

	return cols, nil
}

// isNumericCHType checks if a ClickHouse data type string represents a numeric type.
func isNumericCHType(dataType string) bool {
	dataType = strings.ToLower(dataType)
	return strings.HasPrefix(dataType, "int") ||
		strings.HasPrefix(dataType, "uint") ||
		strings.HasPrefix(dataType, "float") ||
		strings.HasPrefix(dataType, "decimal")
}

// isStringCHType checks if a ClickHouse data type is a string type.
func isStringCHType(dataType string) bool {
	dataType = strings.ToLower(dataType)
	return strings.HasPrefix(dataType, "string") ||
		strings.HasPrefix(dataType, "fixedstring")
}

func enqueueColumnTask(taskPool *dbqcore.TaskPool, subsetWg *sync.WaitGroup, taskId string, task func() error) {
	subsetWg.Add(1)
	taskPool.Enqueue(taskId, func() error {
		defer subsetWg.Done()
		return task()
	})
}
