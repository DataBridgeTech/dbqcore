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

func (c *ClickhouseDbqDataProfiler) ProfileDataset(dataset string, sample bool, maxConcurrent int) (*dbqcore.TableMetrics, error) {
	startTime := time.Now()
	ctx := context.Background()
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
	columnsToProcess, err := fetchColumns(c.cnn, ctx, databaseName, tableName)
	if err != nil {
		return metrics, err
	}

	if len(columnsToProcess) == 0 {
		c.logger.Warn("no columns found for table", dataset, ", returning basic info")
		metrics.ProfilingDurationMs = time.Since(startTime).Milliseconds()
		return metrics, nil
	}

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

		colMetrics := &dbqcore.ColumnMetrics{
			ColumnName:     col.Name,
			DataType:       col.Type,
			ColumnComment:  col.Comment,
			ColumnPosition: col.Position,
		}

		taskIdPrefix := fmt.Sprintf("task:%s:", col.Name)

		// Null Count (all types)
		enqueueColumnTask(taskPool, &colWg, taskIdPrefix+"null_count", func() error {
			nullQuery := fmt.Sprintf("select count() as null_count from %s where isNull(%s)", dataset, col.Name)
			rows, err := c.cnn.Query(ctx, nullQuery)
			defer rows.Close()

			if err != nil {
				c.logger.Warn("failed to get NULL count",
					"error", err.Error(),
					"col_name", col.Name,
					"col_type", col.Type)
			} else {
				for rows.Next() {
					var nullCount uint64
					if err = rows.Scan(&nullCount); err != nil {
						c.logger.Warn("failed to scan nulls count",
							"error", err.Error(),
							"col_name", col.Name,
							"col_type", col.Type)
					} else {
						colMetrics.NullCount = nullCount
					}
				}
			}

			return err
		})

		// Blank Count (String types only)
		if isStringCHType(col.Type) {
			enqueueColumnTask(taskPool, &colWg, taskIdPrefix+"blank_count", func() error {
				blankQuery := fmt.Sprintf("select count() as blank_count from %s where empty(%s)", dataset, col.Name)

				var blankCount uint64
				rows, err := c.cnn.Query(ctx, blankQuery)
				defer rows.Close()

				if err != nil {
					c.logger.Warn("failed to get blank count for column",
						"error", err.Error(),
						"col_name", col.Name,
						"col_type", col.Type)
					colMetrics.BlankCount = nil
				} else {
					for rows.Next() {
						if err = rows.Scan(&blankCount); err != nil {
							c.logger.Warn("failed to scan blank count",
								"error", err.Error(),
								"col_name", col.Name,
								"col_type", col.Type)
						} else {
							val := int64(blankCount)
							colMetrics.BlankCount = &val
						}
					}
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
                    min(%s) as min_value,
                    max(%s) as max_value,
                    avg(%s) as avg_value,
                    stddevPop(%s) as stddev_value
                from %s`, col.Name, col.Name, col.Name, col.Name, dataset)

				var minValue sql.NullFloat64
				var maxValue sql.NullFloat64
				var avgValue sql.NullFloat64
				var stddevValue sql.NullFloat64

				rows, err := c.cnn.Query(ctx, numericQuery)
				defer rows.Close()

				if err != nil {
					c.logger.Warn("failed to get numeric aggregates for column",
						"error", err.Error(),
						"col_name", col.Name,
						"col_type", col.Type)
				} else {
					for rows.Next() {
						err = rows.Scan(&minValue, &maxValue, &avgValue, &stddevValue)
						if err != nil {
							c.logger.Warn("failed to scan numeric aggregates",
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
			mfvQuery := fmt.Sprintf("select cast(arrayElement(topK(1)('%s'), 1), 'Nullable(String)') as mfv from %s", col.Name, dataset)
			rows, err := c.cnn.Query(ctx, mfvQuery)
			if err != nil {
				c.logger.Error("failed to get most frequent value for column",
					"error", err.Error(),
					"col_name", col.Name,
					"col_type", col.Type,
					"raw_query", mfvQuery)
				colMetrics.MostFrequentValue = nil
			}
			defer rows.Close()

			for rows.Next() {
				var mfv sql.NullString
				if err := rows.Scan(&mfv); err != nil {
					c.logger.Error("failed to scan most frequent value",
						"error", err.Error(),
						"col_name", col.Name,
						"col_type", col.Type)
					return err
				} else {
					if mfv.Valid {
						colMetrics.MostFrequentValue = &mfv.String
					} else {
						colMetrics.MostFrequentValue = nil
					}
				}
			}

			return err
		})

		go func() {
			colWg.Wait()
			elapsed := time.Since(colStartTime).Milliseconds()

			metricsLock.Lock()
			defer metricsLock.Unlock()

			colMetrics.ProfilingDurationMs = elapsed
			metrics.ColumnsMetrics[col.Name] = colMetrics
			c.logger.Debug("finished processing column",
				"col_name", col.Name,
				"proc_duration_ms", elapsed)
		}()
	}

	// todo: add timeout & cancellation
	taskPool.Join()

	metrics.ProfilingDurationMs = time.Since(startTime).Milliseconds()
	metrics.DbqErrors = taskPool.Errors()

	c.logger.Debug("finished data profiling for table",
		"dataset", dataset,
		"profile_duration_ms", metrics.ProfilingDurationMs)

	return metrics, nil
}

func fetchColumns(cnn driver.Conn, ctx context.Context, databaseName string, tableName string) ([]dbqcore.ColumnInfo, error) {
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

func enqueueColumnTask(taskPool *dbqcore.TaskPool, subsetWg *sync.WaitGroup, taskId string, task func() error) {
	subsetWg.Add(1)
	taskPool.Enqueue(taskId, func() error {
		defer subsetWg.Done()
		return task()
	})
}
