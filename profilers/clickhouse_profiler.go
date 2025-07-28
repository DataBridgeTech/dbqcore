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

func (c *ClickhouseDbqDataProfiler) ProfileDataset(ctx context.Context, dataset string, sample bool, maxConcurrent int, collectErrors bool) (*dbqcore.TableMetrics, error) {
	baseProfiler := NewBaseProfiler(c, c.logger)
	return baseProfiler.ProfileDataset(ctx, dataset, sample, maxConcurrent, collectErrors)
}

func (c *ClickhouseDbqDataProfiler) GetColumns(ctx context.Context, databaseName string, tableName string) ([]*dbqcore.ColumnInfo, error) {
	columnQuery := `
        SELECT name, type, comment, position
        FROM system.columns
        WHERE database = ? AND table = ?
        ORDER BY position`

	rows, err := c.cnn.Query(ctx, columnQuery, databaseName, tableName)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch columns info for %s.%s: %w", databaseName, tableName, err)
	}

	defer func() {
		if err := rows.Close(); err != nil {
			c.logger.Warn("failed to close rows", "error", err)
		}
	}()

	var cols []*dbqcore.ColumnInfo
	for rows.Next() {
		var colName, colType, comment string
		var pos uint64
		if err := rows.Scan(&colName, &colType, &comment, &pos); err != nil {
			return nil, fmt.Errorf("failed to scan column info: %w", err)
		}
		cols = append(cols, &dbqcore.ColumnInfo{Name: colName, Type: colType, Comment: comment, Position: uint(pos)})
	}

	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating column info rows: %w", err)
	}

	return cols, nil
}

func (c *ClickhouseDbqDataProfiler) GetTotalRows(ctx context.Context, dataset string) (uint64, error) {
	var totalRows uint64
	err := c.cnn.QueryRow(ctx, fmt.Sprintf("select count() as row_count from %s", dataset)).Scan(&totalRows)
	if err != nil {
		return 0, fmt.Errorf("failed to get total row count for %s: %w", dataset, err)
	}
	return totalRows, nil
}

func (c *ClickhouseDbqDataProfiler) GetNullCount(ctx context.Context, dataset string, column *dbqcore.ColumnInfo) (uint64, error) {
	nullQuery := fmt.Sprintf("select count() as null_count from %s where isNull(%s)", dataset, column.Name)

	rows, err := c.cnn.Query(ctx, nullQuery)
	if err != nil {
		c.logger.Warn("failed to get NULL count", "error", err.Error(), "col_name", column.Name)
		return 0, err
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
			return 0, err
		} else {
			return nullCount, nil
		}
	}

	return 0, rows.Err()
}

func (c *ClickhouseDbqDataProfiler) GetBlankCount(ctx context.Context, dataset string, column *dbqcore.ColumnInfo) (int64, error) {
	blankQuery := fmt.Sprintf("select count() as blank_count from %s where empty(%s)", dataset, column.Name)

	rows, err := c.cnn.Query(ctx, blankQuery)
	if err != nil {
		c.logger.Warn("failed to get blank count", "error", err.Error(), "col_name", column.Name)
		return 0, err
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
			return 0, err
		} else {
			return int64(blankCount), nil
		}
	}

	return 0, rows.Err()
}

func (c *ClickhouseDbqDataProfiler) GetNumericStats(ctx context.Context, dataset string, column *dbqcore.ColumnInfo) (*dbqcore.NumericStats, error) {
	numericQuery := fmt.Sprintf(`select min(%s), max(%s), avg(%s), stddevPop(%s) from %s`,
		column.Name, column.Name, column.Name, column.Name, dataset)

	rows, err := c.cnn.Query(ctx, numericQuery)
	if err != nil {
		c.logger.Warn("failed to get numeric aggregates", "error", err.Error(), "col_name", column.Name)
		return nil, err
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
			return nil, err
		} else {
			stats := &dbqcore.NumericStats{}
			if minValue.Valid {
				stats.MinValue = &minValue.Float64
			}
			if maxValue.Valid {
				stats.MaxValue = &maxValue.Float64
			}
			if avgValue.Valid {
				stats.AvgValue = &avgValue.Float64
			}
			if stddevValue.Valid {
				stats.StddevValue = &stddevValue.Float64
			}
			return stats, nil
		}
	}

	return nil, rows.Err()
}

func (c *ClickhouseDbqDataProfiler) GetMostFrequentValue(ctx context.Context, dataset string, column *dbqcore.ColumnInfo) (*string, error) {
	mfvQuery := fmt.Sprintf("select cast(arrayElement(topK(1)(%s), 1), 'Nullable(String)') as mfv from %s", column.Name, dataset)

	rows, err := c.cnn.Query(ctx, mfvQuery)
	if err != nil {
		c.logger.Warn("failed to get most frequent value", "error", err.Error(), "col_name", column.Name)
		return nil, err
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
			return nil, err
		} else {
			if mfv.Valid {
				return &mfv.String, nil
			} else {
				return nil, nil
			}
		}
	}

	return nil, rows.Err()
}

func (c *ClickhouseDbqDataProfiler) GetSampleData(ctx context.Context, dataset string) ([]map[string]interface{}, error) {
	sampleQuery := fmt.Sprintf("select * from %s order by rand() limit 100", dataset)

	toCtx, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()

	rows, err := c.cnn.Query(toCtx, sampleQuery)
	if err != nil {
		c.logger.Warn("failed to sample data",
			"dataset", dataset,
			"error", err.Error())
		return nil, nil
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
		return nil, err
	}

	return allRows, nil
}

// IsNumericType checks if a ClickHouse data type string represents a numeric type.
func (c *ClickhouseDbqDataProfiler) IsNumericType(dataType string) bool {
	dataType = strings.ToLower(dataType)
	return strings.HasPrefix(dataType, "int") ||
		strings.HasPrefix(dataType, "uint") ||
		strings.HasPrefix(dataType, "float") ||
		strings.HasPrefix(dataType, "decimal") ||
		strings.HasPrefix(dataType, "nullable(int") ||
		strings.HasPrefix(dataType, "nullable(uint") ||
		strings.HasPrefix(dataType, "nullable(float") ||
		strings.HasPrefix(dataType, "nullable(decimal")
}

// IsStringType checks if a ClickHouse data type is a string type.
func (c *ClickhouseDbqDataProfiler) IsStringType(dataType string) bool {
	dataType = strings.ToLower(dataType)
	return strings.HasPrefix(dataType, "string") ||
		strings.HasPrefix(dataType, "fixedstring") ||
		strings.HasPrefix(dataType, "nullable(string") ||
		strings.HasPrefix(dataType, "nullable(fixedstring")
}
