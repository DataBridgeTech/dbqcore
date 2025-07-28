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
	"github.com/DataBridgeTech/dbqcore"
	"io"
	"log/slog"
	"strings"
)

type PostgresqlDbqDataProfiler struct {
	db     *sql.DB
	logger *slog.Logger
}

func NewPostgresqlDbqDataProfiler(db *sql.DB, logger *slog.Logger) dbqcore.DbqDataProfiler {
	if logger == nil {
		logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}

	return &PostgresqlDbqDataProfiler{
		db:     db,
		logger: logger,
	}
}

func (p *PostgresqlDbqDataProfiler) ProfileDataset(ctx context.Context, dataset string, sample bool, maxConcurrent int, collectErrors bool) (*dbqcore.TableMetrics, error) {
	baseProfiler := NewBaseProfiler(p, p.logger)
	return baseProfiler.ProfileDataset(ctx, dataset, sample, maxConcurrent, collectErrors)
}

func (p *PostgresqlDbqDataProfiler) GetColumns(ctx context.Context, schemaName string, tableName string) ([]*dbqcore.ColumnInfo, error) {
	query := `
		SELECT c.column_name, c.data_type, d.description, c.ordinal_position
		FROM information_schema.columns c
		LEFT JOIN pg_catalog.pg_description d
		ON d.objoid = (SELECT oid FROM pg_catalog.pg_class WHERE relname = c.table_name)
		AND d.objsubid = c.ordinal_position
		WHERE c.table_schema = $1 AND c.table_name = $2
		ORDER BY c.ordinal_position
	`

	rows, err := p.db.QueryContext(ctx, query, schemaName, tableName)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch columns info for %s.%s: %w", schemaName, tableName, err)
	}

	defer func() {
		if err := rows.Close(); err != nil {
			p.logger.Warn("failed to close rows", "error", err)
		}
	}()

	var cols []*dbqcore.ColumnInfo
	for rows.Next() {
		var colName, colType string
		var comment sql.NullString
		var pos uint
		if err := rows.Scan(&colName, &colType, &comment, &pos); err != nil {
			return nil, fmt.Errorf("failed to scan column info: %w", err)
		}
		cols = append(cols, &dbqcore.ColumnInfo{Name: colName, Type: colType, Comment: comment.String, Position: pos})
	}

	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating column info rows: %w", err)
	}

	return cols, nil
}

func (p *PostgresqlDbqDataProfiler) GetTotalRows(ctx context.Context, dataset string) (uint64, error) {
	var totalRows uint64
	err := p.db.QueryRowContext(ctx, fmt.Sprintf("SELECT count(*) FROM %s", dataset)).Scan(&totalRows)
	if err != nil {
		return 0, fmt.Errorf("failed to get total row count for %s: %w", dataset, err)
	}
	return totalRows, nil
}

func (p *PostgresqlDbqDataProfiler) GetNullCount(ctx context.Context, dataset string, column *dbqcore.ColumnInfo) (uint64, error) {
	nullQuery := fmt.Sprintf(`SELECT count(*) FROM %s WHERE "%s" IS NULL`, dataset, column.Name)

	rows, err := p.db.QueryContext(ctx, nullQuery)
	if err != nil {
		p.logger.Warn("failed to get NULL count", "error", err.Error(), "col_name", column.Name)
		return 0, err
	}

	defer func() {
		if err := rows.Close(); err != nil {
			p.logger.Warn("failed to close rows", "error", err)
		}
	}()

	if rows.Next() {
		var nullCount uint64
		if err = rows.Scan(&nullCount); err != nil {
			p.logger.Warn("failed to scan nulls count", "error", err.Error(), "col_name", column.Name)
			return 0, err
		} else {
			return nullCount, nil
		}
	}
	return 0, rows.Err()
}

func (p *PostgresqlDbqDataProfiler) GetBlankCount(ctx context.Context, dataset string, column *dbqcore.ColumnInfo) (int64, error) {
	blankQuery := fmt.Sprintf(`SELECT count(*) FROM %s WHERE "%s" = ''`, dataset, column.Name)

	rows, err := p.db.QueryContext(ctx, blankQuery)
	if err != nil {
		p.logger.Warn("failed to get blank count", "error", err.Error(), "col_name", column.Name)
		return 0, err
	}

	defer func() {
		if err := rows.Close(); err != nil {
			p.logger.Warn("failed to close rows", "error", err)
		}
	}()

	if rows.Next() {
		var blankCount uint64
		if err = rows.Scan(&blankCount); err != nil {
			p.logger.Warn("failed to scan blank count", "error", err.Error(), "col_name", column.Name)
			return 0, err
		} else {
			return int64(blankCount), nil
		}
	}
	return 0, rows.Err()
}

func (p *PostgresqlDbqDataProfiler) GetNumericStats(ctx context.Context, dataset string, column *dbqcore.ColumnInfo) (*dbqcore.NumericStats, error) {
	numericQuery := fmt.Sprintf(`SELECT min("%s"), max("%s"), avg("%s"), stddev_pop("%s") FROM %s`,
		column.Name, column.Name, column.Name, column.Name, dataset)

	rows, err := p.db.QueryContext(ctx, numericQuery)
	if err != nil {
		p.logger.Warn("failed to get numeric aggregates", "error", err.Error(), "col_name", column.Name)
		return nil, err
	}

	defer func() {
		if err := rows.Close(); err != nil {
			p.logger.Warn("failed to close rows", "error", err)
		}
	}()

	if rows.Next() {
		var minValue, maxValue, avgValue, stddevValue sql.NullFloat64
		if err = rows.Scan(&minValue, &maxValue, &avgValue, &stddevValue); err != nil {
			p.logger.Warn("failed to scan numeric aggregates", "error", err.Error(), "col_name", column.Name)
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

func (p *PostgresqlDbqDataProfiler) GetMostFrequentValue(ctx context.Context, dataset string, column *dbqcore.ColumnInfo) (*string, error) {
	mfvQuery := fmt.Sprintf(`
				SELECT CAST(mode() WITHIN GROUP (ORDER BY "%s") AS TEXT) AS mfv
				FROM %s`, column.Name, dataset)

	rows, err := p.db.QueryContext(ctx, mfvQuery)
	if err != nil {
		p.logger.Warn("failed to get most frequent value", "error", err.Error(), "col_name", column.Name)
		return nil, err
	}

	defer func() {
		if err := rows.Close(); err != nil {
			p.logger.Warn("failed to close rows", "error", err)
		}
	}()

	if rows.Next() {
		var mfv sql.NullString
		if err := rows.Scan(&mfv); err != nil {
			p.logger.Warn("failed to scan most frequent value", "error", err.Error(), "col_name", column.Name)
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

func (p *PostgresqlDbqDataProfiler) GetSampleData(ctx context.Context, dataset string) ([]map[string]interface{}, error) {
	sampleQuery := fmt.Sprintf("SELECT * FROM %s ORDER BY random() LIMIT 100", dataset)

	rows, err := p.db.QueryContext(ctx, sampleQuery)
	if err != nil {
		p.logger.Warn("failed to sample data",
			"dataset", dataset,
			"error", err.Error())
		return nil, nil
	}

	defer func() {
		if err := rows.Close(); err != nil {
			p.logger.Warn("failed to close rows", "error", err)
		}
	}()

	cols, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	var allRows []map[string]interface{}
	for rows.Next() {
		columns := make([]interface{}, len(cols))
		columnPointers := make([]interface{}, len(cols))
		for i := range columns {
			columnPointers[i] = &columns[i]
		}

		if err := rows.Scan(columnPointers...); err != nil {
			return nil, err
		}

		rowData := make(map[string]interface{})
		for i, colName := range cols {
			val := columnPointers[i].(*interface{})
			rowData[colName] = *val
		}
		allRows = append(allRows, rowData)
	}

	return allRows, nil
}

func (p *PostgresqlDbqDataProfiler) IsNumericType(dataType string) bool {
	dataType = strings.ToLower(dataType)
	return strings.Contains(dataType, "int") ||
		strings.Contains(dataType, "serial") ||
		strings.Contains(dataType, "numeric") ||
		strings.Contains(dataType, "decimal") ||
		strings.Contains(dataType, "real") ||
		strings.Contains(dataType, "double precision")
}

func (p *PostgresqlDbqDataProfiler) IsStringType(dataType string) bool {
	dataType = strings.ToLower(dataType)
	return strings.Contains(dataType, "char") ||
		strings.Contains(dataType, "text")
}
