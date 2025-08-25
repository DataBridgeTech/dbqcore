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

package connectors

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"

	"github.com/DataBridgeTech/dbqcore"
)

type MysqlDbqConnector struct {
	db     *sql.DB
	logger *slog.Logger
}

func NewMysqlDbqConnector(db *sql.DB, logger *slog.Logger) dbqcore.DbqConnector {
	return &MysqlDbqConnector{db: db, logger: logger}
}

func (c *MysqlDbqConnector) Ping(ctx context.Context) (string, error) {
	err := c.db.PingContext(ctx)
	if err != nil {
		return "", err
	}
	return "OK", nil
}

func (c *MysqlDbqConnector) ImportDatasets(ctx context.Context, filter string) ([]string, error) {
	query := `
		select table_schema, table_name
		from information_schema.tables
		where table_schema not in ('mysql', 'information_schema', 'performance_schema', 'sys')
	`

	var args []interface{}
	if filter != "" {
		query += " and (table_schema like ? or table_name like ?)"
		args = append(args, fmt.Sprintf("%%%s%%", filter), fmt.Sprintf("%%%s%%", filter))
	}
	query += " order by table_schema, table_name"

	rows, err := c.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to query information_schema.tables: %w", err)
	}

	defer func() {
		if err := rows.Close(); err != nil {
			c.logger.Warn("failed to close rows", "error", err)
		}
	}()

	var datasets []string
	for rows.Next() {
		var schemaName, tableName string
		if err := rows.Scan(&schemaName, &tableName); err != nil {
			return nil, fmt.Errorf("failed to scan row: %w", err)
		}
		datasets = append(datasets, fmt.Sprintf("%s.%s", schemaName, tableName))
	}

	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("error occurred during row iteration: %w", err)
	}

	return datasets, nil
}
