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
	"fmt"
	"io"
	"log/slog"
	"strings"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/DataBridgeTech/dbqcore"
)

type ClickhouseDbqConnector struct {
	cnn    driver.Conn
	logger *slog.Logger
}

func NewClickhouseDbqConnector(cnn driver.Conn, logger *slog.Logger) dbqcore.DbqConnector {
	if logger == nil {
		logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}

	return &ClickhouseDbqConnector{
		cnn:    cnn,
		logger: logger,
	}
}

func (c *ClickhouseDbqConnector) Ping() (string, error) {
	serverVersion, err := c.cnn.ServerVersion()
	if err != nil {
		return "", err
	}

	return serverVersion.String(), nil
}

func (c *ClickhouseDbqConnector) ImportDatasets(filter string) ([]string, error) {
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
