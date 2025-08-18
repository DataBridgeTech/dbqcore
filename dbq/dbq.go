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

package dbq

import (
	"fmt"
	"log/slog"

	"github.com/DataBridgeTech/dbqcore"
	"github.com/DataBridgeTech/dbqcore/adapters"
	"github.com/DataBridgeTech/dbqcore/cnn"
	"github.com/DataBridgeTech/dbqcore/connectors"
	"github.com/DataBridgeTech/dbqcore/profilers"
)

const (
	Version = "v0.5.0"
)

func GetDbqCoreLibVersion() string {
	return Version
}

func NewDbqConnector(dataSource *dbqcore.DataSource, poolSize int, logger *slog.Logger) (dbqcore.DbqConnector, error) {
	switch dataSource.Type {
	case dbqcore.DataSourceTypeClickhouse:
		connection, err := cnn.NewClickhouseConnection(dataSource.Configuration, poolSize)
		if err != nil {
			return nil, fmt.Errorf("failed to create clickhouse connection: %w", err)
		}
		return connectors.NewClickhouseDbqConnector(connection, logger), nil
	case dbqcore.DataSourceTypePostgresql:
		connection, err := cnn.NewPostgresqlConnection(dataSource.Configuration, poolSize)
		if err != nil {
			return nil, fmt.Errorf("failed to create postgresql connection: %w", err)
		}
		return connectors.NewPostgresqlDbqConnector(connection, logger), nil
	case dbqcore.DataSourceTypeMysql:
		connection, err := cnn.NewMysqlConnection(dataSource.Configuration, poolSize)
		if err != nil {
			return nil, fmt.Errorf("failed to create mysql connection: %w", err)
		}
		return connectors.NewMysqlDbqConnector(connection, logger), nil
	default:
		return nil, fmt.Errorf("unsupported data source type: %s", dataSource.Type)
	}
}

func NewDbqProfiler(dataSource *dbqcore.DataSource, poolSize int, logger *slog.Logger) (dbqcore.DbqDataProfiler, error) {
	switch dataSource.Type {
	case dbqcore.DataSourceTypeClickhouse:
		connection, err := cnn.NewClickhouseConnection(dataSource.Configuration, poolSize)
		if err != nil {
			return nil, fmt.Errorf("failed to create clickhouse connection: %w", err)
		}
		return profilers.NewClickhouseDbqDataProfiler(connection, logger), nil
	case dbqcore.DataSourceTypePostgresql:
		connection, err := cnn.NewPostgresqlConnection(dataSource.Configuration, poolSize)
		if err != nil {
			return nil, fmt.Errorf("failed to create postgresql connection: %w", err)
		}
		return profilers.NewPostgresqlDbqDataProfiler(connection, logger), nil
	case dbqcore.DataSourceTypeMysql:
		connection, err := cnn.NewMysqlConnection(dataSource.Configuration, poolSize)
		if err != nil {
			return nil, fmt.Errorf("failed to create mysql connection: %w", err)
		}
		return profilers.NewMysqlDbqDataProfiler(connection, logger), nil
	default:
		return nil, fmt.Errorf("unsupported data source type: %s", dataSource.Type)
	}
}

func NewDbqAdapter(dataSource *dbqcore.DataSource, poolSize int, logger *slog.Logger) (dbqcore.DbqDataSourceAdapter, error) {
	switch dataSource.Type {
	case dbqcore.DataSourceTypeClickhouse:
		connection, err := cnn.NewClickhouseConnection(dataSource.Configuration, poolSize)
		if err != nil {
			return nil, fmt.Errorf("failed to create clickhouse connection: %w", err)
		}
		return adapters.NewClickhouseDbqDataSourceAdapter(connection, logger), nil
	case dbqcore.DataSourceTypePostgresql:
		connection, err := cnn.NewPostgresqlConnection(dataSource.Configuration, poolSize)
		if err != nil {
			return nil, fmt.Errorf("failed to create postgresql connection: %w", err)
		}
		return adapters.NewPostgresqlDbqDataSourceAdapter(connection, logger), nil
	case dbqcore.DataSourceTypeMysql:
		connection, err := cnn.NewMysqlConnection(dataSource.Configuration, poolSize)
		if err != nil {
			return nil, fmt.Errorf("failed to create mysql connection: %w", err)
		}
		return adapters.NewMysqlDbqDataSourceAdapter(connection, logger), nil
	default:
		return nil, fmt.Errorf("unsupported data source type: %s", dataSource.Type)
	}
}
