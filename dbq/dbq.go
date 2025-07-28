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
	"github.com/DataBridgeTech/dbqcore/cnn"
	"github.com/DataBridgeTech/dbqcore/connectors"
	"github.com/DataBridgeTech/dbqcore/profilers"
	"github.com/DataBridgeTech/dbqcore/validators"
)

const (
	Version = "v0.3.0"
)

func GetDbqCoreLibVersion() string {
	return Version
}

func NewDbqConnector(dataSource *dbqcore.DataSource, logger *slog.Logger) (dbqcore.DbqConnector, error) {
	switch dataSource.Type {
	case dbqcore.DataSourceTypeClickhouse:
		connection, err := cnn.NewClickhouseConnection(dataSource.Configuration)
		if err != nil {
			return nil, fmt.Errorf("failed to create clickhouse connection: %w", err)
		}
		return connectors.NewClickhouseDbqConnector(connection, logger), nil
	case dbqcore.DataSourceTypePostgresql:
		connection, err := cnn.NewPostgresqlConnection(dataSource.Configuration)
		if err != nil {
			return nil, fmt.Errorf("failed to create postgresql connection: %w", err)
		}
		return connectors.NewPostgresqlDbqConnector(connection, logger), nil
	case dbqcore.DataSourceTypeMysql:
		connection, err := cnn.NewMysqlConnection(dataSource.Configuration)
		if err != nil {
			return nil, fmt.Errorf("failed to create mysql connection: %w", err)
		}
		return connectors.NewMysqlDbqConnector(connection, logger), nil
	default:
		return nil, fmt.Errorf("unsupported data source type: %s", dataSource.Type)
	}
}

func NewDbqProfiler(dataSource *dbqcore.DataSource, logger *slog.Logger) (dbqcore.DbqDataProfiler, error) {
	switch dataSource.Type {
	case dbqcore.DataSourceTypeClickhouse:
		connection, err := cnn.NewClickhouseConnection(dataSource.Configuration)
		if err != nil {
			return nil, fmt.Errorf("failed to create clickhouse connection: %w", err)
		}
		return profilers.NewClickhouseDbqDataProfiler(connection, logger), nil
	case dbqcore.DataSourceTypePostgresql:
		connection, err := cnn.NewPostgresqlConnection(dataSource.Configuration)
		if err != nil {
			return nil, fmt.Errorf("failed to create postgresql connection: %w", err)
		}
		return profilers.NewPostgresqlDbqDataProfiler(connection, logger), nil
	case dbqcore.DataSourceTypeMysql:
		connection, err := cnn.NewMysqlConnection(dataSource.Configuration)
		if err != nil {
			return nil, fmt.Errorf("failed to create mysql connection: %w", err)
		}
		return profilers.NewMysqlDbqDataProfiler(connection, logger), nil
	default:
		return nil, fmt.Errorf("unsupported data source type: %s", dataSource.Type)
	}
}

func NewDbqValidator(dataSource *dbqcore.DataSource, logger *slog.Logger) (dbqcore.DbqDataValidator, error) {
	switch dataSource.Type {
	case dbqcore.DataSourceTypeClickhouse:
		connection, err := cnn.NewClickhouseConnection(dataSource.Configuration)
		if err != nil {
			return nil, fmt.Errorf("failed to create clickhouse connection: %w", err)
		}
		return validators.NewClickhouseDbqDataValidator(connection, logger), nil
	case dbqcore.DataSourceTypePostgresql:
		connection, err := cnn.NewPostgresqlConnection(dataSource.Configuration)
		if err != nil {
			return nil, fmt.Errorf("failed to create postgresql connection: %w", err)
		}
		return validators.NewPostgresqlDbqDataValidator(connection, logger), nil
	case dbqcore.DataSourceTypeMysql:
		connection, err := cnn.NewMysqlConnection(dataSource.Configuration)
		if err != nil {
			return nil, fmt.Errorf("failed to create mysql connection: %w", err)
		}
		return validators.NewMysqlDbqDataValidator(connection, logger), nil
	default:
		return nil, fmt.Errorf("unsupported data source type: %s", dataSource.Type)
	}
}
