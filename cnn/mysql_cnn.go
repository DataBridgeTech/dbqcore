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

package cnn

import (
	"database/sql"
	"fmt"

	"github.com/DataBridgeTech/dbqcore"
	_ "github.com/go-sql-driver/mysql"
)

func NewMysqlConnection(connectionCfg dbqcore.ConnectionConfig, poolSize int) (*sql.DB, error) {
	connStr := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s",
		connectionCfg.Username, connectionCfg.Password, connectionCfg.Host, connectionCfg.Port, connectionCfg.Database)
	db, err := sql.Open("mysql", connStr)
	if err != nil {
		return nil, err
	}

	db.SetMaxOpenConns(poolSize)
	db.SetMaxIdleConns(poolSize)

	return db, nil
}
