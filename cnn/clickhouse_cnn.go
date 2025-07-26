package cnn

import (
	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/DataBridgeTech/dbqcore"
)

func NewClickhouseConnection(connectionCfg dbqcore.ConnectionConfig) (driver.Conn, error) {
	cnn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{connectionCfg.Host},
		Auth: clickhouse.Auth{
			Database: connectionCfg.Database,
			Username: connectionCfg.Username,
			Password: connectionCfg.Password,
		},
		MaxOpenConns: 32,
		MaxIdleConns: 32,
		//TLS: &tls.Config{
		//	InsecureSkipVerify: true,
		//},
	})
	return cnn, err
}
