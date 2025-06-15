package clickhouseclient

import (
	"1CLogPumpClickHouse/config"
	"context"
	"fmt"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"go.uber.org/zap"

	"1CLogPumpClickHouse/models"
	"1CLogPumpClickHouse/transform"
)

// Config — структура для подключения к ClickHouse
// Protocol: "native" или "http"
type Config struct {
	Address  string
	Username string
	Password string
	Database string
	Table    string
	Protocol string
}

type Client struct {
	conn   clickhouse.Conn
	Table  string
	Logger *zap.Logger
}

// New создает клиента ClickHouse
func New(cfg config.ClickHouseConfig, logger *zap.Logger) (*Client, error) {
	protocol := clickhouse.Native
	if cfg.Protocol == "http" {
		protocol = clickhouse.HTTP
	}

	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{cfg.Address},
		Auth: clickhouse.Auth{
			Database: cfg.Database,
			Username: cfg.Username,
			Password: cfg.Password,
		},
		DialTimeout: 5 * time.Second,
		Compression: &clickhouse.Compression{Method: clickhouse.CompressionLZ4},
		Protocol:    protocol,
	})
	if err != nil {
		return nil, fmt.Errorf("clickhouse open: %w", err)
	}
	return &Client{conn: conn, Table: cfg.Table, Logger: logger}, nil
}

// InsertTechLogBatch конвертирует LogEntry в TechLogRow через transform и отправляет в ClickHouse
func (c *Client) InsertTechLogBatch(ctx context.Context, entries []models.LogEntry) error {
	batch, err := c.conn.PrepareBatch(ctx,
		"INSERT INTO "+c.Table+" ("+
			"EventDate, EventTime, EventType, Duration, User, InfoBase, SessionID, ClientID, ConnectionID, ExceptionType, ErrorText, SQLText, Rows, RowsAffected, Context, ProcessName"+
			") VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)")
	if err != nil {
		c.Logger.Error("prepare batch", zap.Error(err))
		return fmt.Errorf("prepare batch: %w", err)
	}

	for _, entry := range entries {
		row, err := transform.TransformLogEntry(entry)
		if err != nil {
			c.Logger.Error("transform", zap.Error(err), zap.Any("entry", entry))
			return fmt.Errorf("transform: %w", err)
		}
		err = batch.Append(
			row.EventDate,
			row.EventTime,
			row.EventType,
			row.Duration,
			row.User,
			row.InfoBase,
			row.SessionID,
			row.ClientID,
			row.ConnectionID,
			row.ExceptionType,
			row.ErrorText,
			row.SQLText,
			row.Rows,
			row.RowsAffected,
			row.Context,
			row.ProcessName,
		)
		if err != nil {
			c.Logger.Error("append batch", zap.Error(err), zap.Any("row", row))
			return fmt.Errorf("append: %w", err)
		}
	}
	return batch.Send()
}

func (c *Client) Close() error {
	return c.conn.Close()
}
