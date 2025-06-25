package clickhouseclient

import (
	"1CLogPumpClickHouse/internal/config"
	"1CLogPumpClickHouse/internal/models"
	"1CLogPumpClickHouse/internal/transform"
	"context"
	"fmt"
	"github.com/ClickHouse/clickhouse-go/v2"
	"go.uber.org/zap"
	"time"
)

// Config — структура для подключения к ClickHouse
// Protocol: "native" или "http"
type Config struct {
	Address      string
	Username     string
	Password     string
	Database     string
	DefaultTable string
	Protocol     string
	TableMap     map[string]string
}

type Client struct {
	conn         clickhouse.Conn
	DefaultTable string
	TableMap     map[string]string
	Logger       *zap.Logger
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
	return &Client{
		conn:         conn,
		DefaultTable: cfg.DefaultTable,
		TableMap:     cfg.TableMap,
		Logger:       logger,
	}, nil
}

// InsertTechLogBatch конвертирует LogEntry в TechLogRow через transform и отправляет в ClickHouse
func (c *Client) InsertTechLogBatch(ctx context.Context, entries []models.LogEntry) error {
	// Группируем записи по имени таблицы
	grouped := make(map[string][]models.LogEntry)
	for _, entry := range entries {
		tableName := c.DefaultTable
		if tbl, ok := c.TableMap[entry.Component]; ok {
			tableName = tbl
		}
		grouped[tableName] = append(grouped[tableName], entry)
	}

	// Отправляем отдельный батч для каждой таблицы
	for tableName, group := range grouped {
		// Используем отдельный контекст с таймаутом, чтобы отмена сервиса не прерывала операцию
		dbCtx, cancel := context.WithTimeout(context.Background(), 60*time.Second)

		batch, err := c.conn.PrepareBatch(dbCtx,
			"INSERT INTO "+tableName+" ("+
				"EventDate, EventTime, EventType, Duration, User, InfoBase, SessionID, "+
				"ClientID, ConnectionID, ExceptionType, ErrorText, SQLText, Rows, RowsAffected, Context, ProcessName"+
				") VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)")
		if err != nil {
			cancel()
			c.Logger.Error("prepare batch", zap.Error(err), zap.String("table", tableName))
			return fmt.Errorf("prepare batch: %w", err)
		}

		for _, entry := range group {
			row, err := transform.TransformLogEntry(entry)
			if err != nil {
				c.Logger.Warn("Некорректное время события, запись пропущена", zap.Error(err), zap.Any("entry", entry))
				continue // пропускаем эту запись, не останавливая весь цикл
			}
			if err := batch.Append(
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
			); err != nil {
				cancel()
				c.Logger.Error("append batch", zap.Error(err), zap.Any("row", row))
				return fmt.Errorf("append: %w", err)
			}
		}

		if err := batch.Send(); err != nil {
			cancel() // отменяем контекст при ошибке
			c.Logger.Error("send batch", zap.Error(err), zap.String("table", tableName))
			return fmt.Errorf("send batch: %w", err)
		}
		cancel()
	}
	return nil
}

// Close закрывает соединение с ClickHouse
func (c *Client) Close() error {
	return c.conn.Close()
}
