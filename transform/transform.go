// Исправим ошибку "parsing time \"2025-05-26 25:05.805096\": hour out of range"
// В transform.TransformLogEntry или где формируется EventTime нужно ограничить часы максимум до 23

package transform

import (
	"1CLogPumpClickHouse/models"
	"fmt"
	"strconv"
	"strings"
	"time"
)

func TransformLogEntry(entry models.LogEntry) (models.TechLogRow, error) {
	// Вытаскиваем дату из имени файла: "25052607.log" → "2025-05-26"
	ts := entry.Timestamp
	if len(ts) < 6 {
		return models.TechLogRow{}, fmt.Errorf("invalid timestamp filename: %s", ts)
	}
	parsedDate := fmt.Sprintf("20%s-%s-%s", ts[0:2], ts[2:4], ts[4:6])

	// Корректируем LogTimestamp, чтобы не было ошибки "hour out of range"
	rawTime := strings.Split(entry.LogTimestamp, "-")[0] // например, "25:05.805096"
	parts := strings.SplitN(rawTime, ":", 2)
	if len(parts) != 2 {
		return models.TechLogRow{}, fmt.Errorf("invalid log timestamp: %s", entry.LogTimestamp)
	}
	hour := parts[0]
	minute := parts[1]

	// Приводим часы к диапазону 0–23
	hourInt, err := strconv.Atoi(hour)
	if err != nil {
		return models.TechLogRow{}, fmt.Errorf("invalid hour: %v", err)
	}
	if hourInt > 23 {
		hourInt = 23
	}
	fixedTime := fmt.Sprintf("%02d:%s", hourInt, minute)

	timeStr := fmt.Sprintf("%s %s", parsedDate, fixedTime)
	eventTime, err := time.Parse("2006-01-02 15:04.999999", timeStr)
	if err != nil {
		return models.TechLogRow{}, fmt.Errorf("failed to parse event time: %v", err)
	}

	return models.TechLogRow{
		EventDate:     parsedDate,
		EventTime:     eventTime.Format("2006-01-02 15:04:05.999999"),
		EventType:     entry.EventType,
		Duration:      0,
		User:          entry.User,
		InfoBase:      entry.Database,
		SessionID:     uint32(entry.SessionID),
		ClientID:      entry.ClientID,
		ConnectionID:  entry.ConnectID,
		ExceptionType: nil,
		ErrorText:     nil,
		SQLText:       &entry.SQL,
		Rows:          &entry.Rows,
		RowsAffected:  &entry.RowsAffected,
		Context:       &entry.Context,
		ProcessName:   entry.ProcessName,
	}, nil
}
