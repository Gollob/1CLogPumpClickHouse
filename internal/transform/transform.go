package transform

import (
	"1CLogPumpClickHouse/internal/models"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"
)

var timeRegexp = regexp.MustCompile(`\d{2}:\d{2}\.\d{1,6}`)

func TransformLogEntry(entry models.LogEntry) (models.TechLogRow, error) {
	// Вытаскиваем дату из имени файла: "25052607.log" → "2025-05-26"
	ts := entry.Timestamp
	if len(ts) < 6 {
		return models.TechLogRow{}, fmt.Errorf("недопустимый timestamp: %s", ts)
	}
	parsedDate := fmt.Sprintf("20%s-%s-%s", ts[0:2], ts[2:4], ts[4:6])
	parsedHour, err := strconv.Atoi(ts[6:8])
	if err != nil {
		return models.TechLogRow{}, fmt.Errorf("недопустимый час в timestamp: %q", ts[6:8])
	}

	// Извлечение времени события из сырых данных
	raw := entry.LogTimestamp
	// Удаляем возможный BOM
	raw = strings.TrimPrefix(raw, "\uFEFF")
	// Находим первое совпадение по шаблону времени
	match := timeRegexp.FindString(raw)
	if match == "" {
		return models.TechLogRow{}, fmt.Errorf("недопустимый log timestamp: %s", entry.LogTimestamp)
	}

	eventTimeStr := fmt.Sprintf("%s %02d:%s", parsedDate, parsedHour, match)
	// Пытаемся распарсить с дробной частью
	eventTime, err := time.Parse("2006-01-02 15:04:05.000000", eventTimeStr)
	if err != nil {
		// Без дробной части
		eventTime, err = time.Parse("2006-01-02 15:04:05", eventTimeStr)
		if err != nil {
			return models.TechLogRow{}, fmt.Errorf("failed to parse event time: %v", err)
		}
	}

	// Количество миллисекунд или произвольного значения после дефиса
	var duration uint32
	if parts := strings.SplitN(raw, "-", 2); len(parts) > 1 {
		if val, err := strconv.ParseUint(parts[1], 10, 32); err == nil {
			duration = uint32(val)
		}
	}

	return models.TechLogRow{
		EventDate:     parsedDate,
		EventTime:     eventTime.Format("2006-01-02 15:04:05.999999"),
		EventType:     entry.Component,
		Duration:      duration,
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
