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
		return models.TechLogRow{}, fmt.Errorf("недопустимый timestamp: %s", ts)
	}
	parsedDate := fmt.Sprintf("20%s-%s-%s", ts[0:2], ts[2:4], ts[4:6])
	parsedHour, err := strconv.Atoi(ts[6:8])
	if err != nil {
		return models.TechLogRow{}, fmt.Errorf("недопустимый час в timestamp: %q", parsedHour)
	}

	rawLogTimestamp := strings.TrimPrefix(entry.LogTimestamp, "\uFEFF")
	partsRaw := strings.Split(rawLogTimestamp, "-")

	rawTime := partsRaw[0]
	parts := strings.SplitN(rawTime, ":", 2)

	if len(parts) != 2 {
		return models.TechLogRow{}, fmt.Errorf("недопустимый log timestamp: %s", entry.LogTimestamp)
	}

	eventTimeStr := fmt.Sprintf(
		"%s %02d:%s",
		parsedDate, // "2025-06-16"
		parsedHour, // 02
		rawTime,    // "00:03.310025"
	)

	eventTime, err := time.Parse(
		"2006-01-02 15:04:05.999999",
		eventTimeStr,
	)
	if err != nil {
		return models.TechLogRow{}, fmt.Errorf("failed to parse event time: %v", err)
	}
	parts2 := partsRaw

	var duration uint32 = 0
	if len(parts2) > 1 {
		if val, err := strconv.ParseUint(parts2[1], 10, 32); err == nil {
			duration = uint32(val)
		}
	}
	return models.TechLogRow{
		EventDate:     parsedDate,
		EventTime:     eventTime.Format("2006-01-02 15:04:05.999999"),
		EventType:     entry.Component,
		Duration:      duration, // теперь устанавливается из лога
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
