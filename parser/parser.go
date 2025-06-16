package parser

import (
	"strconv"
	"strings"
	"time"

	"1CLogPumpClickHouse/models"
)

// ParseLine парсит одну или несколько строк лога 1С в LogEntry.
// lines — это массив строк, который может содержать как одну, так и несколько связанных строк (multi-line SQL/Context).
// Возвращает полностью заполненную структуру LogEntry.
func ParseLine(lines []string) (models.LogEntry, error) {
	raw := strings.Join(lines, "\n")
	header, sql, context := ParseLogRecord(raw)

	entry := models.LogEntry{
		Timestamp:       safe(header, "Timestamp"),
		LogTimestamp:    safe(header, "LogTimestamp"),
		Component:       safe(header, "Component"),
		Severity:        parseUint8(safe(header, "Severity")),
		Level:           safe(header, "level"),
		Process:         safe(header, "process"),
		ProcessName:     safe(header, "p:processName"),
		OSThread:        parseUint32(safe(header, "OSThread")),
		ClientID:        parseUint32(safe(header, "t:clientID")),
		ApplicationName: safe(header, "t:applicationName"),
		ComputerName:    safe(header, "t:computerName"),
		ConnectID:       parseUint32(safe(header, "t:connectID")),
		SessionID:       parseUint64(safe(header, "SessionID")),
		User:            safe(header, "Usr"),
		DBMS:            safe(header, "DBMS"),
		Database:        safe(header, "DataBase"),
		Trans:           parseUint32(safe(header, "Trans")),
		DBPID:           parseUint32(safe(header, "dbpid")),
		SQL:             sql,
		Rows:            parseInt32(safe(header, "Rows")),
		RowsAffected:    parseInt32(safe(header, "RowsAffected")),
		Context:         context,
		EventType:       safe(header, "Event"),
		File:            safe(header, "File"),
		InsertedAt:      time.Now(),
	}
	return entry, nil
}

// --- Парсер сырого текста ---

// ParseLogRecord разбивает сырой текст лога на шапку, SQL и Context.
func ParseLogRecord(raw string) (header map[string]string, sql string, context string) {
	header = make(map[string]string)
	sqlIdx := strings.Index(raw, "Sql=")
	if sqlIdx == -1 {
		return parseSimpleHeader(raw), "", extractContext(raw)
	}
	headerPart := raw[:sqlIdx]
	sqlAndAfter := raw[sqlIdx+4:]
	if len(sqlAndAfter) == 0 {
		return parseSimpleHeader(headerPart), "", ""
	}
	quote := sqlAndAfter[0]
	sqlAndAfter = sqlAndAfter[1:]
	sqlText, afterSQL := extractSQL(sqlAndAfter, quote)
	contextText := extractContext(afterSQL)
	return parseSimpleHeader(headerPart), sqlText, contextText
}

func parseSimpleHeader(headerRaw string) map[string]string {
	res := make(map[string]string)
	parts := strings.Split(headerRaw, ",")
	if len(parts) > 0 {
		res["LogTimestamp"] = strings.TrimSpace(parts[0])
	}
	if len(parts) > 1 {
		res["Component"] = strings.TrimSpace(parts[1])
	}
	if len(parts) > 2 {
		res["Severity"] = strings.TrimSpace(parts[2])
	}
	for _, part := range parts[3:] {
		if eq := strings.Index(part, "="); eq > 0 {
			k := strings.TrimSpace(part[:eq])
			v := strings.Trim(part[eq+1:], " '")
			res[k] = v
		}
	}
	return res
}

// --- Безопасные преобразования ---
func safe(m map[string]string, k string) string {
	if v, ok := m[k]; ok {
		return v
	}
	return ""
}

func parseUint8(s string) uint8 {
	n, _ := strconv.ParseUint(s, 10, 8)
	return uint8(n)
}

func parseUint32(s string) uint32 {
	n, _ := strconv.ParseUint(s, 10, 32)
	return uint32(n)
}

func parseUint64(s string) uint64 {
	n, _ := strconv.ParseUint(s, 10, 64)
	return uint64(n)
}

func parseInt32(s string) int32 {
	n, _ := strconv.ParseInt(s, 10, 32)
	return int32(n)
}
