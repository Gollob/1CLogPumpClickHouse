package models

import "time"

// LogEntry — структура для основного парсинга и ClickHouse
// Timestamp = имя файла-лога
// LogTimestamp = исходное время события внутри лога (если требуется)
type LogEntry struct {
	Timestamp       string // Имя файла (например: "25052607.log")
	LogTimestamp    string // Время события из первой строки лога, если нужно (например: "00:03.310025-1327862")
	Component       string
	Severity        uint8
	Level           string
	Process         string
	ProcessName     string
	OSThread        uint32
	ClientID        uint32
	ApplicationName string
	ComputerName    string
	ConnectID       uint32
	SessionID       uint64
	User            string
	DBMS            string
	Database        string
	Trans           uint32
	DBPID           uint32
	SQL             string
	Rows            int32
	RowsAffected    int32
	Context         string
	EventType       string
	File            string
	InsertedAt      time.Time
}

// LogEntryFull — алиас для совместимости с clickhouseclient (используй LogEntry как основную модель)
type LogEntryFull = LogEntry
type TechLogRow struct {
	EventDate     string
	EventTime     string
	EventType     string
	Duration      uint32
	User          string
	InfoBase      string
	SessionID     uint32
	ClientID      uint32
	ConnectionID  uint32
	ExceptionType *string
	ErrorText     *string
	SQLText       *string
	Rows          *int32
	RowsAffected  *int32
	Context       *string
	ProcessName   string
}
