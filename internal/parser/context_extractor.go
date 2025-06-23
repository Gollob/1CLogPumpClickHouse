package parser

import "strings"

// extractContext — ищет ,Context='...' и достаёт многострочный Context
func extractContext(s string) string {
	idx := strings.Index(s, ",Context='")
	if idx == -1 {
		return ""
	}
	ctx := s[idx+len(",Context='"):]
	end := strings.LastIndex(ctx, "'") // Исправлено: только одиночная кавычка
	if end == -1 {
		return ctx
	}
	return ctx[:end]
}
