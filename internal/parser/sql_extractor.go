package parser

// extractSQL — находит SQL между кавычками (любые переносы, любые запятые)
func extractSQL(s string, quote byte) (sql string, after string) {
	inEscape := false
	for i := 0; i < len(s); i++ {
		if s[i] == quote && !inEscape {
			return s[:i], s[i+1:]
		}
		if s[i] == '\\' && !inEscape {
			inEscape = true
		} else {
			inEscape = false
		}
	}
	return s, "" // если не нашли — вернуть всё
}
