package parser

import (
	"regexp"
	"strings"
)

// extractSQL — извлекает SQL-текст между кавычками, пропуская временные метки
// s — строка, содержащая SQL и последующий текст
// quote — символ кавычки, ограничивающий SQL
// Возвращает SQL-текст (без временных меток) и остаток строки после SQL
func extractSQL(s string, quote byte) (sql string, after string) {
	// Регулярное выражение для поиска временных меток в формате YYYY-MM-DD HH:MM:SS
	timestampRegex := regexp.MustCompile(`\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}`)

	inEscape := false
	sqlBuilder := strings.Builder{}

	// Проходим по строке до закрывающей кавычки
	for i := 0; i < len(s); i++ {
		if s[i] == quote && !inEscape {
			// Нашли конец SQL, возвращаем результат
			sqlText := sqlBuilder.String()
			// Удаляем временные метки из SQL-текста
			sqlText = timestampRegex.ReplaceAllString(sqlText, "")
			// Удаляем лишние пробелы, которые могли остаться после удаления меток
			sqlText = strings.TrimSpace(sqlText)
			return sqlText, s[i+1:]
		}
		if s[i] == '\\' && !inEscape {
			// Обработка экранирования
			inEscape = true
		} else {
			inEscape = false
			// Добавляем символ в SQL-текст
			sqlBuilder.WriteByte(s[i])
		}
	}

	// Если не нашли закрывающую кавычку, возвращаем весь текст без временных меток
	sqlText := sqlBuilder.String()
	sqlText = timestampRegex.ReplaceAllString(sqlText, "")
	sqlText = strings.TrimSpace(sqlText)
	return sqlText, ""
}
