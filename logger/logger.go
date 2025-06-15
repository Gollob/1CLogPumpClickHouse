package logger

import (
	"go.uber.org/zap"
)

// InitZap инициализирует zap логгер
func InitZap() *zap.Logger {
	logger, _ := zap.NewProduction() // Можно вынести настройки в конфиг
	return logger
}
