package logger

import (
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// InitZap инициализирует zap логгер с цветным выводом в консоль
func InitZap() *zap.Logger {
	encoderCfg := zapcore.EncoderConfig{
		TimeKey:       "T",
		LevelKey:      "L",
		NameKey:       "N",
		CallerKey:     "C",
		MessageKey:    "M",
		StacktraceKey: "S",
		LineEnding:    zapcore.DefaultLineEnding,
		// Цветная кодировка уровней: DEBUG, INFO, WARN, ERROR
		EncodeLevel:    zapcore.CapitalColorLevelEncoder,
		EncodeTime:     zapcore.ISO8601TimeEncoder,
		EncodeDuration: zapcore.StringDurationEncoder,
		EncodeCaller:   zapcore.ShortCallerEncoder,
	}
	cfg := zap.Config{
		Encoding:         "console",
		Level:            zap.NewAtomicLevelAt(zap.InfoLevel),
		OutputPaths:      []string{"stdout"},
		ErrorOutputPaths: []string{"stderr"},
		EncoderConfig:    encoderCfg,
	}
	logger, err := cfg.Build()
	if err != nil {
		panic("Failed to initialize Zap logger: " + err.Error())
	}
	return logger
}
