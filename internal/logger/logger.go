package logger

import (
	"1CLogPumpClickHouse/internal/config"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/getsentry/sentry-go"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// InitZap инициализирует zap-логгер:
// - в консоль выводятся все сообщения (Debug+);
// - в файл — только ошибки (Error+);
// - при EnableSentry отправляет Error+ в Sentry.
func InitZap(cfg *config.LoggingConfig) (*zap.Logger, error) {
	// 1) Создаём директорию для лог-файла
	if cfg.LogFile != "" {
		dir := filepath.Dir(cfg.LogFile)
		if dir != "." {
			if err := os.MkdirAll(dir, 0o755); err != nil {
				return nil, fmt.Errorf("не удалось создать директорию %s: %w", dir, err)
			}
		}
	}

	// 2) Общая конфигурация энкодера (plain text)
	encoderCfg := zapcore.EncoderConfig{
		TimeKey:        "T",
		LevelKey:       "L",
		NameKey:        "N",
		CallerKey:      "C",
		MessageKey:     "M",
		StacktraceKey:  "S",
		LineEnding:     zapcore.DefaultLineEnding,
		EncodeLevel:    zapcore.CapitalLevelEncoder,
		EncodeTime:     zapcore.ISO8601TimeEncoder,
		EncodeDuration: zapcore.StringDurationEncoder,
		EncodeCaller:   zapcore.ShortCallerEncoder,
	}

	// 3) Создаём WriteSyncer для файла
	var fileWS zapcore.WriteSyncer
	if cfg.LogFile != "" {
		f, err := os.OpenFile(cfg.LogFile, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
		if err != nil {
			return nil, fmt.Errorf("не удалось открыть лог-файл %s: %w", cfg.LogFile, err)
		}
		fileWS = zapcore.AddSync(f)
	}

	// 4) WriteSyncer для консоли
	consoleWS := zapcore.AddSync(os.Stdout)

	// 5) Определяем уровни
	consoleLevel := zapcore.DebugLevel // всё, от Debug и выше
	fileLevel := zapcore.ErrorLevel    // только Error и выше

	// 6) Создаём ядра
	cores := []zapcore.Core{
		// консольное ядро
		zapcore.NewCore(
			zapcore.NewConsoleEncoder(encoderCfg),
			consoleWS,
			zap.LevelEnablerFunc(func(lvl zapcore.Level) bool { return lvl >= consoleLevel }),
		),
	}
	// файловое ядро — только если указан путь
	if fileWS != nil {
		cores = append(cores, zapcore.NewCore(
			zapcore.NewConsoleEncoder(encoderCfg),
			fileWS,
			zap.LevelEnablerFunc(func(lvl zapcore.Level) bool { return lvl >= fileLevel }),
		))
	}

	// 7) Собираем Tee
	logger := zap.New(
		zapcore.NewTee(cores...),
		zap.AddCaller(),
		zap.AddStacktrace(fileLevel),
	)

	// 8) Интеграция с Sentry (Error+)
	if cfg.EnableSentry && cfg.SentryDSN != "" {
		if err := sentry.Init(sentry.ClientOptions{Dsn: cfg.SentryDSN}); err != nil {
			fmt.Fprintf(os.Stderr, "Sentry init failed: %v\n", err)
		} else {
			logger = logger.WithOptions(zap.Hooks(func(entry zapcore.Entry) error {
				if entry.Level >= zapcore.ErrorLevel {
					sentry.CaptureMessage(fmt.Sprintf("%s:%d — %s", entry.Caller.File, entry.Caller.Line, entry.Message))
					sentry.Flush(2 * time.Second)
				}
				return nil
			}))
		}
	}

	return logger, nil
}
