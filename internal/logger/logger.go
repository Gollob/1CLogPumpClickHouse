package logger

import (
	"1CLogPumpClickHouse/internal/config"
	"fmt"
	"github.com/getsentry/sentry-go"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"os"
	"path/filepath"
	"time"
)

// InitZap инициализирует zap-логгер:
// - в консоль выводятся все сообщения (Debug+);
// - в файл — только ошибки (Error+);
// - при EnableSentry отправляет сообщения указанного уровня (SentryLevel+) в Sentry.
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

	// 5) Определяем уровни логирования
	fileLevel, err := zapcore.ParseLevel(cfg.Level)
	if err != nil {
		return nil, fmt.Errorf("неверный уровень логирования для файла: %w", err)
	}
	consoleLevel, err := zapcore.ParseLevel(cfg.ConsoleLevel)
	if err != nil {
		return nil, fmt.Errorf("неверный уровень логирования для консоли: %w", err)
	}
	sentryLevel := zapcore.ErrorLevel // По умолчанию Error+
	if cfg.SentryLevel != "" {
		sentryLevel, err = zapcore.ParseLevel(cfg.SentryLevel)
		if err != nil {
			return nil, fmt.Errorf("неверный уровень логирования для Sentry: %w", err)
		}
	}

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

	// 8) Интеграция с Sentry
	if cfg.EnableSentry && cfg.SentryDSN != "" {
		if err := sentry.Init(sentry.ClientOptions{
			Dsn:              cfg.SentryDSN,
			Environment:      cfg.Environment, // Add environment from config
			Release:          cfg.Release,     // Add release version from config
			EnableTracing:    true,            // Enable performance tracing
			TracesSampleRate: 0.2,             // Sample 20% of traces
		}); err != nil {
			fmt.Fprintf(os.Stderr, "Sentry initialization failed: %v\n", err)
		} else {
			logger = logger.WithOptions(zap.Hooks(func(entry zapcore.Entry) error {
				if entry.Level >= sentryLevel {
					// Create Sentry event with enriched context
					event := &sentry.Event{
						Message:     entry.Message,
						Level:       sentryLevelToSentry(entry.Level),
						Timestamp:   time.Now(),
						Environment: cfg.Environment,
						Release:     cfg.Release,
						Extra: map[string]interface{}{
							"caller":      entry.Caller.String(),
							"stacktrace":  entry.Stack,
							"logger_name": entry.LoggerName,
						},
						Tags: map[string]string{
							"service": cfg.ServiceName, // Add service name from config
							"level":   entry.Level.String(),
						},
					}
					// Capture event and flush
					sentry.CaptureEvent(event)
					sentry.Flush(2 * time.Second)
				}
				return nil
			}))
		}
	}

	return logger, nil
}

// sentryLevelToSentry преобразует уровень логирования Zap в уровень Sentry
func sentryLevelToSentry(level zapcore.Level) sentry.Level {
	switch level {
	case zapcore.DebugLevel:
		return sentry.LevelDebug
	case zapcore.InfoLevel:
		return sentry.LevelInfo
	case zapcore.WarnLevel:
		return sentry.LevelWarning
	case zapcore.ErrorLevel:
		return sentry.LevelError
	case zapcore.DPanicLevel, zapcore.PanicLevel, zapcore.FatalLevel:
		return sentry.LevelFatal
	default:
		return sentry.LevelInfo
	}
}
