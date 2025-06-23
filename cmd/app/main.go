package main

import (
	"1CLogPumpClickHouse/internal/batch"
	"1CLogPumpClickHouse/internal/clickhouseclient"
	"1CLogPumpClickHouse/internal/config"
	"1CLogPumpClickHouse/internal/logger"
	"1CLogPumpClickHouse/internal/models"
	"1CLogPumpClickHouse/internal/storage"
	"1CLogPumpClickHouse/internal/watcher"
	"context"
	"os"
	"os/signal"
	"syscall"

	"go.uber.org/zap"
)

func main() {
	cfg, err := config.LoadConfig("config.yaml")
	if err != nil {
		panic(err)
	}

	rootLogger, err := logger.InitZap(&cfg.Logging)
	if err != nil {
		panic(err)
	}
	defer rootLogger.Sync()
	rootLogger.Info("Сервис стартует…")

	var store storage.ProcessedStore
	if cfg.ProcessedStorage == "redis" {
		store, err = storage.NewRedisStore(&cfg.Redis, "processed_files")
		if err != nil {
			rootLogger.Fatal("Ошибка подключения к Redis", zap.Error(err))
		}
	} else {
		store = storage.NewFileStore("temp/processed_files.json")
	}

	// Использование store сохранено — он передаётся в watcher.Config ниже.

	chLogger := rootLogger.Named("clickhouse")
	chClient, err := clickhouseclient.New(cfg.ClickHouse, chLogger)
	if err != nil {
		rootLogger.Fatal("Ошибка ClickHouse", zap.Error(err))
	}
	defer chClient.Close()

	batchCh := make(chan models.LogEntry, cfg.BatchSize*2)

	wCfg := watcher.Config{
		Config:     cfg,
		ConfigPath: "config.yaml",
		Logger:     rootLogger.Named("watcher"),
		Store:      store,
	}
	w := watcher.New(wCfg, batchCh)

	batcher := batch.NewBatcher(cfg.BatchSize, cfg.BatchInterval, rootLogger.Named("batcher"), chClient)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		w.Start(ctx)
	}()
	go func() {
		batcher.Run(ctx, batchCh)
	}()

	<-sigCh
	rootLogger.Info("Получен сигнал завершения, останавливаем…")
	cancel()
	rootLogger.Info("Сервис завершён")
}
