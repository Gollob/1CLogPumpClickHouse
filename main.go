package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"go.uber.org/zap"

	"1CLogPumpClickHouse/batch"
	"1CLogPumpClickHouse/clickhouseclient"
	"1CLogPumpClickHouse/config"
	"1CLogPumpClickHouse/logger"
	"1CLogPumpClickHouse/models"
	"1CLogPumpClickHouse/storage"
	"1CLogPumpClickHouse/watcher"
)

func main() {
	// 1. Загрузка конфига
	cfg, err := config.LoadConfig("config.yaml")
	if err != nil {
		panic(err)
	}

	// 2. Инициализация логгера с Sentry
	rootLogger, err := logger.InitZap(&cfg.Logging)
	if err != nil {
		panic(err)
	}
	defer rootLogger.Sync()
	rootLogger.Info("Сервис стартует…")

	// 3. Выбор хранилища processed_files
	var store storage.ProcessedStore
	if cfg.ProcessedStorage == "redis" {
		store = storage.NewRedisStore(&cfg.Redis, "processed_files")
	} else {
		store = storage.NewFileStore("processed_files.json")
	}

	// 4. Подключение к ClickHouse
	chLogger := rootLogger.Named("clickhouse")
	chClient, err := clickhouseclient.New(cfg.ClickHouse, chLogger)
	if err != nil {
		rootLogger.Fatal("Ошибка ClickHouse", zap.Error(err))
	}
	defer chClient.Close()

	// 5. Канал для батчей
	batchCh := make(chan models.LogEntry, cfg.BatchSize*2)

	// 6. Настройка Watcher
	wCfg := watcher.Config{
		Config:     cfg,
		ConfigPath: "config.yaml",
		Logger:     rootLogger.Named("watcher"),
		Store:      store,
	}
	w := watcher.New(wCfg, batchCh)

	// 7. Настройка Batcher
	// batch.NewBatcher(batchSize int, batchIntervalSeconds int, logger, client)
	batcher := batch.NewBatcher(cfg.BatchSize, cfg.BatchInterval, rootLogger.Named("batcher"), chClient)

	// 8. Запуск и graceful shutdown
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
	// по завершении watcher должен сам вызвать store.Save(...)
	rootLogger.Info("Сервис завершён")
}
