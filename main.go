package main

import (
	"context"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"go.uber.org/zap"

	"1CLogPumpClickHouse/batch"
	"1CLogPumpClickHouse/clickhouseclient"
	"1CLogPumpClickHouse/config"
	"1CLogPumpClickHouse/logger"
	"1CLogPumpClickHouse/models"
	"1CLogPumpClickHouse/watcher"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)

	rootLogger := logger.InitZap()
	lg := rootLogger.Named("main")
	defer lg.Sync()
	lg.Info("Сервис 1CLogPump стартует…")

	cfg, err := config.LoadConfig("config.xml")
	if err != nil {
		lg.Fatal("Ошибка загрузки config.xml", zap.Error(err))
	}
	lg.Info("config.xml успешно загружен")

	// Проверка существования logcfg.xml
	if _, err := os.Stat(cfg.LogCfgPath); err != nil {
		lg.Fatal("Файл logcfg.xml не найден по пути", zap.String("LogCfgPath", cfg.LogCfgPath), zap.Error(err))
	}

	logFiles, err := config.LoadLogFiles(cfg.LogCfgPath)
	if err != nil {
		lg.Fatal("Ошибка загрузки logcfg.xml", zap.Error(err), zap.String("LogCfgPath", cfg.LogCfgPath))
	}
	lg.Info("logcfg.xml успешно загружен", zap.Int("count", len(logFiles)), zap.String("LogCfgPath", cfg.LogCfgPath))

	clickhouseLogger := lg.Named("clickhouse")
	chClient, err := clickhouseclient.New(cfg.ClickHouse, clickhouseLogger)
	if err != nil {
		lg.Fatal("Ошибка подключения к ClickHouse", zap.Error(err))
	}
	defer chClient.Close()

	batchCh := make(chan models.LogEntry, cfg.BatchSize*2)

	// ВНИМАНИЕ! Передаём путь к logcfg.xml явно, чтобы watcher мог отслеживать именно этот файл
	watcherLogger := lg.Named("watcher")
	watcherCfg := watcher.Config{Files: logFiles, Logger: watcherLogger, LogCfgPath: cfg.LogCfgPath}
	w := watcher.New(watcherCfg, batchCh)

	batcherLogger := lg.Named("batcher")
	batcher := batch.NewBatcher(cfg.BatchSize, cfg.BatchInterval(), batcherLogger, chClient)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() { defer wg.Done(); w.Start(ctx) }()
	wg.Add(1)
	go func() { defer wg.Done(); batcher.Run(ctx, batchCh) }()

	<-stop
	lg.Info("Получен сигнал остановки, начинаем завершение работы")
	cancel()
	wg.Wait()
	lg.Info("Сервис завершил работу")
}
