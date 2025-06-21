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
	// контекст для остановки
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// ловим сигналы завершения
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)

	// инициализируем логгер
	rootLogger := logger.InitZap()
	lg := rootLogger.Named("main")
	defer lg.Sync()
	lg.Info("Сервис 1CLogPump стартует…")

	// загружаем конфиг из YAML
	cfg, err := config.LoadConfig("config.yaml")
	if err != nil {
		lg.Fatal("Ошибка загрузки config.yaml", zap.Error(err))
	}
	lg.Info("config.yaml успешно загружен")

	// создаем клиента ClickHouse
	clickhouseLogger := lg.Named("clickhouse")
	chClient, err := clickhouseclient.New(cfg.ClickHouse, clickhouseLogger)
	if err != nil {
		lg.Fatal("Ошибка подключения к ClickHouse", zap.Error(err))
	}
	defer chClient.Close()

	// канал для батчей
	batchCh := make(chan models.LogEntry, cfg.BatchSize*2)

	// настраиваем watcher
	watcherLogger := lg.Named("watcher")
	watcherCfg := watcher.Config{
		Config:     cfg,
		ConfigPath: "config.yaml",
		Logger:     watcherLogger,
	}
	w := watcher.New(watcherCfg, batchCh)

	// настраиваем батчер
	batcherLogger := lg.Named("batcher")
	batcher := batch.NewBatcher(cfg.BatchSize, cfg.BatchInterval, batcherLogger, chClient)

	// запускаем Watcher и Batcher
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		w.Start(ctx)
	}()
	wg.Add(1)
	go func() {
		defer wg.Done()
		batcher.Run(ctx, batchCh)
	}()

	// ожидаем сигнала остановки
	<-stop
	lg.Info("Получен сигнал остановки, начинаем завершение работы")
	cancel()
	wg.Wait()
	lg.Info("Сервис завершил работу")
}
