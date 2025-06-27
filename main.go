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
	"github.com/kardianos/service"
	"go.uber.org/zap"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
)

type program struct {
	ctx        context.Context
	cancel     context.CancelFunc
	sigCh      chan os.Signal
	rootLogger *zap.Logger
}

func (p *program) Start(s service.Service) error {
	p.ctx, p.cancel = context.WithCancel(context.Background())
	p.sigCh = make(chan os.Signal, 1)
	signal.Notify(p.sigCh, syscall.SIGINT, syscall.SIGTERM)

	go p.run()
	return nil
}

func (p *program) run() {
	//fixWorkingDir()
	cfg, err := config.LoadConfig("config.yaml")
	if err != nil {
		panic(err)
	}

	p.rootLogger, err = logger.InitZap(&cfg.Logging)
	if err != nil {
		panic(err)
	}
	defer p.rootLogger.Sync()
	p.rootLogger.Info("Сервис стартует…")

	var store storage.ProcessedStore
	if cfg.ProcessedStorage == "redis" {
		store, err = storage.NewRedisStore(&cfg.Redis, "processed_files")
		if err != nil {
			p.rootLogger.Fatal("Ошибка подключения к Redis", zap.Error(err))
		}
	} else {
		store = storage.NewFileStore("temp/processed_files.json")
	}

	chLogger := p.rootLogger.Named("clickhouse")
	chClient, err := clickhouseclient.New(cfg.ClickHouse, chLogger)
	if err != nil {
		p.rootLogger.Fatal("Ошибка ClickHouse", zap.Error(err))
	}
	defer chClient.Close()

	batchCh := make(chan models.LogEntry, cfg.BatchSize*2)

	wCfg := watcher.Config{
		Config:     cfg,
		ConfigPath: "config.yaml",
		Logger:     p.rootLogger.Named("watcher"),
		Store:      store,
	}
	w := watcher.New(wCfg, batchCh)
	batcher := batch.NewBatcher(cfg.BatchSize, cfg.BatchInterval, p.rootLogger.Named("batcher"), chClient)

	go w.Start(p.ctx)
	go batcher.Run(p.ctx, batchCh)

	<-p.sigCh
	p.rootLogger.Info("Получен сигнал завершения, останавливаем…")
	p.cancel()
	p.rootLogger.Info("Сервис завершён")
}

func (p *program) Stop(s service.Service) error {
	if p.cancel != nil {
		p.cancel()
	}
	return nil
}

func fixWorkingDir() {
	exePath, err := os.Executable()
	if err != nil {
		panic(err)
	}
	dir := filepath.Dir(exePath)
	err = os.Chdir(dir)
	if err != nil {
		panic(err)
	}
}

func main() {
	svcConfig := &service.Config{
		Name:        "ClickHouseLogPump",
		DisplayName: "1C ClickHouse Log Pump",
		Description: "Служба выгрузки логов 1С в ClickHouse",
	}

	prg := &program{}
	s, err := service.New(prg, svcConfig)
	if err != nil {
		panic(err)
	}

	// Обработка командной строки: install, start, stop, uninstall
	if len(os.Args) > 1 {
		err := service.Control(s, os.Args[1])
		if err != nil {
			panic(err)
		}
		return
	}

	// Запуск как сервис
	err = s.Run()
	if err != nil {
		panic(err)
	}
}
