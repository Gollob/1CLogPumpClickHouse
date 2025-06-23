package watcher

import (
	"1CLogPumpClickHouse/internal/config"
	"1CLogPumpClickHouse/internal/models"
	"1CLogPumpClickHouse/internal/storage"
	"context"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/fsnotify/fsnotify"
	"github.com/hpcloud/tail"
	"go.uber.org/zap"
)

// Config — параметры watcher
// ConfigPath — путь к config.yaml
// Logger — zap логгер
// Храним смещение (offset) последней прочитанной позиции для каждого файла

// При первом запуске (processed файлы отсутствуют) сканируем все файлы
// Далее — только последние файлы и продолжаем с сохранённых offset

type Config struct {
	Config     *config.Config
	ConfigPath string
	Logger     *zap.Logger
	Store      storage.ProcessedStore
}

type Watcher struct {
	cfg        Config
	store      storage.ProcessedStore
	batchCh    chan<- models.LogEntry
	files      map[string]*tail.Tail // активные tail'ы
	processed  map[string]int64      // path -> смещение
	mu         sync.RWMutex
	ctx        context.Context
	dirWatcher *fsnotify.Watcher
}

// New создаёт Watcher и загружает состояние processed из JSONL
func New(cfg Config, batchCh chan models.LogEntry) *Watcher {
	// загружаем уже обработанные смещения
	processed, err := cfg.Store.Load()
	if err != nil {
		cfg.Logger.Error("Не удалось загрузить processed_files", zap.Error(err))
		processed = make(map[string]int64)
	}

	return &Watcher{
		cfg:       cfg,
		store:     cfg.Store,
		batchCh:   batchCh,
		files:     make(map[string]*tail.Tail), // инициализируем map, чтобы avoid nil
		processed: processed,                   // уже гарантированно non-nil
	}
}

// Start запускает Watcher
func (w *Watcher) Start(ctx context.Context) {
	w.ctx = ctx
	go w.watchConfig()
	w.scanInitialFiles()
	dw, err := fsnotify.NewWatcher()
	if err != nil {
		w.cfg.Logger.Error("Ошибка создания watcher для каталогов", zap.Error(err))
	} else {
		w.dirWatcher = dw
		for _, dir := range w.cfg.Config.LogDirectoryMap {
			filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
				if err == nil && info.IsDir() {
					dw.Add(path)
				}
				return nil
			})
		}
		w.cfg.Logger.Info("Старт слежения за каталогами логов")
		go w.handleDirEvents(dw)
	}

	for _, dir := range w.cfg.Config.LogDirectoryMap {
		root := filepath.Dir(dir) // берём родителя
		_ = dw.Add(root)          // подписываемся на него тоже
	}

	// Периодическое сохранение processed
	go func() {
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if err := w.store.Save(w.processed); err != nil {
					w.cfg.Logger.Error("Не удалось сохранить processed_files", zap.Error(err))
				}
			}
		}
	}()

	<-ctx.Done()
	w.cfg.Logger.Info("Watcher остановлен по сигналу shutdown")
	if err := w.store.Save(w.processed); err != nil {
		w.cfg.Logger.Error("Не удалось сохранить processed_files", zap.Error(err))
	}
}
