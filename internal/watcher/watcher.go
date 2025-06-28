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

type Config struct {
	Config     *config.Config
	ConfigPath string
	Logger     *zap.Logger
	Store      storage.ProcessedStore
}

type Watcher struct {
	cfg         Config
	store       storage.ProcessedStore
	batchCh     chan<- models.LogEntry
	files       map[string]*tail.Tail
	processed   map[string]int64
	mu          sync.RWMutex
	ctx         context.Context
	dirWatcher  *fsnotify.Watcher
	watchedDirs map[string]struct{} // Отслеживаемые директории
}

func New(cfg Config, batchCh chan models.LogEntry) *Watcher {
	processed, err := cfg.Store.Load()
	if err != nil {
		cfg.Logger.Error("Не удалось загрузить processed_files", zap.Error(err))
		processed = make(map[string]int64)
	}

	return &Watcher{
		cfg:         cfg,
		store:       cfg.Store,
		batchCh:     batchCh,
		files:       make(map[string]*tail.Tail),
		processed:   processed,
		watchedDirs: make(map[string]struct{}),
	}
}

// addWatchers рекурсивно добавляет наблюдателей для директорий
func (w *Watcher) addWatchers(dir string, dw *fsnotify.Watcher) error {
	return filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			w.cfg.Logger.Debug("Ошибка при обходе директории", zap.String("path", path), zap.Error(err))
			return nil
		}
		if info.IsDir() {
			w.mu.Lock()
			if _, exists := w.watchedDirs[path]; !exists {
				if err := dw.Add(path); err != nil {
					w.cfg.Logger.Error("Ошибка добавления наблюдателя", zap.String("dir", path), zap.Error(err))
				} else {
					w.watchedDirs[path] = struct{}{}
					w.cfg.Logger.Debug("Добавлен наблюдатель для директории", zap.String("dir", path))
				}
			}
			w.mu.Unlock()
		}
		return nil
	})
}

// runPeriodicScan периодически сканирует директории
func (w *Watcher) runPeriodicScan() {
	ticker := time.NewTicker(time.Duration(w.cfg.Config.RescanInterval) * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-w.ctx.Done():
			w.cfg.Logger.Info("Периодическое сканирование завершено")
			return
		case <-ticker.C:
			w.cfg.Logger.Debug("Запуск периодического сканирования директорий")
			w.ScanInitialFiles()
		}
	}
}

func (w *Watcher) Start(ctx context.Context) error {
	w.ctx = ctx

	// Инициализируем fsnotify
	dw, err := fsnotify.NewWatcher()
	if err != nil {
		w.cfg.Logger.Fatal("Ошибка создания watcher для каталогов", zap.Error(err))
		return err
	}
	w.dirWatcher = dw
	defer dw.Close()

	// Добавляем наблюдателей для всех директорий и их родителей
	for _, dir := range w.cfg.Config.LogDirectoryMap {
		root := filepath.Dir(dir)
		if err := w.addWatchers(root, dw); err != nil {
			w.cfg.Logger.Debug("Ошибка при добавлении наблюдателей", zap.String("dir", root), zap.Error(err))
		}
		if err := w.addWatchers(dir, dw); err != nil {
			w.cfg.Logger.Debug("Ошибка при добавлении наблюдателей", zap.String("dir", dir), zap.Error(err))
		}
	}

	// Запускаем начальное сканирование
	w.ScanInitialFiles()

	// Запускаем обработку событий
	go w.handleDirEvents(dw)

	// Запускаем наблюдение за конфигом
	go w.watchConfig()

	// Запускаем периодическое сканирование
	go w.runPeriodicScan()

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
	return nil
}
