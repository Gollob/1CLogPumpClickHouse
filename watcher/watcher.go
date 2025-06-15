package watcher

import (
	"context"
	"strings"
	"sync"
	"time"

	"1CLogPumpClickHouse/config"
	"1CLogPumpClickHouse/models"
	"1CLogPumpClickHouse/parser"
	"github.com/fsnotify/fsnotify"
	"github.com/hpcloud/tail"
	"go.uber.org/zap"
)

// Config — параметры watcher
// Files — список файлов для наблюдения
// Logger — zap логгер
type Config struct {
	Files      []config.LogFile
	Logger     *zap.Logger
	LogCfgPath string // добавлено поле для корректного reload
}

type Watcher struct {
	cfg     Config
	batchCh chan<- models.LogEntry
	files   map[string]*tail.Tail
	mu      sync.Mutex
	ctx     context.Context
}

func New(cfg Config, batchCh chan<- models.LogEntry) *Watcher {
	return &Watcher{
		cfg:     cfg,
		batchCh: batchCh,
		files:   make(map[string]*tail.Tail),
	}
}

func (w *Watcher) Start(ctx context.Context) {
	w.ctx = ctx
	go w.watchLogCfg()
	for _, lf := range w.cfg.Files {
		w.startTail(lf.Path)
	}
	<-ctx.Done()
	w.cfg.Logger.Info("Watcher остановлен по сигналу shutdown")
}

func (w *Watcher) startTail(path string) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if _, ok := w.files[path]; ok {
		return
	}
	t, err := tail.TailFile(path, tail.Config{
		Follow:    true,
		ReOpen:    true,
		MustExist: false,
		Poll:      true,
	})
	if err != nil {
		w.cfg.Logger.Error("Ошибка открытия tail", zap.String("file", path), zap.Error(err))
		return
	}
	w.files[path] = t
	w.cfg.Logger.Info("Запущен tail для файла", zap.String("file", path))
	go w.readTailLines(path, t)
}

func (w *Watcher) readTailLines(path string, t *tail.Tail) {
	var (
		buffer []string
		timer  *time.Timer
	)
	flush := func() {
		if len(buffer) == 0 {
			return
		}
		entry, err := parser.ParseLine(buffer)
		if err != nil {
			w.cfg.Logger.Warn("Ошибка парсинга лога", zap.String("file", path), zap.Error(err))
			buffer = buffer[:0]
			return
		}
		w.batchCh <- entry
		buffer = buffer[:0]
	}
	resetTimer := func() {
		if timer != nil {
			timer.Stop()
		}
		timer = time.NewTimer(2 * time.Second)
	}
	for {
		select {
		case <-w.ctx.Done():
			flush()
			return
		case line, ok := <-t.Lines:
			if !ok {
				flush()
				return
			}
			text := line.Text
			if isNewLogRecord(text) {
				flush()
			}
			buffer = append(buffer, text)
			resetTimer()
		case <-func() <-chan time.Time {
			if timer != nil {
				return timer.C
			}
			return make(chan time.Time)
		}():
			flush()
		}
	}
}

func isNewLogRecord(s string) bool {
	if len(s) < 10 {
		return false
	}
	return s[2] == ':' && s[5] == '.' && strings.Index(s, "-") > 0
}

// Теперь для watchLogCfg/reloadLogFiles используем путь из конфига
func (w *Watcher) watchLogCfg() {
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		w.cfg.Logger.Error("Ошибка создания fsnotify для logcfg.xml", zap.Error(err))
		return
	}
	defer watcher.Close()

	logCfgPath := w.cfg.LogCfgPath
	if logCfgPath == "" {
		logCfgPath = "logcfg.xml"
	}
	if err := watcher.Add(logCfgPath); err != nil {
		w.cfg.Logger.Error("Ошибка добавления logcfg.xml в watcher", zap.Error(err), zap.String("LogCfgPath", logCfgPath))
		return
	}
	w.cfg.Logger.Info("Старт слежения за logcfg.xml", zap.String("LogCfgPath", logCfgPath))
	for {
		select {
		case <-w.ctx.Done():
			return
		case event := <-watcher.Events:
			if event.Op&fsnotify.Write == fsnotify.Write || event.Op&fsnotify.Create == fsnotify.Create {
				w.cfg.Logger.Info("Обнаружено обновление logcfg.xml, перечитываем...", zap.String("LogCfgPath", logCfgPath))
				w.reloadLogFiles(logCfgPath)
			}
		case err := <-watcher.Errors:
			w.cfg.Logger.Error("Ошибка watcher-а logcfg.xml", zap.Error(err))
		}
	}
}

func (w *Watcher) reloadLogFiles(logCfgPath string) {
	logFiles, err := config.LoadLogFiles(logCfgPath)
	if err != nil {
		w.cfg.Logger.Error("Ошибка при перечитывании logcfg.xml", zap.Error(err), zap.String("LogCfgPath", logCfgPath))
		return
	}
	for _, lf := range logFiles {
		w.startTail(lf.Path)
	}
}
