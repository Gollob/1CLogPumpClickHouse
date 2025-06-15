package watcher

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
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
	cfg       Config
	batchCh   chan<- models.LogEntry
	files     map[string]*tail.Tail
	processed map[string]bool // уже обработанные файлы
	mu        sync.Mutex
	ctx       context.Context
}

func New(cfg Config, batchCh chan<- models.LogEntry) *Watcher {
	return &Watcher{
		cfg:     cfg,
		batchCh: batchCh,
		files:   make(map[string]*tail.Tail),
	}
}

// При инициализации читаем сохранённый список:
func (w *Watcher) loadProcessed(file string) {
	data, err := os.ReadFile(file)
	if err == nil {
		json.Unmarshal(data, &w.processed)
	}
}

// Метод пометки и сохранения:
func (w *Watcher) markProcessed(path string) {
	w.processed[path] = true
	data, _ := json.Marshal(w.processed)
	os.WriteFile("processed_files.json", data, 0644)
}
func (w *Watcher) Start(ctx context.Context) {
	w.ctx = ctx
	go w.watchLogCfg()

	// 1) Запускаем tail для уже известных файлов
	for _, lf := range w.cfg.Files {
		w.startTail(lf.Path)
	}

	// 2) Создаём fsnotify-обработчик для каталогов логов
	dirWatcher, err := fsnotify.NewWatcher()
	if err != nil {
		w.cfg.Logger.Error("Ошибка создания fsnotify для папок логов", zap.Error(err))
	} else {
		// Получаем список уникальных директорий из cfg.Files
		dirs := make(map[string]struct{})
		for _, lf := range w.cfg.Files {
			dirs[filepath.Dir(lf.Path)] = struct{}{}
		}
		// Рекурсивно добавляем каталоги в watcher (аналогично filepath.Walk из config):contentReference[oaicite:1]{index=1}
		for dir := range dirs {
			// например, можно пройтись по подкаталогам:
			filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
				if err != nil || !info.IsDir() {
					return nil
				}
				if err := dirWatcher.Add(path); err != nil {
					w.cfg.Logger.Error("Ошибка добавления каталога в watcher", zap.String("dir", path), zap.Error(err))
				}
				return nil
			})
		}
		w.cfg.Logger.Info("Старт слежения за каталогами логов")

		// Горутина обработки событий файловой системы
		go func() {
			for {
				select {
				case <-w.ctx.Done():
					return
				case event := <-dirWatcher.Events:
					// Интересуют только *.log
					if filepath.Ext(event.Name) == ".log" {
						// Создание нового файла
						if event.Op&fsnotify.Create == fsnotify.Create {
							w.startTail(event.Name)
						}
						// Удаление или переименование (файл исчез)
						if event.Op&(fsnotify.Remove|fsnotify.Rename) != 0 {
							w.stopTail(event.Name)
						}
					}
				case err := <-dirWatcher.Errors:
					w.cfg.Logger.Error("Ошибка watcher-а для каталогов", zap.Error(err))
				}
			}
		}()
	}

	<-ctx.Done()
	w.cfg.Logger.Info("Watcher остановлен по сигналу shutdown")
}

func (w *Watcher) stopTail(path string) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if t, ok := w.files[path]; ok {
		// Останавливаем tail (остановка чтения, см. hpcloud/tail):contentReference[oaicite:4]{index=4}
		t.Stop()
		delete(w.files, path)
		w.cfg.Logger.Info("Остановлен tail для файла", zap.String("file", path))
		// Отмечаем файл как обработанный, чтобы избежать повторного разбора
		w.markProcessed(path)
	}
}

func (w *Watcher) startTail(path string) {
	w.mu.Lock()
	defer w.mu.Unlock()
	// Если уже обработано, пропускаем
	if w.processed[path] {
		w.cfg.Logger.Info("Файл уже обработан, пропускаем", zap.String("file", path))
		return
	}
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
