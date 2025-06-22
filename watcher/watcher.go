package watcher

import (
	"bufio"
	"context"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/fsnotify/fsnotify"
	"github.com/hpcloud/tail"
	"go.uber.org/zap"

	"1CLogPumpClickHouse/config"
	"1CLogPumpClickHouse/models"
	"1CLogPumpClickHouse/parser"
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
}

type Watcher struct {
	cfg       Config
	batchCh   chan<- models.LogEntry
	files     map[string]*tail.Tail // активные tail'ы
	processed map[string]int64      // path -> смещение
	mu        sync.RWMutex
	ctx       context.Context
}

// New создаёт Watcher и загружает состояние processed из JSONL
func New(cfg Config, batchCh chan<- models.LogEntry) *Watcher {
	w := &Watcher{
		cfg:       cfg,
		batchCh:   batchCh,
		files:     make(map[string]*tail.Tail),
		processed: make(map[string]int64),
	}
	w.loadProcessed()
	return w
}

// loadProcessed загружает processed metadata из JSONL файла: по одной записи на строку
func (w *Watcher) loadProcessed() {
	file, err := os.Open("processed_files.json")
	if err != nil {
		// Первый запуск или отсутствует файл
		return
	}
	defer file.Close()
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		var rec struct {
			Path   string `json:"path"`
			Offset int64  `json:"offset"`
		}
		err := json.Unmarshal(scanner.Bytes(), &rec)
		if err == nil {
			w.processed[rec.Path] = rec.Offset
		}
	}
}

// saveProcessed сохраняет processed metadata в JSONL файл: по одной записи на строку
func (w *Watcher) saveProcessed() {
	tmp := "processed_files.json.tmp"
	file, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		w.cfg.Logger.Error("Не удалось создать временный processed file", zap.Error(err))
		return
	}
	encoder := json.NewEncoder(file)
	w.mu.RLock()
	for path, off := range w.processed {
		rec := struct {
			Path   string `json:"path"`
			Offset int64  `json:"offset"`
		}{Path: path, Offset: off}
		if err := encoder.Encode(&rec); err != nil {
			w.cfg.Logger.Error("Не удалось сериализовать запись processed", zap.String("path", path), zap.Error(err))
		}
	}
	w.mu.RUnlock()
	file.Close()
	os.Rename(tmp, "processed_files.json")
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
	<-ctx.Done()
	w.cfg.Logger.Info("Watcher остановлен по сигналу shutdown")
	w.saveProcessed()
}

// scanInitialFiles: если processed пуст — первый запуск, сканируем все файлы; иначе — только последний
func (w *Watcher) scanInitialFiles() {
	pattern := w.cfg.Config.FilePattern
	firstRun := len(w.processed) == 0
	for _, dir := range w.cfg.Config.LogDirectoryMap {
		if firstRun {
			filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
				if err != nil || info.IsDir() {
					return nil
				}
				if ok, _ := filepath.Match(pattern, filepath.Base(path)); ok {
					w.startTail(path)
				}
				return nil
			})
		} else {
			var latest string
			var mt time.Time
			filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
				if err != nil || info.IsDir() {
					return nil
				}
				if ok, _ := filepath.Match(pattern, filepath.Base(path)); ok && info.ModTime().After(mt) {
					mt = info.ModTime()
					latest = path
				}
				return nil
			})
			if latest != "" {
				w.startTail(latest)
			}
		}
	}
}

// watchConfig следит за изменениями config.yaml
func (w *Watcher) watchConfig() {
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		w.cfg.Logger.Error("Не удалось создать watcher для конфига", zap.Error(err))
		return
	}
	defer watcher.Close()
	watcher.Add(w.cfg.ConfigPath)
	for {
		select {
		case <-w.ctx.Done():
			return
		case ev := <-watcher.Events:
			if ev.Op&(fsnotify.Write|fsnotify.Create) != 0 {
				w.cfg.Logger.Info("Конфиг изменился, перечитываем", zap.String("path", w.cfg.ConfigPath))
				newCfg, err := config.LoadConfig(w.cfg.ConfigPath)
				if err != nil {
					w.cfg.Logger.Error("Ошибка загрузки config.yaml", zap.Error(err))
					continue
				}
				w.mu.Lock()
				w.cfg.Config = newCfg
				w.mu.Unlock()
			}
		case err := <-watcher.Errors:
			w.cfg.Logger.Error("Ошибка watcher-а конфига", zap.Error(err))
		}
	}
}

// handleDirEvents обрабатывает fsnotify события в папках
func (w *Watcher) handleDirEvents(dw *fsnotify.Watcher) {
	for {
		select {
		case <-w.ctx.Done():
			return
		case ev := <-dw.Events:
			if ev.Op&fsnotify.Create != 0 {
				info, err := os.Stat(ev.Name)
				if err == nil && info.IsDir() {
					filepath.Walk(ev.Name, func(p string, i os.FileInfo, e error) error {
						if e == nil && i.IsDir() {
							dw.Add(p)
						}
						return nil
					})
					continue
				}
			}
			if filepath.Ext(ev.Name) == ".log" {
				if ev.Op&(fsnotify.Create|fsnotify.Write|fsnotify.Rename) != 0 {
					w.startTail(ev.Name)
				}
				if ev.Op&fsnotify.Remove != 0 {
					w.stopTail(ev.Name)
				}
			}
		case err := <-dw.Errors:
			w.cfg.Logger.Error("Ошибка watcher для каталогов", zap.Error(err))
		}
	}
}

// startTail запускает tail для файла, начиная с сохранённого смещения
func (w *Watcher) startTail(path string) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if _, exists := w.files[path]; exists {
		return
	}
	var loc tail.SeekInfo
	if offset, ok := w.processed[path]; ok {
		loc = tail.SeekInfo{Offset: offset, Whence: io.SeekStart}
	} else {
		loc = tail.SeekInfo{Offset: 0, Whence: io.SeekStart}
	}
	t, err := tail.TailFile(path, tail.Config{Follow: true, ReOpen: true, MustExist: false, Location: &loc, Logger: tail.DiscardingLogger})
	if err != nil {
		w.cfg.Logger.Error("Ошибка открытия tail", zap.String("file", path), zap.Error(err))
		return
	}
	w.files[path] = t
	w.cfg.Logger.Info("Запущен tail для файла", zap.String("file", path))
	go w.readTail(path, t)
}

// stopTail останавливает tail и сохраняет processed
func (w *Watcher) stopTail(path string) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if t, ok := w.files[path]; ok {
		t.Stop()
		delete(w.files, path)
		w.saveProcessed()
	}
}

// readTail читает строки, парсит записи и обновляет offset
func (w *Watcher) readTail(path string, t *tail.Tail) {
	defer func() {
		if r := recover(); r != nil {
			w.cfg.Logger.Error("Паника в readTail восстановлена", zap.Any("error", r))
		}
	}()
	var buffer []string
	var timer *time.Timer

	resetTimer := func() {
		if timer != nil {
			timer.Stop()
		}
		timer = time.NewTimer(2 * time.Second)
	}

	flushBuffer := func() {
		if len(buffer) == 0 {
			return
		}
		entry, err := parser.ParseLine(buffer)
		if err != nil {
			w.cfg.Logger.Warn("Ошибка парсинга лога", zap.String("file", path), zap.Error(err))
			buffer = buffer[:0]
			return
		}
		entry.Timestamp = filepath.Base(path)
		// Блокирующая отправка в канал, чтобы не терять записи
		w.batchCh <- entry
		off, err := t.Tell()
		if err == nil {
			w.mu.Lock()
			w.processed[path] = off
			w.mu.Unlock()
		}
		buffer = buffer[:0]
	}

	for {
		select {
		case <-w.ctx.Done():
			flushBuffer()
			return
		case line, ok := <-t.Lines:
			if !ok {
				flushBuffer()
				return
			}
			if isNewLogRecord(line.Text) {
				flushBuffer()
			}
			buffer = append(buffer, line.Text)
			resetTimer()
		case <-func() <-chan time.Time {
			if timer != nil {
				return timer.C
			}
			return make(chan time.Time)
		}():
			flushBuffer()
		}
	}
}

// isNewLogRecord определяет начало новой записи по шаблону времени и дефису
func isNewLogRecord(s string) bool {
	if len(s) < 10 {
		return false
	}
	return s[2] == ':' && s[5] == '.' && strings.Contains(s, "-")
}
