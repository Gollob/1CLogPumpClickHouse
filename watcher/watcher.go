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
// Config — полная структура сервиса
// Используем glob-паттерн для файлов по FilePattern
// Обходим корневые директории и поддиректории
// Динамически перечитываем конфиг при изменениях
// Сохраняем processed_files.json в рабочей папке
// Метаданные: последняя распарсенная строка для каждого файла
// При инициализации обрабатываем только последний файл в каждой директории

type Config struct {
	Config     *config.Config
	ConfigPath string
	Logger     *zap.Logger
}

type Watcher struct {
	cfg       Config
	batchCh   chan<- models.LogEntry
	files     map[string]*tail.Tail
	processed map[string]string // path->last parsed line
	mu        sync.Mutex
	ctx       context.Context
}

// New создаёт Watcher
func New(cfg Config, batchCh chan<- models.LogEntry) *Watcher {
	w := &Watcher{
		cfg:       cfg,
		batchCh:   batchCh,
		files:     make(map[string]*tail.Tail),
		processed: make(map[string]string),
	}
	w.loadProcessed()
	return w
}

// loadProcessed загружает processed metadata
func (w *Watcher) loadProcessed() {
	data, err := os.ReadFile("processed_files.json")
	if err == nil {
		json.Unmarshal(data, &w.processed)
	}
}

// saveProcessed сохраняет processed metadata
func (w *Watcher) saveProcessed() {
	data, err := json.Marshal(w.processed)
	if err != nil {
		w.cfg.Logger.Error("Не удалось сериализовать processed map", zap.Error(err))
		return
	}
	tmp := "processed_files.json.tmp"
	if err := os.WriteFile(tmp, data, 0644); err != nil {
		w.cfg.Logger.Error("Не удалось записать временный processed file", zap.Error(err))
		return
	}
	os.Rename(tmp, "processed_files.json")
}

// Start запускает Watcher
func (w *Watcher) Start(ctx context.Context) {
	w.ctx = ctx
	// Следим за конфигом
	go w.watchConfig()

	// Первичная обработка: для каждого директория - только последний файл
	w.scanInitialFiles()

	// Слежение за директориями логов
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

// scanInitialFiles обрабатывает только последний файл в каждой директории
func (w *Watcher) scanInitialFiles() {
	pattern := w.cfg.Config.FilePattern
	for _, dir := range w.cfg.Config.LogDirectoryMap {
		var latest string
		var mt time.Time
		filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
			if err != nil || info.IsDir() {
				return nil
			}
			match, _ := filepath.Match(pattern, filepath.Base(path))
			if match && info.ModTime().After(mt) {
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

// watchConfig следит за изменениями config.yaml
func (w *Watcher) watchConfig() {
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		w.cfg.Logger.Error("Не удалось создать watcher для конфига", zap.Error(err))
		return
	}
	defer watcher.Close()
	watcher.Add(w.cfg.ConfigPath)
	w.cfg.Logger.Info("Старт слежения за config.yaml", zap.String("path", w.cfg.ConfigPath))
	for {
		select {
		case <-w.ctx.Done():
			return
		case ev := <-watcher.Events:
			if ev.Op&fsnotify.Write != 0 || ev.Op&fsnotify.Create != 0 {
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
				if filepath.Ext(ev.Name) == ".log" {
					if ev.Op&(fsnotify.Create|fsnotify.Write|fsnotify.Rename) != 0 {
						w.startTail(ev.Name)
					}
					if ev.Op&fsnotify.Remove != 0 {
						w.stopTail(ev.Name)
					}
				}
			}
		case err := <-dw.Errors:
			w.cfg.Logger.Error("Ошибка watcher для каталогов", zap.Error(err))
		}
	}
}

// startTail запускает tail для файла
func (w *Watcher) startTail(path string) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if _, ok := w.files[path]; ok {
		return
	}

	// Определяем смещение для tail, пропуская строки до последней обработанной
	var loc tail.SeekInfo
	if last, ok := w.processed[path]; ok {
		file, err := os.Open(path)
		if err != nil {
			loc = tail.SeekInfo{Offset: 0, Whence: io.SeekStart}
		} else {
			scanner := bufio.NewScanner(file)
			var offset int64
			for scanner.Scan() {
				if scanner.Text() == last {
					pos, err := file.Seek(0, io.SeekCurrent)
					if err == nil {
						offset = pos
					}
					break
				}
			}
			file.Close()
			loc = tail.SeekInfo{Offset: offset, Whence: io.SeekStart}
		}
	} else {
		loc = tail.SeekInfo{Offset: 0, Whence: io.SeekStart}
	}

	t, err := tail.TailFile(path, tail.Config{
		Follow:    true,
		ReOpen:    true,
		MustExist: false,
		Location:  &loc,
	})
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

// readTail читает линии и пачки отправляет в batchCh
func (w *Watcher) readTail(path string, t *tail.Tail) {
	var buffer []string
	var timer *time.Timer
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
		entry.Timestamp = filepath.Base(path)
		w.batchCh <- entry
		w.mu.Lock()
		w.processed[path] = buffer[len(buffer)-1]
		w.mu.Unlock()
		buffer = buffer[:0]
	}
	reset := func() {
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
			if isNewLogRecord(line.Text) {
				flush()
			}
			buffer = append(buffer, line.Text)
			reset()
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

// isNewLogRecord определяет начало записи
func isNewLogRecord(s string) bool {
	if len(s) < 10 {
		return false
	}
	return s[2] == ':' && s[5] == '.' && strings.Index(s, "-") > 0
}
