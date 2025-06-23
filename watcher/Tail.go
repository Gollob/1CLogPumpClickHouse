package watcher

import (
	"1CLogPumpClickHouse/parser"
	"io"
	"path/filepath"
	"strings"
	"time"

	"github.com/hpcloud/tail"
	"go.uber.org/zap"
)

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
		if err := w.store.Save(w.processed); err != nil {
			w.cfg.Logger.Error("Не удалось сохранить processed_files", zap.Error(err))
		}
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
			clean := strings.ReplaceAll(line.Text, "\x00", "")
			if strings.Contains(line.Text, "\x00") {
				w.cfg.Logger.Warn("Обнаружены нулевые байты в строке", zap.String("file", path))
			}
			if isNewLogRecord(clean) {
				flushBuffer()
			}
			buffer = append(buffer, clean)
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
