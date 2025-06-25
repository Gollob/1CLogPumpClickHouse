package watcher

import (
	"1CLogPumpClickHouse/internal/config"
	"github.com/fsnotify/fsnotify"
	"go.uber.org/zap"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"time"
)

// --- Добавим регулярное выражение для определения начала новой лог-записи ---
var logRecordRegex = regexp.MustCompile(`\d{2}:\d{2}\.\d{2,}.*-.*`)

// isNewLogRecord определяет начало новой записи по регулярному выражению
func isNewLogRecord(s string) bool {
	return logRecordRegex.MatchString(s)
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
	// Компилируем шаблон имен файлов один раз
	patternStr := w.cfg.Config.FilePattern
	patternStr = strings.ReplaceAll(patternStr, ".", `\.`)
	patternStr = strings.ReplaceAll(patternStr, "*", ".*")
	patternStr = strings.ReplaceAll(patternStr, "?", ".")
	filePattern, err := regexp.Compile("^" + patternStr + "$")
	if err != nil {
		w.cfg.Logger.Error("Неверный FilePattern в конфиге", zap.String("pattern", w.cfg.Config.FilePattern), zap.Error(err))
		// Если шаблон сломан, пропускаем сканирование файлов
	}

	for {
		select {
		case <-w.ctx.Done():
			return
		case ev := <-dw.Events:
			if ev.Op&fsnotify.Create != 0 {
				info, err := os.Stat(ev.Name)
				if err == nil && info.IsDir() {
					filepath.Walk(ev.Name, func(p string, i os.FileInfo, e error) error {
						if e != nil {
							return nil
						}
						if i.IsDir() {
							dw.Add(p)
							w.cfg.Logger.Info("Добавлен watcher для директории", zap.String("dir", p))
						} else if filePattern != nil && filePattern.MatchString(filepath.Base(p)) {
							w.cfg.Logger.Info("Найден файл в новой папке, запускаем tail", zap.String("file", p))
							w.startTail(p)
						}
						return nil
					})
				}
				continue
			}
			if filepath.Ext(ev.Name) == ".log" {
				if ev.Op&(fsnotify.Create|fsnotify.Rename) != 0 {
					w.startTail(ev.Name)
				}
				if ev.Op&fsnotify.Write != 0 {
					// проверяем, добавились ли новые данные
					info, err := os.Stat(ev.Name)
					if err == nil {
						if offset, ok := w.processed[ev.Name]; ok && info.Size() > offset {
							w.startTail(ev.Name)
						}
					}
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

// scanInitialFiles: если processed пуст — первый запуск, сканируем все файлы; иначе — только последний
func (w *Watcher) ScanInitialFiles() {
	patternStr := w.cfg.Config.FilePattern
	patternStr = strings.ReplaceAll(patternStr, ".", `\.`)
	patternStr = strings.ReplaceAll(patternStr, "*", ".*")
	patternStr = strings.ReplaceAll(patternStr, "?", ".")
	pattern, err := regexp.Compile("^" + patternStr + "$")
	if err != nil {
		w.cfg.Logger.Error("Неверный FilePattern", zap.String("pattern", w.cfg.Config.FilePattern), zap.Error(err))
		return
	}

	w.mu.RLock()
	firstRun := len(w.processed) == 0
	w.mu.RUnlock()

	for _, dir := range w.cfg.Config.LogDirectoryMap {
		var files []os.FileInfo
		var paths []string
		filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
			if err != nil || info.IsDir() {
				return nil
			}
			if pattern.MatchString(filepath.Base(path)) {
				files = append(files, info)
				paths = append(paths, path)
			}
			return nil
		})
		type fileWithTime struct {
			Path string
			Mod  time.Time
		}
		var sorted []fileWithTime
		for i, fi := range files {
			sorted = append(sorted, fileWithTime{Path: paths[i], Mod: fi.ModTime()})
		}
		sort.Slice(sorted, func(i, j int) bool {
			return sorted[i].Mod.Before(sorted[j].Mod)
		})
		for _, f := range sorted {
			w.mu.RLock()
			_, already := w.processed[f.Path]
			w.mu.RUnlock()
			if !already || firstRun {
				w.cfg.Logger.Info("Запускаем tail для", zap.String("file", f.Path))
				w.startTail(f.Path)
			} else {
				w.cfg.Logger.Debug("Пропускаем ранее обработанный файл", zap.String("file", f.Path))
			}
		}
	}
}
