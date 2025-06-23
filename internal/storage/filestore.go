package storage

import (
	"encoding/json"
	"io/ioutil"
	"os"
	"sync"
)

type FileStore struct {
	Path string
	mu   sync.Mutex // добавляем мьютекс для защиты при записи
}

func NewFileStore(path string) *FileStore {
	return &FileStore{Path: path}
}

func (f *FileStore) Load() (map[string]int64, error) {
	processed := make(map[string]int64)
	if _, err := os.Stat(f.Path); os.IsNotExist(err) {
		return processed, nil
	}
	bs, err := ioutil.ReadFile(f.Path)
	if err != nil {
		return nil, err
	}
	if err := json.Unmarshal(bs, &processed); err != nil {
		return nil, err
	}
	return processed, nil
}

func (f *FileStore) Save(data map[string]int64) error {
	f.mu.Lock() // начинаем критическую секцию
	defer f.mu.Unlock()
	tmp := f.Path + ".tmp"
	// Кодировка данных в JSON
	bs, err := json.Marshal(data)
	if err != nil {
		return err
	}
	// Запись во временный файл
	if err := ioutil.WriteFile(tmp, bs, 0644); err != nil {
		return err
	}
	// Удаляем старый файл, чтобы Rename не ошибся (актуально для Windows)
	_ = os.Remove(f.Path) // игнорируем ошибку, если файла нет
	// Атомарно переименовываем временный файл в основной
	if err := os.Rename(tmp, f.Path); err != nil {
		return err
	}
	return nil
}
