package storage

import (
	"encoding/json"
	"io/ioutil"
	"os"
)

type FileStore struct {
	Path string
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
	tmp := f.Path + ".tmp"
	bs, err := json.Marshal(data)
	if err != nil {
		return err
	}
	if err := ioutil.WriteFile(tmp, bs, 0644); err != nil {
		return err
	}
	return os.Rename(tmp, f.Path)
}
