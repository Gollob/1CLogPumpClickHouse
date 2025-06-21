package config

import (
	"bytes"
	"os"

	"gopkg.in/yaml.v2"
)

// ClickHouseConfig содержит настройки подключения и маппинг таблиц по Component
// Загружается из YAML
type ClickHouseConfig struct {
	Address      string            `yaml:"Address"`
	Username     string            `yaml:"Username"`
	Password     string            `yaml:"Password"`
	Database     string            `yaml:"Database"`
	DefaultTable string            `yaml:"DefaultTable"`
	Protocol     string            `yaml:"Protocol"`
	TableMap     map[string]string `yaml:"TableMap"`
}

// Config описывает основные настройки сервиса
type Config struct {
	LogDirectoryMap map[string]string `yaml:"LogDirectoryMap"`
	FilePattern     string            `yaml:"FilePattern"`
	BatchSize       int               `yaml:"BatchSize"`
	BatchInterval   int               `yaml:"BatchInterval"`
	ClickHouse      ClickHouseConfig  `yaml:"ClickHouse"`
}

// LoadConfig загружает конфигурацию из YAML-файла.
// Поддерживает файлы с BOM и игнорирует неизвестные поля.
func LoadConfig(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	// Удаляем UTF-8 BOM, если есть
	data = bytes.TrimPrefix(data, []byte{0xEF, 0xBB, 0xBF})
	// Заменяем табуляции на два пробела, чтобы YAML-парсер не жаловался
	data = bytes.ReplaceAll(data, []byte("\t"), []byte("  "))

	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, err
	}
	return &cfg, nil
}
