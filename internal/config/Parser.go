package config

import (
	"bytes"
	"fmt"
	"gopkg.in/yaml.v3"
	"os"
)

// readFile читает все байты из файла по пути
func readFile(path string) ([]byte, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	return data, nil
}

// sanitize удаляет BOM и табуляции
func sanitize(data []byte) []byte {
	// Удаляем UTF-8 BOM
	data = bytes.TrimPrefix(data, []byte{0xEF, 0xBB, 0xBF})
	// Заменяем табы на два пробела
	data = bytes.ReplaceAll(data, []byte("\t"), []byte("  "))
	return data
}

// parseYAML парсит YAML-данные в структуру Config
func parseYAML(data []byte) (*Config, error) {
	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, err
	}
	return &cfg, nil
}

// Validate проверяет обязательные поля конфигурации
func (c *Config) Validate() error {
	if len(c.LogDirectoryMap) == 0 {
		return fmt.Errorf("LogDirectoryMap must not be empty")
	}
	if c.FilePattern == "" {
		return fmt.Errorf("FilePattern must not be empty")
	}
	if c.BatchSize <= 0 {
		return fmt.Errorf("BatchSize must be positive")
	}
	if c.BatchInterval <= 0 {
		return fmt.Errorf("BatchInterval must be positive")
	}
	if c.ClickHouse.Address == "" {
		return fmt.Errorf("ClickHouse.Address must not be empty")
	}
	if c.ClickHouse.Database == "" {
		return fmt.Errorf("ClickHouse.Database must not be empty")
	}
	return nil
}
