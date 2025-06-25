package config

import (
	"bytes"
	"fmt"
	"gopkg.in/yaml.v2"
	"os"
)

// ClickHouseConfig содержит настройки подключения и маппинг таблиц по компонентам
// Загружается из YAML
// Поля обязательны: Address, Database
// TableMap может быть пустым
type ClickHouseConfig struct {
	Address      string            `yaml:"Address"`
	Username     string            `yaml:"Username"`
	Password     string            `yaml:"Password"`
	Database     string            `yaml:"Database"`
	DefaultTable string            `yaml:"DefaultTable"`
	Protocol     string            `yaml:"Protocol"`
	TableMap     map[string]string `yaml:"TableMap"`
}

// RedisConfig содержит настройки подключения к Redis
type RedisConfig struct {
	Host     string `yaml:"Host"`
	Port     int    `yaml:"Port"`
	DB       int    `yaml:"DB"`
	Password string `yaml:"Password"`
}

// LoggingConfig содержит настройки логирования и интеграции с Sentry
type LoggingConfig struct {
	LogFile      string `yaml:"LogFile"`      // путь к файлу логов
	SentryDSN    string `yaml:"SentryDSN"`    // DSN для Sentry
	EnableSentry bool   `yaml:"EnableSentry"` // включить отправку ошибок в Sentry
}

// Config описывает основные настройки сервиса
// LogDirectoryMap и FilePattern обязательны
// BatchSize и BatchInterval должны быть положительными
// ClickHouse — вложенная конфигурация
// Загружается из YAML
// Пример конфигурации см. README.md

type Config struct {
	LogDirectoryMap map[string]string `yaml:"LogDirectoryMap"`
	FilePattern     string            `yaml:"FilePattern"`
	BatchSize       int               `yaml:"BatchSize"`
	BatchInterval   int               `yaml:"BatchInterval"`

	ClickHouse       ClickHouseConfig `yaml:"ClickHouse"`
	ProcessedStorage string           `yaml:"ProcessedStorage"` // "file" или "redis"
	Redis            RedisConfig      `yaml:"Redis"`
	Logging          LoggingConfig    `yaml:"Logging"`
}

// LoadConfig читает и парсит конфиг из YAML-файла по указанному пути.
// Шаги:
// 1. Чтение сырого файла
// 2. Очистка данных: удаление BOM, замена табуляций
// 3. Парсинг YAML в структуру Config
// 4. Валидация обязательных полей
func LoadConfig(path string) (*Config, error) {
	// 1. Чтение
	raw, err := readFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config: %w", err)
	}

	// 2. Очистка
	sanitized := sanitize(raw)

	// 3. Парсинг
	cfg, err := parseYAML(sanitized)
	if err != nil {
		return nil, fmt.Errorf("unmarshal yaml: %w", err)
	}

	// 4. Валидация
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("validate config: %w", err)
	}
	return cfg, nil
}

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
