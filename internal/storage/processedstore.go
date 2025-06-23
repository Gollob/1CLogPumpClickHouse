package storage

// ProcessedStore — интерфейс для загрузки/сохранения списка обработанных файлов.
type ProcessedStore interface {
	Load() (map[string]int64, error)
	Save(data map[string]int64) error
}
