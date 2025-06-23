package storage

import (
	"1CLogPumpClickHouse/internal/config"
	"context"
	"fmt"
	"github.com/redis/go-redis/v9"
	"time"
)

type RedisStore struct {
	client *redis.Client
	key    string
}

func NewRedisStore(cfg *config.RedisConfig, key string) (*RedisStore, error) {
	// Создаём клиента Redis
	rdb := redis.NewClient(&redis.Options{
		Addr:     fmt.Sprintf("%s:%d", cfg.Host, cfg.Port),
		Password: cfg.Password,
		DB:       cfg.DB,
	})
	// Проверяем подключение с тайм-аутом
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := rdb.Ping(ctx).Err(); err != nil {
		// Соединение не установлено - возвращаем ошибку
		return nil, fmt.Errorf("не удалось подключиться к Redis: %w", err)
	}
	return &RedisStore{client: rdb, key: key}, nil
}

func (r *RedisStore) Load() (map[string]int64, error) {
	ctx := context.Background()
	members, err := r.client.SMembers(ctx, r.key).Result()
	if err != nil {
		return nil, err
	}
	processed := make(map[string]int64, len(members))
	for _, m := range members {
		processed[m] = 0
	}
	return processed, nil
}

func (r *RedisStore) Save(data map[string]int64) error {
	ctx := context.Background()
	for filename := range data {
		if err := r.client.SAdd(ctx, r.key, filename).Err(); err != nil {
			return err
		}
	}
	return nil
}
