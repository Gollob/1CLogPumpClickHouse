package storage

import (
	"1CLogPumpClickHouse/config"
	"context"
	"fmt"
	"github.com/redis/go-redis/v9"
)

type RedisStore struct {
	client *redis.Client
	key    string
}

func NewRedisStore(cfg *config.RedisConfig, key string) *RedisStore {
	rdb := redis.NewClient(&redis.Options{
		Addr:     fmt.Sprintf("%s:%d", cfg.Host, cfg.Port),
		Password: cfg.Password,
		DB:       int(cfg.DB),
	})
	return &RedisStore{
		client: rdb,
		key:    key,
	}
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
