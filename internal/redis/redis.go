package redis

import (
	"github.com/redis/go-redis/v9"
	"github.com/vysogota0399/gophermart_accural_adapter/internal/config"
)

type Redis struct {
	Client *redis.Client
}

func NewRedis(cfg *config.Config) (*Redis, error) {
	opts, err := redis.ParseURL(cfg.RedisDSN)
	if err != nil {
		return nil, err
	}
	return &Redis{Client: redis.NewClient(opts)}, nil
}
