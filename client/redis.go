package client

import (
	"butler-server/initializers"

	"github.com/go-redis/redis"
)

// RedisClient struct represents a Redis client
type RedisClient struct {
	client *redis.Client
}

// NewRedisClient creates a new RedisClient instance
func NewRedisClient() *RedisClient {
	return &RedisClient{
		client: initializers.RedisClient,
	}
}

// SetString sets a key-value pair in Redis
func (r *RedisClient) SetString(key, value string) error {
	return r.client.Set(key, value, 0).Err()
}

// GetString gets the value associated with a key in Redis
func (r *RedisClient) GetString(key string) (string, error) {
	return r.client.Get(key).Result()
}
