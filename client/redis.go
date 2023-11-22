package client

import (
	"butler-server/initializers"
	"encoding/json"
	"fmt"
	"time"

	"github.com/go-redis/redis"
)

// RedisClient struct represents a Redis client
type RedisClient struct {
	client *redis.Client
}

const (
	// KeyPrefixDatabase is the prefix for database keys
	KeyPrefixDatabase = "Database"

	// KeyPrefixTables is the prefix for tables keys
	KeyPrefixTables = "Tables"

	// KeyPrefixMetadata is the prefix for metadata keys
	KeyPrefixMetadata = "Metadata"
)

// NewRedisClient creates a new RedisClient instance
func NewRedisClient() *RedisClient {
	return &RedisClient{
		client: initializers.RedisClient,
	}
}

// SetString sets a key-value pair in Redis
func (r *RedisClient) SetString(key, value string, ttl time.Duration) error {
	return r.client.Set(key, value, ttl).Err()
}

// GetString gets the value associated with a key in Redis
func (r *RedisClient) GetString(key string) (string, error) {
	return r.client.Get(key).Result()
}

// SetString sets a key-value pair in Redis
func (r *RedisClient) SetMap(key string, value map[string]interface{}, ttl time.Duration) error {
	jsonData, err := json.Marshal(value)
	if err != nil {
		return err
	}
	return r.client.Set(key, jsonData, ttl).Err()
}

// GetMap gets the value associated with a key in Redis of Type Map
func (r *RedisClient) GetMap(key string) (map[string]interface{}, error) {
	jsonData, err := r.client.Get(key).Result()
	if err != nil {
		return nil, err
	}
	var data map[string]interface{}
	err = json.Unmarshal([]byte(jsonData), &data)
	if err != nil {
		return nil, err
	}
	return data, nil
}

// SetString sets a key-value pair in Redis
func (r *RedisClient) SetSlice(key string, value []map[string]interface{}, ttl time.Duration) error {
	jsonData, err := json.Marshal(value)
	if err != nil {
		return err
	}
	return r.client.Set(key, jsonData, ttl).Err()
}

// GetString gets the value associated with a key in Redis
func (r *RedisClient) GetSlice(key string) ([]map[string]interface{}, error) {
	result, err := r.client.Get(key).Result()
	if err != nil {
		return nil, err
	}
	var data []map[string]interface{}
	err = json.Unmarshal([]byte(result), &data)
	if err != nil {
		return nil, err
	}
	return data, nil
}

func (r *RedisClient) GenerateDatabaseKey(clusterID string) string {
	return fmt.Sprintf("%s:%s", KeyPrefixDatabase, clusterID)
}

func (r *RedisClient) GenerateTablesKey(clusterID, databaseName string) string {
	return fmt.Sprintf("%s:%s~%s", KeyPrefixTables, clusterID, databaseName)
}

func (r *RedisClient) GenerateMetadataKey(clusterID, databaseName, tableName string) string {
	return fmt.Sprintf("%s:%s~%s~%s", KeyPrefixMetadata, clusterID, databaseName, tableName)
}
