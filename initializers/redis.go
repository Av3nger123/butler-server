package initializers

import (
	"butler-server/config"
	"fmt"
	"log"

	"github.com/go-redis/redis"
)

var RedisClient *redis.Client

func InitRedis() (*redis.Client, error) {
	// Redis connection
	opt, _ := redis.ParseURL(config.GetString("REDIS_CONNECTION_STRING"))
	redisClient := redis.NewClient(opt)

	pong, err := redisClient.Ping().Result()
	if err != nil {
		log.Fatal("Error connecting to Redis:", err)
		return nil, err
	}
	fmt.Println("Connected to Redis! Server response:", pong)

	return redisClient, nil
}
