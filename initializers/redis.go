package initializers

import (
	"butler-server/config"
	"fmt"
	"log"

	"github.com/go-redis/redis"
)

var RedisClient *redis.Client

func InitRedis() {
	// Redis connection
	redisClient := redis.NewClient(&redis.Options{
		Addr:     config.GetString("REDIS_ADDR"),
		Password: config.GetString("REDIS_PASSWORD"),
		DB:       0,
	})

	pong, err := redisClient.Ping().Result()
	if err != nil {
		log.Fatal("Error connecting to Redis:", err)
	}
	fmt.Println("Connected to Redis! Server response:", pong)

	RedisClient = redisClient
}
