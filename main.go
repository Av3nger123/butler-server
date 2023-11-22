package main

import (
	"butler-server/client"
	"butler-server/config"
	"butler-server/handlers"
	"butler-server/initializers"
)

func main() {
	config.LoadEnv()

	initializers.InitPostgres()
	initializers.InitRedis()

	dbClient := client.NewDatabase()
	redisClient := client.NewRedisClient()

	handlers.StartServer(dbClient, redisClient, config.GetString("PORT"))
}
