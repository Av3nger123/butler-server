package main

import (
	"butler-server/client"
	"butler-server/config"
	"butler-server/handlers"
	"butler-server/initializers"
)

func main() {
	config.LoadEnv()

	db, err := initializers.InitPostgres()
	if err != nil {
		panic(err)
	}
	redis, err := initializers.InitRedis()
	if err != nil {
		panic(err)
	}

	dbClient := client.NewDatabase(db)
	redisClient := client.NewRedisClient(redis)

	handlers.StartServer(dbClient, redisClient, config.GetString("PORT"))
}
