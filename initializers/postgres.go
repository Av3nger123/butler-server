package initializers

import (
	"butler-server/config"
	"log"

	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

func InitPostgres() (*gorm.DB, error) {
	db, err := gorm.Open(postgres.Open(config.GetString("POSTGRES_CONNECTION_STRING")+"?sslmode=disable"), &gorm.Config{})
	if err != nil {
		log.Fatal("Error connecting to PostgreSQL:", err)
		return nil, err
	}

	return db, nil
}
