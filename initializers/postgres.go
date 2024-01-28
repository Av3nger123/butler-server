package initializers

import (
	"butler-server/config"
	"log"

	"github.com/jinzhu/gorm"
	_ "github.com/jinzhu/gorm/dialects/postgres"
)

func InitPostgres() (*gorm.DB, error) {
	// PostgreSQL connection
	db, err := gorm.Open("postgres", config.GetString("POSTGRES_CONNECTION_STRING")+"?sslmode=disable")
	if err != nil {
		log.Fatal("Error connecting to PostgreSQL:", err)
		return nil, err
	}

	// Enable verbose logging (optional)
	db.LogMode(true)

	return db, nil
}
