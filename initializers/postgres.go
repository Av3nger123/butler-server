package initializers

import (
	"butler-server/config"
	"database/sql"
	"log"

	_ "github.com/lib/pq"
)

func InitPostgres() (*sql.DB, error) {

	// PostgreSQL connection
	db, err := sql.Open("postgres", config.GetString("POSTGRES_CONNECTION_STRING")+"?sslmode=disable")
	if err != nil {
		log.Fatal(err)
	}

	err = db.Ping()
	if err != nil {
		log.Fatal("Error connecting to PostgreSQL:", err)
		return nil, err
	}

	return db, nil
}
