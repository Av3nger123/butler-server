package initializers

import (
	"butler-server/config"
	"database/sql"
	"log"

	_ "github.com/lib/pq"
)

var Db *sql.DB

func InitPostgres() {

	// PostgreSQL connection
	db, err := sql.Open("postgres", config.GetString("POSTGRES_CONNECTION_STRING"))
	if err != nil {
		log.Fatal(err)
	}

	err = db.Ping()
	if err != nil {
		log.Fatal("Error connecting to PostgreSQL:", err)
	}

	Db = db
}
