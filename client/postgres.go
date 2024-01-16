// client/client.go
package client

import (
	"butler-server/internals"
	"database/sql"
)

// Database struct represents a PostgreSQL client
type Database struct {
	db *sql.DB
}

// NewDatabase creates a new Database instance
func NewDatabase(db *sql.DB) *Database {
	return &Database{
		db: db,
	}
}

// QueryToJSON executes a SQL query and returns the result as JSON
func (d *Database) Execute(query string) ([]map[string]interface{}, error) {
	rows, err := d.db.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	result, _, err := internals.ParseRows(rows)
	if err != nil {
		return nil, err
	}

	return result, nil
}
