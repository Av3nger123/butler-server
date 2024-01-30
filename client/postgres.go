package client

import (
	"gorm.io/gorm"
)

// Database struct represents a PostgreSQL client
type Database struct {
	Db *gorm.DB
}

// NewDatabase creates a new Database instance
func NewDatabase(db *gorm.DB) *Database {
	return &Database{
		Db: db,
	}
}

// QueryToJSON executes a SQL query and returns the result as JSON
func (d *Database) Execute(query string, args ...interface{}) ([]map[string]interface{}, error) {
	var results []map[string]interface{}

	// Execute the raw SQL query
	err := d.Db.Raw(query, args...).Scan(&results).Error
	if err != nil {
		return nil, err
	}
	return results, nil
}
