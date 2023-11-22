// client/client.go
package client

import (
	"butler-server/initializers"
	"database/sql"
)

// Database struct represents a PostgreSQL client
type Database struct {
	db *sql.DB
}

// NewDatabase creates a new Database instance
func NewDatabase() *Database {
	return &Database{
		db: initializers.Db,
	}
}

// QueryToJSON executes a SQL query and returns the result as JSON
func (d *Database) Execute(query string) ([]map[string]interface{}, error) {
	rows, err := d.db.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	result, err := RowsToJSON(rows)
	if err != nil {
		return nil, err
	}

	return result, nil
}

// RowsToJSON converts database rows to JSON
func RowsToJSON(rows *sql.Rows) ([]map[string]interface{}, error) {
	columns, err := rows.Columns()
	if err != nil {
		return nil, err
	}
	var result []map[string]interface{}
	values := make([]interface{}, len(columns))
	for rows.Next() {

		for i := range values {
			values[i] = new(interface{})
		}
		err := rows.Scan(values...)
		if err != nil {
			return nil, err
		}
		rowData := make(map[string]interface{})
		for i, column := range columns {
			rowData[column] = *values[i].(*interface{})
		}
		result = append(result, rowData)
	}
	return result, nil
}
