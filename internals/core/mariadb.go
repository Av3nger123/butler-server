package core

import (
	"butler-server/internals"
	"database/sql"
	"fmt"
)

type MariaDatabase struct {
	conn   *sql.DB
	config DatabaseConfig
}

func (m *MariaDatabase) Connect() error {
	connectionString := fmt.Sprintf("%s:%s@tcp(%s:%d)",
		m.config.Username, m.config.Password, m.config.Hostname, m.config.Port)
	if m.config.Database != "" {
		connectionString += "/" + m.config.Database
	}

	db, err := sql.Open("mysql", connectionString)
	if err != nil {
		return err
	}

	if err = db.Ping(); err != nil {
		return err
	}

	m.conn = db
	fmt.Println("Connected to MariaDB database")
	return nil
}

func (m *MariaDatabase) Databases() ([]string, error) {
	rows, err := m.conn.Query("SHOW DATABASES")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var databases []string
	for rows.Next() {
		var dbName string
		err := rows.Scan(&dbName)
		if err != nil {
			return nil, err
		}
		databases = append(databases, dbName)
	}

	return databases, nil
}
func (m *MariaDatabase) Tables() ([]string, error) {
	query := fmt.Sprintf("SHOW TABLES FROM %s", m.config.Database)
	rows, err := m.conn.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var tables []string
	for rows.Next() {
		var tableName string
		err := rows.Scan(&tableName)
		if err != nil {
			return nil, err
		}
		tables = append(tables, tableName)
	}

	return tables, nil
}
func (m *MariaDatabase) Metadata() (map[string]interface{}, error) {
	return nil, nil
}

func (m *MariaDatabase) Data(table string, filter Filter) (map[string]interface{}, error) {

	filterMap := internals.ParseFilterParam(filter.filter)
	query, err := ParseSQLQuery(table, filter, filterMap)
	if err != nil {
		return nil, err
	}

	rows, err := m.conn.Query(query, internals.FilterValues(filterMap)...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	result, count, err := internals.ParseRows(rows)
	if err != nil {
		return nil, err
	}

	dbMap := make(map[string]interface{})
	dbMap["data"] = result
	dbMap["count"] = count
	return dbMap, nil

}

func (m *MariaDatabase) Query() ([]interface{}, error) {
	return nil, nil
}

func (m *MariaDatabase) Close() error {
	if m.conn != nil {
		if err := m.conn.Close(); err != nil {
			return err
		}
		fmt.Println("Closed Maria database connection")
	}
	return nil
}
