package core

import (
	"butler-server/internals"
	"database/sql"
	"fmt"
)

type PostgreSQLDatabase struct {
	conn   *sql.DB
	config DatabaseConfig
}

func (p *PostgreSQLDatabase) Connect() error {

	connectionString := fmt.Sprintf("host=%s port=%d user=%s password=%s ",
		p.config.Hostname, p.config.Port, p.config.Username, p.config.Password)
	if p.config.Database != "" {
		connectionString += "dbname=" + p.config.Database
	}
	connectionString += " sslmode=disable"

	db, err := sql.Open("postgres", connectionString)
	if err != nil {
		return err
	}

	if err = db.Ping(); err != nil {
		return err
	}
	p.conn = db
	fmt.Println("Connected to PostgreSQL database")
	return nil
}

func (m *PostgreSQLDatabase) Databases() ([]string, error) {
	databaseQuery := "SELECT datname FROM pg_database"
	rows, err := m.conn.Query(databaseQuery)
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
func (m *PostgreSQLDatabase) Tables() ([]string, error) {
	query := fmt.Sprintf("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' AND table_catalog = '%s'", m.config.Database)
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
func (m *PostgreSQLDatabase) Metadata() (map[string]interface{}, error) {
	return nil, nil
}

func (m *PostgreSQLDatabase) Data(table string, filter Filter) (map[string]interface{}, error) {

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

func (p *PostgreSQLDatabase) Query() ([]interface{}, error) {
	return nil, nil
}

func (p *PostgreSQLDatabase) Close() error {
	if p.conn != nil {
		if err := p.conn.Close(); err != nil {
			return err
		}
		fmt.Println("Closed PostgreSQL database connection")
	}
	return nil
}
