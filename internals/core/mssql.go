package core

import (
	"butler-server/internals"
	"database/sql"
	"fmt"
)

type MsSQLDatabase struct {
	conn   *sql.DB
	config DatabaseConfig
}

func (m *MsSQLDatabase) Connect() error {
	connectionString := fmt.Sprintf("server=%s;user id=%s;password=%s;port=%d;",
		m.config.Hostname, m.config.Username, m.config.Password, m.config.Port)
	if m.config.Database != "" {
		connectionString += "database=" + m.config.Database
	}

	db, err := sql.Open("sqlserver", connectionString)
	if err != nil {
		return err
	}

	if err = db.Ping(); err != nil {
		return err
	}

	m.conn = db
	fmt.Println("Connected to MSSQL database")
	return nil
}

func (m *MsSQLDatabase) Databases() ([]string, error) {
	rows, err := m.conn.Query("SELECT name FROM sys.databases")
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
func (m *MsSQLDatabase) Tables() ([]string, error) {
	query := fmt.Sprintf("SELECT table_name FROM information_schema.tables WHERE table_type = 'BASE TABLE' AND table_catalog = '%s'", m.config.Database)
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
func (m *MsSQLDatabase) Metadata() (map[string]interface{}, error) {
	return nil, nil
}

func (m *MsSQLDatabase) Data(table string, filter Filter) (map[string]interface{}, error) {

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

func (m *MsSQLDatabase) Query() ([]interface{}, error) {
	return nil, nil
}

func (m *MsSQLDatabase) Close() error {
	if m.conn != nil {
		if err := m.conn.Close(); err != nil {
			return err
		}
		fmt.Println("Closed MsSQL database connection")
	}
	return nil
}
