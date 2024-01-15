package core

import (
	"butler-server/internals"
	"database/sql"
	"fmt"
	"sync"
)

type MsSQLDatabase struct {
	conn   *sql.DB
	config DatabaseConfig
}

func (m *MsSQLDatabase) Connect() error {
	connectionString := fmt.Sprintf("server=%s;user id=%s;password=%s;port=%s;",
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
func (m *MsSQLDatabase) Metadata(table string) (map[string]internals.SchemaDetails, error) {
	resultCh := make(chan Result, 3)
	var wg sync.WaitGroup

	go func() {
		defer wg.Done()
		query := `
		SELECT
			fk.name AS constraint_name,
			OBJECT_NAME(fk.parent_object_id) AS table_name,
			c1.name AS column_name,
			OBJECT_NAME(fk.referenced_object_id) AS foreign_table_name,
			c2.name AS foreign_column_name
		FROM sys.foreign_keys AS fk
		JOIN sys.foreign_key_columns AS fkc
			ON fk.object_id = fkc.constraint_object_id
		JOIN sys.columns AS c1
			ON fkc.parent_object_id = c1.object_id
			AND fkc.parent_column_id = c1.column_id
		JOIN sys.columns AS c2
			ON fkc.referenced_object_id = c2.object_id
			AND fkc.referenced_column_id = c2.column_id;`
		schemaDetails, err := internals.FetchSchemaDetails(m.conn, query, table)
		if err != nil {
			resultCh <- Result{Details: nil, Error: err, Type: "schema"}
			return
		}
		resultCh <- Result{Details: schemaDetails, Error: nil, Type: "schema"}
	}()
	go func() {
		defer wg.Done()
		query := `
		SELECT
			conname AS constraint_name,
			conrelid::regclass AS table_name,
			ta.attname AS column_name,
			confrelid::regclass AS foreign_table_name,
			fa.attname AS foreign_column_name
		FROM (
			SELECT
				conname,
				conrelid,
				confrelid,
				unnest(conkey) AS conkey,
				unnest(confkey) AS confkey
			FROM pg_constraint
		) sub
		JOIN pg_attribute AS ta ON ta.attrelid = conrelid AND ta.attnum = conkey
		JOIN pg_attribute AS fa ON fa.attrelid = confrelid AND fa.attnum = confkey;`
		foreignKeyDetails, err := internals.FetchForeignKeyDetails(m.conn, query, table)
		if err != nil {
			resultCh <- Result{Details: nil, Error: err, Type: "foreign key"}
			return
		}
		resultCh <- Result{Details: foreignKeyDetails, Error: nil, Type: "foreign key"}
	}()

	go func() {
		defer wg.Done()
		query := `
		SELECT 
			i.name AS indexname,
			i.definition AS indexdef
		FROM sys.indexes i
		INNER JOIN sys.objects o ON i.object_id = o.object_id
		WHERE o.name = ?;`
		indexDetails, err := internals.FetchIndexDetails(m.conn, query, table)
		if err != nil {
			resultCh <- Result{Details: nil, Error: err, Type: "index"}
			return
		}
		resultCh <- Result{Details: indexDetails, Error: nil, Type: "index"}
	}()

	wg.Wait()

	results := make(map[string]interface{})

	for i := 0; i < 3; i++ {
		result := <-resultCh
		if result.Error != nil {
			return nil, result.Error
		}
		results[result.Type] = result.Details
	}
	schemaDetails := internals.MergeMetaData(results["schema"].(map[string]internals.SchemaDetails), results["index"].([]internals.IndexDetails), results["foreign key"].([]internals.ForeignKeyDetails))
	return schemaDetails, nil
}

func (m *MsSQLDatabase) Data(table string, filter Filter) (map[string]interface{}, error) {

	filterMap := internals.ParseFilterParam(filter.Filter)
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
