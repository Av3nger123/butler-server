package core

import (
	"butler-server/internals"
	"database/sql"
	"fmt"
	"sync"
)

type MariaDatabase struct {
	conn   *sql.DB
	config DatabaseConfig
}

func (this *MariaDatabase) Connect() error {
	connectionString := fmt.Sprintf("%s:%s@tcp(%s:%s)",
		this.config.Username, this.config.Password, this.config.Hostname, this.config.Port)
	if this.config.Database != "" {
		connectionString += "/" + this.config.Database
	}

	db, err := sql.Open("mysql", connectionString)
	if err != nil {
		return err
	}

	if err = db.Ping(); err != nil {
		return err
	}

	this.conn = db
	fmt.Println("Connected to MariaDB database")
	return nil
}

func (this *MariaDatabase) Databases() ([]string, error) {
	rows, err := this.conn.Query("SHOW DATABASES")
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
func (this *MariaDatabase) Tables() ([]string, error) {
	query := fmt.Sprintf("SHOW TABLES FROM %s", this.config.Database)
	rows, err := this.conn.Query(query)
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
func (this *MariaDatabase) Metadata(table string) (map[string]internals.SchemaDetails, error) {
	resultCh := make(chan Result, 3)
	var wg sync.WaitGroup

	go func() {
		defer wg.Done()
		query := `
		SELECT 
			column_name, 
			data_type, 
			character_maximum_length, 
			is_nullable, 
			column_default, 
			udt_name AS data_type_name, 
			ordinal_position
		FROM information_schema.COLUMNS
		WHERE table_name = ?;`
		schemaDetails, err := internals.FetchSchemaDetails(this.conn, query, table)
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
			tc.CONSTRAINT_NAME AS constraint_name,
			tc.TABLE_NAME AS table_name,
			kcu.COLUMN_NAME AS column_name,
			ccu.TABLE_NAME AS foreign_table_name,
			ccu.COLUMN_NAME AS foreign_column_name
		FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS AS tc
		JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE AS kcu
			ON tc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME
			AND tc.TABLE_SCHEMA = kcu.TABLE_SCHEMA
		JOIN INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS
		
		AS rc
			ON tc.CONSTRAINT_NAME = rc.CONSTRAINT_NAME
			AND tc.CONSTRAINT_SCHEMA = rc.CONSTRAINT_SCHEMA
		JOIN INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE AS ccu
			ON rc.UNIQUE_CONSTRAINT_NAME = ccu.CONSTRAINT_NAME
			AND rc.CONSTRAINT_SCHEMA = ccu.CONSTRAINT_SCHEMA
		WHERE tc.CONSTRAINT_TYPE = 'FOREIGN KEY';`
		foreignKeyDetails, err := internals.FetchForeignKeyDetails(this.conn, query, table)
		if err != nil {
			resultCh <- Result{Details: nil, Error: err, Type: "foreign key"}
			return
		}
		resultCh <- Result{Details: foreignKeyDetails, Error: nil, Type: "foreign key"}
	}()

	go func() {
		defer wg.Done()
		query := `
		SELECT index_name AS indexname, index_definition AS indexdef
		FROM information_schema.STATISTICS
		WHERE table_name = ?;`
		indexDetails, err := internals.FetchIndexDetails(this.conn, query, table)
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

func (this *MariaDatabase) Data(table string, filter Filter) (map[string]interface{}, error) {

	filterMap := internals.ParseFilterParam(filter.Filter)
	query, err := ParseSQLQuery(table, filter, filterMap)
	if err != nil {
		return nil, err
	}

	rows, err := this.conn.Query(query, internals.FilterValues(filterMap)...)
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

func (this *MariaDatabase) Query(query string, page int, size int) ([]map[string]interface{}, error) {
	return nil, nil
}

func (this *MariaDatabase) Close() error {
	if this.conn != nil {
		if err := this.conn.Close(); err != nil {
			return err
		}
		fmt.Println("Closed Maria database connection")
	}
	return nil
}

func (this *MariaDatabase) Execute(queries []string) error {
	return nil
}
