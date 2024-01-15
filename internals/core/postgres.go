package core

import (
	"butler-server/internals"
	"database/sql"
	"fmt"
	"strconv"
	"strings"
	"sync"

	_ "github.com/lib/pq"
)

type PostgreSQLDatabase struct {
	conn   *sql.DB
	config DatabaseConfig
}

func (p *PostgreSQLDatabase) Connect() error {
	connStr := fmt.Sprintf("host=%s port=%s user=%s password=%s", p.config.Hostname, p.config.Port, p.config.Username, p.config.Password)
	if p.config.Database != "" {
		connStr += " dbname=" + p.config.Database
	} else {
		connStr += " dbname=postgres"
	}
	connStr += " sslmode=disable"
	db, err := sql.Open("postgres", connStr)
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
func (p *PostgreSQLDatabase) Metadata(table string) (map[string]internals.SchemaDetails, error) {
	resultCh := make(chan Result, 3)
	var wg sync.WaitGroup
	wg.Add(3)
	go func() {
		defer wg.Done()
		query := `
		SELECT column_name, data_type, character_maximum_length, is_nullable, column_default, udt_name, ordinal_position 
		FROM information_schema.columns 
		WHERE table_name = $1;`
		schemaDetails, err := internals.FetchSchemaDetails(p.conn, query, table)
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
		foreignKeyDetails, err := internals.FetchForeignKeyDetails(p.conn, query, table)
		if err != nil {
			resultCh <- Result{Details: nil, Error: err, Type: "foreign key"}
			return
		}
		resultCh <- Result{Details: foreignKeyDetails, Error: nil, Type: "foreign key"}
	}()

	go func() {
		defer wg.Done()
		query := `
		SELECT indexname, indexdef
        FROM pg_indexes
        WHERE tablename = $1;`
		indexDetails, err := internals.FetchIndexDetails(p.conn, query, table)
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

func (m *PostgreSQLDatabase) Data(table string, filter Filter) (map[string]interface{}, error) {

	filterMap := internals.ParseFilterParam(filter.Filter)
	query, err := m.parseSQLQuery(table, filter, filterMap)
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

func (m *PostgreSQLDatabase) parseSQLQuery(table string, filter Filter, filterMap map[string]string) (string, error) {
	page, err := strconv.Atoi(filter.Page)
	if err != nil {
		return "", nil
	}
	size, err := strconv.Atoi(filter.Size)
	if err != nil {
		return "", nil
	}
	if filter.Order != "asc" && filter.Order != "desc" {
		return "", fmt.Errorf("invalid order parameter")
	}
	offset := (page) * size

	query := fmt.Sprintf(`SELECT *, COUNT(*) OVER() as total_count FROM "%s"`, table)
	var operator string
	if filter.Operator == "and" {
		operator = "AND"
	} else if filter.Operator == "or" {
		operator = "OR"
	}

	if len(filterMap) > 0 {
		whereClauses := make([]string, 0)
		for key, value := range filterMap {
			operator, conditionValue := internals.ParseOperatorAndValue(value)
			whereClauses = append(whereClauses, internals.ConstructCondition(key, operator, conditionValue, whereClauses))
		}
		if operator != "" {
			query += " WHERE " + strings.Join(whereClauses, " "+operator+" ")
		} else {
			query += " WHERE " + whereClauses[0]
		}
	}
	if filter.Sort != "" {
		query += fmt.Sprintf(" ORDER BY %s %s", filter.Sort, filter.Order)
	}
	query += fmt.Sprintf(" LIMIT %d OFFSET %d;", size, offset)

	return query, nil
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
