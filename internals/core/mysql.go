package core

import (
	"butler-server/internals"
	"database/sql"
	"fmt"
	"strconv"
	"strings"
	"sync"

	_ "github.com/go-sql-driver/mysql"
)

type MySQLDatabase struct {
	conn   *sql.DB
	config DatabaseConfig
}

func (this *MySQLDatabase) Connect() error {
	connectionString := fmt.Sprintf("%s:%s@tcp(%s:%s)/",
		this.config.Username, this.config.Password, this.config.Hostname, this.config.Port)
	if this.config.Database != "" {
		connectionString += this.config.Database
	}
	db, err := sql.Open("mysql", connectionString)
	if err != nil {
		return err
	}

	if err = db.Ping(); err != nil {
		return err
	}

	this.conn = db
	fmt.Println("Connected to MySQL database")
	return nil
}

func (this *MySQLDatabase) Databases() ([]string, error) {
	databaseQuery := "SHOW DATABASES"
	rows, err := this.conn.Query(databaseQuery)
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
func (this *MySQLDatabase) Tables() ([]string, error) {
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
func (this *MySQLDatabase) Metadata(table string) (map[string]internals.SchemaDetails, error) {
	resultCh := make(chan Result, 3)
	var wg sync.WaitGroup

	wg.Add(3)

	go func() {
		defer wg.Done()
		schemaDetails, err := this.fetchSchemaDetails(table)
		if err != nil {
			resultCh <- Result{Details: nil, Error: err, Type: "schema"}
			return
		}
		resultCh <- Result{Details: schemaDetails, Error: nil, Type: "schema"}
	}()
	go func() {
		defer wg.Done()
		foreignKeyDetails, err := this.fetchForeignKeyDetails(table)
		if err != nil {
			resultCh <- Result{Details: nil, Error: err, Type: "fk"}
			return
		}
		resultCh <- Result{Details: foreignKeyDetails, Error: nil, Type: "fk"}
	}()

	go func() {
		defer wg.Done()
		indexes, err := this.fetchIndexDetails(table)
		if err != nil {
			resultCh <- Result{Details: nil, Error: err, Type: "index"}
			return
		}
		resultCh <- Result{Details: indexes, Error: nil, Type: "index"}
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
	schemaDetails := internals.MergeMetaData(results["schema"].(map[string]internals.SchemaDetails), results["index"].([]internals.IndexDetails), results["fk"].([]internals.ForeignKeyDetails))
	return schemaDetails, nil
}

func (this *MySQLDatabase) Data(table string, filter Filter) (map[string]interface{}, error) {

	filterMap := internals.ParseFilterParam(filter.Filter)
	query, err := this.parseSQLQuery(table, filter, filterMap)
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

func (this *MySQLDatabase) Query(query string, page int, size int) ([]map[string]interface{}, error) {
	rows, err := this.conn.Query(query)
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

func (this *MySQLDatabase) Close() error {
	if this.conn != nil {
		if err := this.conn.Close(); err != nil {
			return err
		}
		fmt.Println("Closed MySQL database connection")
	}
	return nil
}

func (this *MySQLDatabase) parseSQLQuery(table string, filter Filter, filterMap map[string]string) (string, error) {
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

	query := fmt.Sprintf(`SELECT *, (SELECT COUNT(*) FROM %s) as total_count FROM %s`, table, table)
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

func (this *MySQLDatabase) fetchSchemaDetails(table string) (map[string]internals.SchemaDetails, error) {
	query := fmt.Sprintf(`
		SELECT ordinal_position as ordinal_position,
			column_name as column_name,
			column_type AS data_type,
			is_nullable as is_nullable,
			column_default as column_default
		FROM information_schema.columns
		WHERE table_schema='%s' AND table_name='%s';
	`, this.config.Database, table)
	rows, err := this.conn.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	schemaDetails := make(map[string]internals.SchemaDetails)
	for rows.Next() {
		var columnName, dataType, isNullable, ordinalPosition string
		var columnDefault sql.NullString

		err := rows.Scan(&ordinalPosition, &columnName, &dataType, &isNullable, &columnDefault)
		if err != nil {
			return nil, err
		}
		columnDetails := internals.SchemaDetails{
			DataType:      dataType,
			IsNullable:    isNullable,
			Position:      ordinalPosition,
			ColumnDefault: columnDefault,
		}
		schemaDetails[columnName] = columnDetails
	}

	return schemaDetails, nil
}

func (this *MySQLDatabase) fetchIndexDetails(table string) ([]internals.IndexDetails, error) {
	query := fmt.Sprintf(`
		SELECT index_name as index_name, index_type AS index_algorithm,
		CASE non_unique WHEN 0 THEN'TRUE'ELSE'FALSE'END AS is_unique,
		column_name as column_name FROM information_schema.statistics 
		WHERE table_schema='%s' AND table_name='%s' ORDER BY seq_in_index ASC;
	`, this.config.Database, table)
	rows, err := this.conn.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var indexes []internals.IndexDetails
	for rows.Next() {
		var indexName, unique, indexAlgorithm, columnName string
		err := rows.Scan(&indexName, &indexAlgorithm, &unique, &columnName)
		if err != nil {
			return nil, err
		}
		isUnique := false
		if unique == "TRUE" {
			isUnique = true
		}
		indexDetails := internals.IndexDetails{
			IndexName:      indexName,
			IndexAlgorithm: indexAlgorithm,
			IsUnique:       isUnique,
			ColumnName:     columnName,
		}

		indexes = append(indexes, indexDetails)
	}

	return indexes, nil
}

func (this *MySQLDatabase) fetchForeignKeyDetails(table string) ([]internals.ForeignKeyDetails, error) {
	query := fmt.Sprintf(`
		SELECT constraint_name,referenced_table_name,referenced_column_name,
		column_name FROM information_schema.key_column_usage WHERE 
		table_name='%s' AND table_schema='%s' AND referenced_column_name is not NULL;
	`, table, this.config.Database)
	rows, err := this.conn.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var foreignKeys []internals.ForeignKeyDetails
	for rows.Next() {
		var (
			constraintName    string
			columnName        string
			foreignTableName  string
			foreignColumnName string
		)

		err := rows.Scan(&constraintName, &foreignTableName, &foreignColumnName, &columnName)
		if err != nil {
			return nil, err
		}
		result := internals.ForeignKeyDetails{
			ConstraintName:    constraintName,
			TableName:         table,
			ColumnName:        columnName,
			ForeignTableName:  foreignTableName,
			ForeignColumnName: foreignColumnName,
		}
		foreignKeys = append(foreignKeys, result)
	}
	return foreignKeys, nil
}

func (this *MySQLDatabase) Execute(queries []string) error {
	return nil
}
