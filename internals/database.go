package internals

import (
	"database/sql"
	"fmt"
	"regexp"
	"strings"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"
)

type SchemaDetails struct {
	DataType      string         `json:"dataType"`
	MaxLength     sql.NullInt64  `json:"maxLength"`
	IsNullable    string         `json:"isNullable"`
	Position      string         `json:"position"`
	ColumnDefault sql.NullString `json:"columnDefault"`
	Index         bool           `json:"index"`
	IsPrimary     bool           `json:"isPrimary"`
	ForeignKey    string         `json:"foreignKey"`
}

type IndexDetails struct {
	IndexName      string `json:"indexName"`
	IndexDef       string `json:"indexDef"`
	IndexAlgorithm string `json:"indexAlgorithm"`
	IsUnique       bool   `json:"isUnique"`
	ColumnName     string `json:"columnName"`
}

type ForeignKeyDetails struct {
	ConstraintName    string `json:"constraintName"`
	TableName         string `json:"tableName"`
	ColumnName        string `json:"columnName"`
	ForeignTableName  string `json:"foreignTableName"`
	ForeignColumnName string `json:"foreignColumnName"`
}

type QueryRequest struct {
	Id        int8   `json:"id"`
	Driver    string `json:"type"`
	Username  string `json:"username"`
	Password  string `json:"password"`
	Host      string `json:"host"`
	Port      int    `json:"port"`
	DbName    string `json:"database"`
	Query     string `json:"query"`
	TableName string `json:"table"`
}

func ConnectToDB(driverName, username, password, host, port, dbName string) (*sql.DB, error) {
	var dataSourceName string

	switch driverName {
	case "postgres":
		dataSourceName = fmt.Sprintf("postgres://%s:%s@%s:%s/%s?sslmode=disable", username, password, host, port, dbName)
	case "mysql":
		dataSourceName = fmt.Sprintf("%s:%s@tcp(%s:%s)/%s", username, password, host, port, dbName)
	default:
		return nil, fmt.Errorf("unsupported database driver: %s", driverName)
	}

	db, err := sql.Open(driverName, dataSourceName)
	if err != nil {
		return nil, err
	}

	err = db.Ping()
	if err != nil {
		return nil, err
	}

	return db, nil
}

func ExecuteQuery(db *sql.DB, query string) (*sql.Rows, error) {
	tx, err := db.Begin()
	if err != nil {
		return nil, err

	}
	defer func() {
		if p := recover(); p != nil {
			tx.Rollback()
			panic(p)
		} else if err != nil {
			tx.Rollback()
		} else {
			err = tx.Commit()
		}
	}()
	result, err := tx.Query(query)
	if err != nil {
		return nil, err

	}
	return result, nil
}

func ParseFilterParam(filterParam string) map[string]string {
	filterMap := make(map[string]string)
	if filterParam != "" {
		filters := strings.Split(filterParam, "|")
		for _, pair := range filters {
			filter := strings.Split(pair, ":")
			if len(filter) == 3 && filter[0] != "" {
				val := strings.Join(filter[1:3], ":")
				if val != ":" {
					filterMap[filter[0]] = val

				}
			}
		}
	}
	return filterMap
}

func ParseRows(rows *sql.Rows) ([]map[string]interface{}, interface{}, error) {
	columns, err := rows.Columns()
	if err != nil {
		return nil, 0, err
	}
	var totalCount interface{}
	var result []map[string]interface{}
	values := make([]interface{}, len(columns))
	for rows.Next() {

		for i := range values {
			values[i] = new(interface{})
		}
		err := rows.Scan(values...)
		if err != nil {
			return nil, 0, err
		}
		rowData := make(map[string]interface{})
		for i, column := range columns {
			if column != "total_count" {
				rowData[column] = *values[i].(*interface{})
			} else {
				totalCount = *values[i].(*interface{})
			}
		}
		result = append(result, rowData)
	}
	return result, totalCount, err
}

func FilterValues(filters map[string]string) []interface{} {
	values := make([]interface{}, 0, len(filters))
	for _, value := range filters {
		_, val := ParseOperatorAndValue(value)
		values = append(values, val)
	}
	return values
}

// parseOperatorAndValue extracts the operator and condition value from the filter string
func ParseOperatorAndValue(filterValue string) (string, string) {
	parts := strings.SplitN(filterValue, ":", 2)
	if len(parts) == 2 {
		return parts[0], parts[1]
	}
	return "", ""
}

// constructCondition constructs the SQL condition based on the operator and condition value
func ConstructCondition(column, operator, value string, whereClauses []string) string {
	switch operator {
	case "=":
		return fmt.Sprintf("%s = $%d", column, len(whereClauses)+1)
	case "!=":
		return fmt.Sprintf("%s != $%d", column, len(whereClauses)+1)
	case "<":
		return fmt.Sprintf("%s < $%d", column, len(whereClauses)+1)
	case ">":
		return fmt.Sprintf("%s > $%d", column, len(whereClauses)+1)
	case ">=":
		return fmt.Sprintf("%s >= $%d", column, len(whereClauses)+1)
	case "<=":
		return fmt.Sprintf("%s <= $%d", column, len(whereClauses)+1)
	case "in":
		// Assuming value is a comma-separated list, adjust as needed
		return fmt.Sprintf("%s IN ($%d)", column, len(whereClauses)+1)
	case "not in":
		// Assuming value is a comma-separated list, adjust as needed
		return fmt.Sprintf("%s NOT IN ($%d)", column, len(whereClauses)+1)
	case "is null":
		return fmt.Sprintf("%s IS NULL", column)
	case "is not null":
		return fmt.Sprintf("%s IS NOT NULL", column)
	case "between":
		// Assuming value is a comma-separated range, adjust as needed
		return fmt.Sprintf("%s BETWEEN $%d AND $%d", column, len(whereClauses)+1, len(whereClauses)+2)
	case "not between":
		// Assuming value is a comma-separated range, adjust as needed
		return fmt.Sprintf("%s NOT BETWEEN $%d AND $%d", column, len(whereClauses)+1, len(whereClauses)+2)
	case "contains":
		// Assuming case-sensitive contains
		return fmt.Sprintf("%s LIKE $%d", column, len(whereClauses)+1)
	case "not contains":
		// Assuming case-sensitive not contains
		return fmt.Sprintf("%s NOT LIKE $%d", column, len(whereClauses)+1)
	case "contains_ci":
		// Assuming case-insensitive contains
		return fmt.Sprintf("%s ILIKE $%d", column, len(whereClauses)+1)
	case "not contains_ci":
		// Assuming case-insensitive not contains
		return fmt.Sprintf("%s NOT ILIKE $%d", column, len(whereClauses)+1)
	case "has suffix":
		return fmt.Sprintf("%s LIKE $%d", column, len(whereClauses)+1)
	case "has prefix":
		return fmt.Sprintf("%s LIKE $%d", column, len(whereClauses)+1)
	}
	return ""
}

func ConvertIndexDef(sqlStatement string) (map[string]interface{}, error) {
	// Define a regular expression pattern to extract relevant information
	pattern := `^CREATE\s+(\w+)\s+INDEX\s+(\w+)\s+ON\s+public\.(\w+)\s+USING\s+(\w+)\s+\((\w+)\)`

	// Use regex to find matches in the SQL statement
	re := regexp.MustCompile(pattern)
	matches := re.FindStringSubmatch(sqlStatement)
	fmt.Println(matches)
	if len(matches) == 0 {
		return nil, fmt.Errorf("No match found in the SQL statement")
	}

	// Extract matched groups
	isUnique := strings.TrimSpace(matches[1]) == "UNIQUE"
	indexName, _, indexAlgorithm, columnName := matches[2], matches[3], matches[4], matches[5]

	// Create a map with the extracted information
	indexInfo := map[string]interface{}{
		"indexName":      indexName,
		"indexAlgorithm": indexAlgorithm,
		"isUnique":       isUnique,
		"columnName":     columnName,
	}

	return indexInfo, nil
}

func FetchSchemaDetails(db *sql.DB, query, tableName string) (map[string]SchemaDetails, error) {
	rows, err := db.Query(query, tableName)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	schemaDetails := make(map[string]SchemaDetails)
	for rows.Next() {
		var columnName, dataType, isNullable, udtName, ordinalPosition string
		var characterMaxLength sql.NullInt64
		var columnDefault sql.NullString

		err := rows.Scan(&columnName, &dataType, &characterMaxLength, &isNullable, &columnDefault, &udtName, &ordinalPosition)
		if err != nil {
			return nil, err
		}

		columnDetails := SchemaDetails{
			DataType:      udtName,
			MaxLength:     characterMaxLength,
			IsNullable:    isNullable,
			Position:      ordinalPosition,
			ColumnDefault: columnDefault,
		}

		schemaDetails[columnName] = columnDetails
	}

	return schemaDetails, nil
}

func FetchIndexDetails(db *sql.DB, query, tableName string) ([]IndexDetails, error) {
	rows, err := db.Query(query, tableName)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var indexes []IndexDetails
	for rows.Next() {
		var indexName, indexDef string
		err := rows.Scan(&indexName, &indexDef)
		if err != nil {
			return nil, err
		}

		indexDetails := IndexDetails{
			IndexName: indexName,
			IndexDef:  indexDef,
		}
		indexDefJson, err := ConvertIndexDef(indexDef)
		if err == nil {
			indexDetails.IndexAlgorithm = indexDefJson["indexAlgorithm"].(string)
			indexDetails.IsUnique = indexDefJson["isUnique"].(bool)
			indexDetails.ColumnName = indexDefJson["columnName"].(string)
		}

		indexes = append(indexes, indexDetails)
	}

	return indexes, nil
}

func FetchForeignKeyDetails(db *sql.DB, query, schemaName string) ([]ForeignKeyDetails, error) {
	rows, err := db.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var foreignKeys []ForeignKeyDetails
	for rows.Next() {
		var (
			constraintName    string
			tableName         string
			columnName        string
			foreignTableName  string
			foreignColumnName string
		)

		err := rows.Scan(&constraintName, &tableName, &columnName, &foreignTableName, &foreignColumnName)
		if err != nil {
			return nil, err
		}
		if tableName == schemaName {
			result := ForeignKeyDetails{
				ConstraintName:    constraintName,
				TableName:         tableName,
				ColumnName:        columnName,
				ForeignTableName:  foreignTableName,
				ForeignColumnName: foreignColumnName,
			}
			foreignKeys = append(foreignKeys, result)
		}
	}
	return foreignKeys, nil
}

func MergeMetaData(schemaDetails map[string]SchemaDetails, indexDetails []IndexDetails, foreignKeyDetails []ForeignKeyDetails) map[string]SchemaDetails {
	for i := 0; i < len(indexDetails); i++ {
		indexDetail := indexDetails[i]
		schema := schemaDetails[indexDetail.ColumnName]
		if strings.Contains(indexDetail.IndexName, "pkey") {
			schema.IsPrimary = true
		}
		schema.Index = true
		schemaDetails[indexDetail.ColumnName] = schema
	}
	for i := 0; i < len(foreignKeyDetails); i++ {
		foreignKeyDetail := foreignKeyDetails[i]
		schema := schemaDetails[foreignKeyDetail.ColumnName]
		schema.ForeignKey = fmt.Sprintf("%s.%s", foreignKeyDetail.ForeignTableName, foreignKeyDetail.ForeignColumnName)
		schemaDetails[foreignKeyDetail.ColumnName] = schema
	}
	return schemaDetails
}
