package service

import (
	"database/sql"
	"fmt"
	"strings"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"
)

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
		filters := strings.Split(filterParam, ",")
		for _, pair := range filters {

			filter := strings.Split(pair, ":")
			if len(filter) == 2 {
				filterMap[filter[0]] = filter[1]
			}
		}
	}
	return filterMap
}

func ParseRows(rows *sql.Rows) ([]map[string]interface{}, error) {
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
	return result, err
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
