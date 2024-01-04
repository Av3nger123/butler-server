package core

import (
	"butler-server/internals"
	"fmt"
	"strconv"
	"strings"
)

type Database interface {
	Connect() error
	Query() ([]interface{}, error)
	Close() error
	Databases() ([]string, error)
	Tables() ([]string, error)
	Metadata(table string) (map[string]internals.SchemaDetails, error)
	Data(table string, filter Filter) (map[string]interface{}, error)
}

type Result struct {
	Details interface{}
	Type    string
	Error   error
}

type DatabaseConfig struct {
	Driver   string
	Hostname string
	Port     int
	Username string
	Password string
	Database string
}

type Filter struct {
	Page     string
	Size     string
	Sort     string
	Order    string
	Filter   string
	Operator string
}

func NewDatabase(config DatabaseConfig) (Database, error) {
	switch config.Driver {
	case "mysql":
		return &MySQLDatabase{config: config}, nil
	case "mssql":
		return &MsSQLDatabase{config: config}, nil
	case "postgres":
		return &PostgreSQLDatabase{config: config}, nil
	case "mariadb":
		return &MariaDatabase{config: config}, nil
	case "mongodb":
		return &MongoDBDatabase{config: config}, nil
	default:
		return nil, fmt.Errorf("unsupported database driver: %s", config.Driver)
	}
}

func ParseSQLQuery(table string, filter Filter, filterMap map[string]string) (string, error) {
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
