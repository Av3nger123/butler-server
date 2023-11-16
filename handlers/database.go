package handlers

import (
	"butler-server/service"
	"database/sql"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"strings"

	"github.com/gin-gonic/gin"
)

type QueryRequest struct {
	Driver    string `json:"type"`
	Username  string `json:"username"`
	Password  string `json:"password"`
	Host      string `json:"host"`
	Port      string `json:"port"`
	DbName    string `json:"dbname"`
	Query     string `json:"query"`
	TableName string `json:"tablename"`
}

func parseRequest(c *gin.Context) (*QueryRequest, error) {
	// var requestBody struct {
	// 	EncryptedData string `json:"encrypted_payload"`
	// }

	var requestData QueryRequest

	if err := c.ShouldBindJSON(&requestData); err != nil {
		return nil, err
	}
	// key := []byte("your_secret_key_here_of_32_chars")

	// decryptedData, err := service.Decrypt(requestBody.EncryptedData, key)
	// if err != nil {
	// 	return nil, err
	// }

	// if err := json.Unmarshal([]byte(decryptedData), &requestData); err != nil {
	// 	return nil, err
	// }
	return &requestData, nil

}

func HandleDatabases(c *gin.Context) {
	requestData, err := parseRequest(c)
	if err != nil {
		service.BadRequestError(err, c, "Failed to parse request body")
		return
	}

	db, err := service.ConnectToDB(requestData.Driver, requestData.Username, requestData.Password, requestData.Host, requestData.Port, "")
	if err != nil {
		service.InternalServerError(err, c, "Failed connecting to the db cluster")
		return
	}
	defer db.Close()

	var databaseQuery string

	switch requestData.Driver {
	case "postgres":
		databaseQuery = "SELECT datname FROM pg_database"
	case "mysql":
		databaseQuery = "SHOW DATABASES"

	}
	rows, err := db.Query(databaseQuery)
	if err != nil {
		service.InternalServerError(err, c, "Failed to run query")
		return
	}
	defer rows.Close()

	var databases []string
	for rows.Next() {
		var dbName string
		err := rows.Scan(&dbName)
		if err != nil {
			service.InternalServerError(err, c, "Failed to fetch databases")
			return
		}
		databases = append(databases, dbName)
	}
	c.JSON(http.StatusOK, gin.H{"messages": "Databases found", "databases": databases})
}

func HandleQuery(c *gin.Context) {
	requestData, err := parseRequest(c)
	if err != nil {
		service.BadRequestError(err, c, "Failed to parse request body")
		return
	}

	db, err := service.ConnectToDB(requestData.Driver, requestData.Username, requestData.Password, requestData.Host, requestData.Port, "")
	if err != nil {
		service.InternalServerError(err, c, "Failed connecting to the db cluster")
		return
	}
	defer db.Close()

	rows, err := service.ExecuteQuery(db, requestData.Query)
	if err != nil {
		service.InternalServerError(err, c, "Failed to run query")
		return
	}
	defer rows.Close()

	result, err := service.ParseRows(rows)
	if err != nil {
		service.InternalServerError(err, c, "Failed to parse rows")
		return
	}
	c.JSON(http.StatusOK, gin.H{"result": result})
}

func HandleSchema(c *gin.Context) {
	requestData, err := parseRequest(c)
	if err != nil {
		service.BadRequestError(err, c, "Failed to parse request body")
		return
	}

	db, err := service.ConnectToDB(requestData.Driver, requestData.Username, requestData.Password, requestData.Host, requestData.Port, requestData.DbName)
	if err != nil {
		service.InternalServerError(err, c, "Failed connecting to the db cluster")
		return
	}
	defer db.Close()

	rows, err := db.Query(fmt.Sprintf("SELECT column_name, data_type, character_maximum_length, is_nullable, column_default, udt_name, ordinal_position FROM information_schema.columns WHERE table_name = '%s';", requestData.TableName))
	if err != nil {
		service.InternalServerError(err, c, "Failed to run query")
		return
	}
	defer rows.Close()

	schemaDetails := make(map[string]map[string]interface{})
	for rows.Next() {
		var columnName, dataType, isNullable, udtName, ordinalPosition string
		var characterMaxLength sql.NullInt64
		var columnDefault sql.NullString

		err := rows.Scan(&columnName, &dataType, &characterMaxLength, &isNullable, &columnDefault, &udtName, &ordinalPosition)
		if err != nil {
			service.InternalServerError(err, c, "Failed to fetch schema")
			return
		}

		columnDetails := map[string]interface{}{
			"dataType":      udtName,
			"maxLength":     characterMaxLength,
			"isNullable":    isNullable,
			"position":      ordinalPosition,
			"columnDefault": columnDefault,
		}

		schemaDetails[columnName] = columnDetails
	}

	query := fmt.Sprintf(`
		SELECT indexname, indexdef
		FROM pg_indexes
		WHERE tablename = '%s';
	`, requestData.TableName)

	rows, err = db.Query(query)
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()

	indexes := make([]map[string]interface{}, 0)

	for rows.Next() {
		var indexName, indexDef string
		err := rows.Scan(&indexName, &indexDef)
		if err != nil {
			log.Fatal(err)
		}

		indexDetails := map[string]interface{}{
			"indexName": indexName,
			"indexDef":  indexDef,
		}

		indexes = append(indexes, indexDetails)
	}
	c.JSON(http.StatusOK, gin.H{"message": "Schema for " + requestData.TableName + " found", "schema": schemaDetails, "indexes": indexes})

}

func HandleTables(c *gin.Context) {
	requestData, err := parseRequest(c)
	if err != nil {
		service.BadRequestError(err, c, "Failed to parse request body")
		return
	}

	db, err := service.ConnectToDB(requestData.Driver, requestData.Username, requestData.Password, requestData.Host, requestData.Port, requestData.DbName)
	if err != nil {
		service.InternalServerError(err, c, "Failed connecting to the db cluster")
		return
	}
	defer db.Close()

	rows, err := db.Query("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'  AND table_type = 'BASE TABLE';")
	if err != nil {
		service.InternalServerError(err, c, "Failed to run query")
		return
	}
	defer rows.Close()

	var tables []string
	for rows.Next() {
		var tableName string
		err := rows.Scan(&tableName)
		if err != nil {
			service.InternalServerError(err, c, "Failed to fetch tables")
			return
		}
		tables = append(tables, tableName)
	}
	c.JSON(http.StatusOK, gin.H{"messages": "Tables found", "tables": tables})
}

func HandleData(c *gin.Context) {

	pageStr := c.DefaultQuery("page", "0")
	sizeStr := c.DefaultQuery("size", "50")
	sortBy := c.Query("sort")
	order := c.DefaultQuery("order", "asc")
	filterParam := c.Query("filter")
	filterOperator := c.Query("operator")

	// Convert page and size to integers
	page, err := strconv.Atoi(pageStr)
	if err != nil {
		// Handle the error, for example, return a bad request response
		service.BadRequestError(err, c, "Please check the page param in the URL")
		return
	}

	size, err := strconv.Atoi(sizeStr)
	if err != nil {
		// Handle the error, for example, return a bad request response
		service.BadRequestError(err, c, "Please check the page param in the URL")
		return
	}

	if order != "asc" && order != "desc" {
		service.BadRequestError(fmt.Errorf("invalid order parameter"), c, "Please check the order param in the URL. It should be either 'asc' or 'dsc'")
		return
	}

	offset := (page) * size

	requestData, err := parseRequest(c)
	if err != nil {
		service.BadRequestError(err, c, "Failed to parse request body")
		return
	}

	db, err := service.ConnectToDB(requestData.Driver, requestData.Username, requestData.Password, requestData.Host, requestData.Port, requestData.DbName)
	if err != nil {
		service.InternalServerError(err, c, "Failed connecting to the db cluster")
		return
	}
	defer db.Close()

	filterMap := service.ParseFilterParam(filterParam)

	query := fmt.Sprintf("SELECT * FROM %s", requestData.TableName)
	if filterOperator == "and" {
		filterOperator = "AND"
	} else if filterOperator == "or" {
		filterOperator = "OR"
	}

	if len(filterMap) > 0 {
		whereClauses := make([]string, 0)
		for key, value := range filterMap {
			operator, conditionValue := service.ParseOperatorAndValue(value)
			whereClauses = append(whereClauses, service.ConstructCondition(key, operator, conditionValue, whereClauses))
		}
		query += " WHERE " + strings.Join(whereClauses, " "+filterOperator+" ")
	}
	if sortBy != "" {
		query += fmt.Sprintf(" ORDER BY %s %s", sortBy, order)
	}
	query += fmt.Sprintf(" LIMIT %d OFFSET %d;", size, offset)

	rows, err := db.Query(query, service.FilterValues(filterMap)...)
	if err != nil {
		service.InternalServerError(err, c, "Failed to run query")
		return
	}
	defer rows.Close()

	result, err := service.ParseRows(rows)
	if err != nil {
		service.InternalServerError(err, c, "Failed to parse rows")
		return
	}
	c.JSON(http.StatusOK, gin.H{"messages": "Data found for table", "data": result})
}
