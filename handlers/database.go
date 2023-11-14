package handlers

import (
	"butler-server/service"
	"database/sql"
	"fmt"
	"net/http"

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

	var result []map[string]interface{}
	columns, err := rows.Columns()
	if err != nil {
		service.InternalServerError(err, c, "Failed to get columns of the query")
		return
	}

	values := make([]interface{}, len(columns))
	for rows.Next() {
		for i := range values {
			values[i] = new(interface{})
		}

		if err := rows.Scan(values...); err != nil {
			service.InternalServerError(err, c, "Failed to assign values")
			return
		}

		rowData := make(map[string]interface{})
		for i, column := range columns {
			rowData[column] = *values[i].(*interface{})
		}

		result = append(result, rowData)
	}
	c.JSON(http.StatusOK, gin.H{"result": result})
}

func HandleSchema(c *gin.Context) {
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

	rows, err := db.Query(fmt.Sprintf("SELECT column_name, data_type, character_maximum_length, is_nullable FROM information_schema.column WHERE table_name = '%s';", requestData.TableName))
	if err != nil {
		service.InternalServerError(err, c, "Failed to run query")
		return
	}
	defer rows.Close()

	schemaDetails := make(map[string]map[string]interface{})

	for rows.Next() {
		var columnName, dataType, isNullable string
		var characterMaxLength sql.NullInt64

		err := rows.Scan(&columnName, &dataType, &characterMaxLength, &isNullable)
		if err != nil {
			service.InternalServerError(err, c, "Failed to fetch schema")
			return
		}

		columnDetails := map[string]interface{}{
			"Data Type":   dataType,
			"Max Length":  characterMaxLength,
			"Is Nullable": isNullable,
		}

		schemaDetails[columnName] = columnDetails
	}
	c.JSON(http.StatusOK, gin.H{"message": "Schema for " + requestData.TableName + " found", "schema": schemaDetails})

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
