package handlers

import (
	"butler-server/internals"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
)

type Result struct {
	Details interface{}
	Type    string
	Error   error
}

func parseRequest(c *gin.Context) (*internals.QueryRequest, error) {
	// var requestBody struct {
	// 	EncryptedData string `json:"encrypted_payload"`
	// }

	var requestData internals.QueryRequest

	if err := c.ShouldBindJSON(&requestData); err != nil {
		return nil, err
	}
	// key := []byte("your_secret_key_here_of_32_chars")

	// decryptedData, err := internals.Decrypt(requestBody.EncryptedData, key)
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
		internals.BadRequestError(err, c, "Failed to parse request body")
		return
	}
	ctx, err := GetClientContext(c)
	if err != nil {
		internals.InternalServerError(err, c, "Failed to get Handler context")
		return
	}

	key := ctx.RedisClient.GenerateDatabaseKey(fmt.Sprintf("%d", requestData.Id))
	result, err := ctx.RedisClient.GetMap(key)
	if err != nil {
		log.Printf("Cache hit miss for Database")
	} else {
		c.JSON(http.StatusOK, gin.H{"messages": "Databases found", "databases": result["databases"]})
		return
	}

	db, err := internals.ConnectToDB(requestData.Driver, requestData.Username, requestData.Password, requestData.Host, requestData.Port, "")
	if err != nil {
		internals.InternalServerError(err, c, "Failed connecting to the db cluster")
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
		internals.InternalServerError(err, c, "Failed to run query")
		return
	}
	defer rows.Close()

	var databases []string
	for rows.Next() {
		var dbName string
		err := rows.Scan(&dbName)
		if err != nil {
			internals.InternalServerError(err, c, "Failed to fetch databases")
			return
		}
		databases = append(databases, dbName)
	}
	dbMap := make(map[string]interface{})
	dbMap["databases"] = databases
	ctx.RedisClient.SetMap(key, dbMap, time.Duration(24*time.Hour))
	c.JSON(http.StatusOK, gin.H{"messages": "Databases found", "databases": databases})
}

func HandleQuery(c *gin.Context) {
	requestData, err := parseRequest(c)
	if err != nil {
		internals.BadRequestError(err, c, "Failed to parse request body")
		return
	}

	db, err := internals.ConnectToDB(requestData.Driver, requestData.Username, requestData.Password, requestData.Host, requestData.Port, "")
	if err != nil {
		internals.InternalServerError(err, c, "Failed connecting to the db cluster")
		return
	}
	defer db.Close()

	rows, err := internals.ExecuteQuery(db, requestData.Query)
	if err != nil {
		internals.InternalServerError(err, c, "Failed to run query")
		return
	}
	defer rows.Close()

	result, _, err := internals.ParseRows(rows)
	if err != nil {
		internals.InternalServerError(err, c, "Failed to parse rows")
		return
	}
	c.JSON(http.StatusOK, gin.H{"result": result})
}

func HandleMetaData(c *gin.Context) {

	requestData, err := parseRequest(c)
	if err != nil {
		internals.BadRequestError(err, c, "Failed to parse request body")
		return
	}

	ctx, err := GetClientContext(c)
	if err != nil {
		internals.InternalServerError(err, c, "Failed to get Handler context")
		return
	}

	key := ctx.RedisClient.GenerateMetadataKey(fmt.Sprintf("%d", requestData.Id), requestData.DbName, requestData.TableName)
	result, err := ctx.RedisClient.GetMap(key)
	if err != nil {
		log.Printf("Cache hit miss for Metadata")
	} else {
		c.JSON(http.StatusOK, gin.H{"message": "Metadata for " + requestData.TableName + " found", "metadata": result["metadata"]})
		return
	}

	var wg sync.WaitGroup
	resultCh := make(chan Result, 3)
	wg.Add(3)

	db, err := internals.ConnectToDB(requestData.Driver, requestData.Username, requestData.Password, requestData.Host, requestData.Port, requestData.DbName)
	if err != nil {
		internals.InternalServerError(err, c, "Failed connecting to the db cluster")
		return
	}
	defer db.Close()

	go func() {
		defer wg.Done()
		schemaDetails, err := internals.FetchSchemaDetails(db, requestData.TableName)
		if err != nil {
			resultCh <- Result{Details: nil, Error: err, Type: "schema"}
			return
		}
		resultCh <- Result{Details: schemaDetails, Error: nil, Type: "schema"}
	}()

	go func() {
		defer wg.Done()
		foreignKeyDetails, err := internals.FetchForeignKeyDetails(db, requestData.TableName)
		if err != nil {
			resultCh <- Result{Details: nil, Error: err, Type: "foreign key"}
			return
		}
		resultCh <- Result{Details: foreignKeyDetails, Error: nil, Type: "foreign key"}
	}()

	go func() {
		defer wg.Done()
		indexDetails, err := internals.FetchIndexDetails(db, requestData.TableName)
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
			internals.InternalServerError(result.Error, c, fmt.Sprintf("Failed to fetch %s details", result.Type))
			return
		}
		results[result.Type] = result.Details
	}
	schemaDetails := internals.MergeMetaData(results["schema"].(map[string]internals.SchemaDetails), results["index"].([]internals.IndexDetails), results["foreign key"].([]internals.ForeignKeyDetails))

	dbMap := make(map[string]interface{})
	dbMap["metadata"] = schemaDetails
	ctx.RedisClient.SetMap(key, dbMap, time.Duration(24*time.Hour))
	c.JSON(http.StatusOK, gin.H{"message": "Metadata for " + requestData.TableName + " found", "metadata": schemaDetails})
}

func HandleTables(c *gin.Context) {
	requestData, err := parseRequest(c)
	if err != nil {
		internals.BadRequestError(err, c, "Failed to parse request body")
		return
	}

	ctx, err := GetClientContext(c)
	if err != nil {
		internals.InternalServerError(err, c, "Failed to get Handler context")
		return
	}

	key := ctx.RedisClient.GenerateTablesKey(fmt.Sprintf("%d", requestData.Id), requestData.DbName)
	res, err := ctx.RedisClient.GetMap(key)
	if err != nil {
		log.Printf("Cache hit miss for Tables")
	} else {
		c.JSON(http.StatusOK, gin.H{"messages": "Tables found", "tables": res["tables"]})
		return
	}

	db, err := internals.ConnectToDB(requestData.Driver, requestData.Username, requestData.Password, requestData.Host, requestData.Port, requestData.DbName)
	if err != nil {
		internals.InternalServerError(err, c, "Failed connecting to the db cluster")
		return
	}
	defer db.Close()

	rows, err := db.Query("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'  AND table_type = 'BASE TABLE';")
	if err != nil {
		internals.InternalServerError(err, c, "Failed to run query")
		return
	}
	defer rows.Close()

	var tables []string
	for rows.Next() {
		var tableName string
		err := rows.Scan(&tableName)
		if err != nil {
			internals.InternalServerError(err, c, "Failed to fetch tables")
			return
		}
		tables = append(tables, tableName)
	}
	dbMap := make(map[string]interface{})
	dbMap["tables"] = tables
	ctx.RedisClient.SetMap(key, dbMap, time.Duration(24*time.Hour))
	c.JSON(http.StatusOK, gin.H{"messages": "Tables found", "tables": tables})
}

func HandleData(c *gin.Context) {

	requestData, err := parseRequest(c)
	if err != nil {
		internals.BadRequestError(err, c, "Failed to parse request body")
		return
	}

	ctx, err := GetClientContext(c)
	if err != nil {
		internals.InternalServerError(err, c, "Failed to get Handler context")
		return
	}

	key := ctx.RedisClient.GenerateDataKey(fmt.Sprintf("%d", requestData.Id), requestData.DbName, requestData.TableName, c.Request.URL.RawQuery)
	fmt.Println(key)
	res, err := ctx.RedisClient.GetMap(key)
	if err != nil {
		log.Printf("Cache hit miss for data")
	} else {
		c.JSON(http.StatusOK, gin.H{"messages": "Data found for table", "data": res["data"], "count": res["count"]})
		return
	}

	db, err := internals.ConnectToDB(requestData.Driver, requestData.Username, requestData.Password, requestData.Host, requestData.Port, requestData.DbName)
	if err != nil {
		internals.InternalServerError(err, c, "Failed connecting to the db cluster")
		return
	}
	defer db.Close()

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
		internals.BadRequestError(err, c, "Please check the page param in the URL")
		return
	}

	size, err := strconv.Atoi(sizeStr)
	if err != nil {
		// Handle the error, for example, return a bad request response
		internals.BadRequestError(err, c, "Please check the page param in the URL")
		return
	}

	if order != "asc" && order != "desc" {
		internals.BadRequestError(fmt.Errorf("invalid order parameter"), c, "Please check the order param in the URL. It should be either 'asc' or 'dsc'")
		return
	}

	offset := (page) * size

	filterMap := internals.ParseFilterParam(filterParam)

	query := fmt.Sprintf("SELECT *, COUNT(*) OVER() as total_count FROM %s", requestData.TableName)
	if filterOperator == "and" {
		filterOperator = "AND"
	} else if filterOperator == "or" {
		filterOperator = "OR"
	}

	if len(filterMap) > 0 {
		whereClauses := make([]string, 0)
		for key, value := range filterMap {
			operator, conditionValue := internals.ParseOperatorAndValue(value)
			whereClauses = append(whereClauses, internals.ConstructCondition(key, operator, conditionValue, whereClauses))
		}
		if filterOperator != "" {
			query += " WHERE " + strings.Join(whereClauses, " "+filterOperator+" ")
		} else {
			query += " WHERE " + whereClauses[0]
		}
	}
	if sortBy != "" {
		query += fmt.Sprintf(" ORDER BY %s %s", sortBy, order)
	}
	query += fmt.Sprintf(" LIMIT %d OFFSET %d;", size, offset)

	fmt.Println(query)

	rows, err := db.Query(query, internals.FilterValues(filterMap)...)
	if err != nil {
		internals.InternalServerError(err, c, "Failed to run query")
		return
	}
	defer rows.Close()

	result, count, err := internals.ParseRows(rows)
	if err != nil {
		internals.InternalServerError(err, c, "Failed to parse rows")
		return
	}

	dbMap := make(map[string]interface{})
	dbMap["data"] = result
	dbMap["count"] = count
	ctx.RedisClient.SetMap(key, dbMap, time.Duration(time.Hour))
	c.JSON(http.StatusOK, gin.H{"messages": "Data found for table", "data": result, "count": count})
}
