package handlers

import (
	"butler-server/internals"
	"butler-server/internals/core"
	"fmt"
	"log"
	"net/http"
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
	db, err := core.NewDatabase(core.DatabaseConfig{
		Driver:   requestData.Driver,
		Hostname: requestData.Host,
		Port:     requestData.Port,
		Username: requestData.Username,
		Password: requestData.Password,
	})
	if err != nil {
		internals.InternalServerError(err, c, "Failed connecting due to wrong configuration")
		return
	}
	if err := db.Connect(); err != nil {
		internals.InternalServerError(err, c, "Failed connecting to the db cluster")
		return
	}
	defer db.Close()

	databases, err := db.Databases()
	if err != nil {
		internals.InternalServerError(err, c, "Failed to run query")
		return
	}
	dbMap := make(map[string]interface{})
	dbMap["databases"] = databases
	ctx.RedisClient.SetMap(key, dbMap, time.Duration(24*time.Hour))
	c.JSON(http.StatusOK, gin.H{"messages": "Databases found", "databases": databases})
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

	db, err := core.NewDatabase(core.DatabaseConfig{
		Driver:   requestData.Driver,
		Hostname: requestData.Host,
		Port:     requestData.Port,
		Username: requestData.Username,
		Password: requestData.Password,
		Database: requestData.DbName,
	})
	if err != nil {
		internals.InternalServerError(err, c, "Failed connecting due to wrong configuration")
		return
	}
	if err := db.Connect(); err != nil {
		internals.InternalServerError(err, c, "Failed connecting to the db cluster")
		return
	}
	defer db.Close()

	tables, err := db.Tables()
	if err != nil {
		internals.InternalServerError(err, c, "Failed to run query")
		return
	}
	dbMap := make(map[string]interface{})
	dbMap["tables"] = tables
	ctx.RedisClient.SetMap(key, dbMap, time.Duration(24*time.Hour))
	c.JSON(http.StatusOK, gin.H{"messages": "Tables found", "tables": tables})
}

func HandleQuery(c *gin.Context) {
	// requestData, err := parseRequest(c)
	// if err != nil {
	// 	internals.BadRequestError(err, c, "Failed to parse request body")
	// 	return
	// }

	// db, err := internals.ConnectToDB(requestData.Driver, requestData.Username, requestData.Password, requestData.Host, requestData.Port, "")
	// if err != nil {
	// 	internals.InternalServerError(err, c, "Failed connecting to the db cluster")
	// 	return
	// }
	// defer db.Close()

	// rows, err := internals.ExecuteQuery(db, requestData.Query)
	// if err != nil {
	// 	internals.InternalServerError(err, c, "Failed to run query")
	// 	return
	// }
	// defer rows.Close()

	// result, _, err := internals.ParseRows(rows)
	// if err != nil {
	// 	internals.InternalServerError(err, c, "Failed to parse rows")
	// 	return
	// }
	// c.JSON(http.StatusOK, gin.H{"result": result})
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

	db, err := core.NewDatabase(core.DatabaseConfig{
		Driver:   requestData.Driver,
		Hostname: requestData.Host,
		Port:     requestData.Port,
		Username: requestData.Username,
		Password: requestData.Password,
		Database: requestData.DbName,
	})
	if err != nil {
		internals.InternalServerError(err, c, "Failed connecting due to wrong configuration")
		return
	}
	if err := db.Connect(); err != nil {
		internals.InternalServerError(err, c, "Failed connecting to the db cluster")
		return
	}
	defer db.Close()

	schemaDetails, err := db.Metadata(requestData.TableName)
	if err != nil {
		internals.InternalServerError(err, c, "Failed to run query")
		return
	}
	dbMap := make(map[string]interface{})
	dbMap["metadata"] = schemaDetails
	ctx.RedisClient.SetMap(key, dbMap, time.Duration(24*time.Hour))
	c.JSON(http.StatusOK, gin.H{"message": "Metadata for " + requestData.TableName + " found", "metadata": schemaDetails})
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

	db, err := core.NewDatabase(core.DatabaseConfig{
		Driver:   requestData.Driver,
		Hostname: requestData.Host,
		Port:     requestData.Port,
		Username: requestData.Username,
		Password: requestData.Password,
		Database: requestData.DbName,
	})
	if err != nil {
		internals.InternalServerError(err, c, "Failed connecting due to wrong configuration")
		return
	}
	if err := db.Connect(); err != nil {
		internals.InternalServerError(err, c, "Failed connecting to the db cluster")
		return
	}
	defer db.Close()

	pageStr := c.DefaultQuery("page", "0")
	sizeStr := c.DefaultQuery("size", "50")
	sortBy := c.Query("sort")
	orderParam := c.DefaultQuery("order", "asc")
	filterParam := c.Query("filter")
	filterOperator := c.Query("operator")

	dbMap, err := db.Data(requestData.TableName, core.Filter{
		Page:     pageStr,
		Size:     sizeStr,
		Sort:     sortBy,
		Order:    orderParam,
		Filter:   filterParam,
		Operator: filterOperator,
	})
	if err != nil {
		internals.InternalServerError(err, c, "Failed to run query")
		return
	}
	ctx.RedisClient.SetMap(key, dbMap, time.Duration(time.Hour))
	c.JSON(http.StatusOK, gin.H{"messages": "Data found for table", "data": dbMap["result"], "count": dbMap["count"]})
}
