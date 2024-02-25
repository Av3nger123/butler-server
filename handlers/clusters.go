package handlers

import (
	"butler-server/client"
	"butler-server/internals/core"
	"butler-server/internals/errors"
	"butler-server/internals/utils"
	"butler-server/repository"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
)

type Result struct {
	Details interface{}
	Type    string
	Error   error
}

func InitClusterHandlers(router *gin.Engine) {
	clientRoutes := router.Group("/cluster")
	{
		clientRoutes.GET("/query/:id", handleQuery)
		clientRoutes.GET("/databases/:id", handleDatabases)
		clientRoutes.GET("/tables/:id", handleTables)
		clientRoutes.GET("/metadata/:id", handleMetaData)
		clientRoutes.GET("/data/:id", handleData)
		clientRoutes.GET("/ping/:id", handlePing)
		clientRoutes.POST("/execute/:id", handlePing)
	}

}

func handleDatabases(c *gin.Context) {

	ctx, err := GetClientContext(c)
	if err != nil {
		errors.InternalServerError(err, c, "Failed to get handler context")
		return
	}
	clusterData, err := utils.GetClusterData(ctx.RedisClient, c.Param("id"))
	if err != nil {
		errors.InternalServerError(err, c, "Failed to get Cluster Data, Please reconnect again!")
		return
	}

	key := ctx.RedisClient.GenerateDatabaseKey(fmt.Sprintf("%d", clusterData.Cluster.ID))
	result, err := ctx.RedisClient.GetMap(key)
	if err != nil {
		log.Printf("Cache hit miss for Database")
	} else {
		c.JSON(http.StatusOK, gin.H{"messages": "Databases found", "databases": result["databases"]})
		return
	}
	db, err := core.NewDatabase(core.DatabaseConfig{
		Driver:   clusterData.Cluster.Driver,
		Hostname: clusterData.Cluster.Host,
		Port:     clusterData.Cluster.Port,
		Username: clusterData.Cluster.Username,
		Password: clusterData.Cluster.Password,
	})
	if err != nil {
		errors.InternalServerError(err, c, "Failed connecting due to wrong configuration")
		return
	}
	if err := db.Connect(); err != nil {
		errors.InternalServerError(err, c, "Failed connecting to the db cluster")
		return
	}
	defer db.Close()

	databases, err := db.Databases()
	if err != nil {
		errors.InternalServerError(err, c, "Failed to run query")
		return
	}
	dbMap := make(map[string]interface{})
	dbMap["databases"] = databases
	if err := ctx.RedisClient.SetMap(key, dbMap, time.Duration(24*time.Hour)); err != nil {
		fmt.Println("failed to save databases into cache")
	}
	c.JSON(http.StatusOK, gin.H{"messages": "Databases found", "databases": databases})
}

func handleTables(c *gin.Context) {

	dbName := c.Query("db")
	if dbName == "" {
		errors.BadRequestError(nil, c, "mandatory query parameter db is missing in the url")
		return
	}

	ctx, err := GetClientContext(c)
	if err != nil {
		errors.InternalServerError(err, c, "Failed to get handler context")
		return
	}
	clusterData, err := utils.GetClusterData(ctx.RedisClient, c.Param("id"))
	if err != nil {
		errors.InternalServerError(err, c, "Failed to get Cluster Data, Please reconnect again!")
		return
	}

	key := ctx.RedisClient.GenerateTablesKey(fmt.Sprintf("%d", clusterData.Cluster.ID), dbName)
	res, err := ctx.RedisClient.GetMap(key)
	if err != nil {
		log.Printf("Cache hit miss for Tables")
	} else {
		c.JSON(http.StatusOK, gin.H{"messages": "Tables found", "tables": res["tables"]})
		return
	}

	db, err := core.NewDatabase(core.DatabaseConfig{
		Driver:   clusterData.Cluster.Driver,
		Hostname: clusterData.Cluster.Host,
		Port:     clusterData.Cluster.Port,
		Username: clusterData.Cluster.Username,
		Password: clusterData.Cluster.Password,
		Database: dbName,
	})
	if err != nil {
		errors.InternalServerError(err, c, "Failed connecting due to wrong configuration")
		return
	}
	if err := db.Connect(); err != nil {
		errors.InternalServerError(err, c, "Failed connecting to the db cluster")
		return
	}
	defer db.Close()

	tables, err := db.Tables()
	if err != nil {
		errors.InternalServerError(err, c, "Failed to run query")
		return
	}
	dbMap := make(map[string]interface{})
	dbMap["tables"] = tables
	if err := ctx.RedisClient.SetMap(key, dbMap, time.Duration(24*time.Hour)); err != nil {
		fmt.Println("failed to save tables into cache")
	}
	c.JSON(http.StatusOK, gin.H{"messages": "Tables found", "tables": tables})
}

func handleQuery(c *gin.Context) {
	dbName := c.Query("db")
	query := c.Query("query")
	page, err := strconv.Atoi(c.Query("page"))
	if err != nil {
		errors.BadRequestError(nil, c, "page query param should beof type int in the url")
		return
	}
	size, err := strconv.Atoi(c.Query("size"))
	if err != nil {
		errors.BadRequestError(nil, c, "mandatory query parameter db is missing in the url")
		return
	}
	if dbName == "" {
		errors.BadRequestError(nil, c, "mandatory query parameter db is missing in the url")
		return
	}

	ctx, err := GetClientContext(c)
	if err != nil {
		errors.InternalServerError(err, c, "Failed to get handler context")
		return
	}
	clusterData, err := utils.GetClusterData(ctx.RedisClient, c.Param("id"))
	if err != nil {
		errors.InternalServerError(err, c, "Failed to get Cluster Data, Please reconnect again!")
		return
	}

	db, err := core.NewDatabase(core.DatabaseConfig{
		Driver:   clusterData.Cluster.Driver,
		Hostname: clusterData.Cluster.Host,
		Port:     clusterData.Cluster.Port,
		Username: clusterData.Cluster.Username,
		Password: clusterData.Cluster.Password,
		Database: dbName,
	})
	if err != nil {
		errors.InternalServerError(err, c, "Failed connecting due to wrong configuration")
		return
	}
	if err := db.Connect(); err != nil {
		errors.InternalServerError(err, c, "Failed connecting to the db cluster")
		return
	}
	defer db.Close()
	result, err := db.Query(query, page, size)
	if err != nil {
		errors.InternalServerError(err, c, "Failed Execute the query")
		return
	}
	c.JSON(http.StatusOK, gin.H{"result": result, "message": "Results fetched"})
}

func handleMetaData(c *gin.Context) {

	dbName := c.Query("db")

	if dbName == "" {
		errors.BadRequestError(nil, c, "mandatory query parameter db is missing in the url")
		return
	}

	table := c.Query("table")
	if table == "" {
		errors.BadRequestError(nil, c, "mandatory query parameter table is missing in the url")
		return
	}

	ctx, err := GetClientContext(c)
	if err != nil {
		errors.InternalServerError(err, c, "Failed to get handler context")
		return
	}

	clusterData, err := utils.GetClusterData(ctx.RedisClient, c.Param("id"))
	if err != nil {
		errors.InternalServerError(err, c, "Failed to get Cluster Data, Please reconnect again!")
		return
	}

	key := ctx.RedisClient.GenerateMetadataKey(fmt.Sprintf("%d", clusterData.Cluster.ID), dbName, table)
	result, err := ctx.RedisClient.GetMap(key)
	if err != nil {
		log.Printf("Cache hit miss for Metadata")
	} else {
		c.JSON(http.StatusOK, gin.H{"message": "Metadata for " + table + " found", "metadata": result["metadata"]})
		return
	}

	db, err := core.NewDatabase(core.DatabaseConfig{
		Driver:   clusterData.Cluster.Driver,
		Hostname: clusterData.Cluster.Host,
		Port:     clusterData.Cluster.Port,
		Username: clusterData.Cluster.Username,
		Password: clusterData.Cluster.Password,
		Database: dbName,
	})
	if err != nil {
		errors.InternalServerError(err, c, "Failed connecting due to wrong configuration")
		return
	}
	if err := db.Connect(); err != nil {
		errors.InternalServerError(err, c, "Failed connecting to the db cluster")
		return
	}
	defer db.Close()

	schemaDetails, err := db.Metadata(table)
	if err != nil {
		errors.InternalServerError(err, c, "Failed to run query")
		return
	}
	dbMap := make(map[string]interface{})
	dbMap["metadata"] = schemaDetails
	if err := ctx.RedisClient.SetMap(key, dbMap, time.Duration(24*time.Hour)); err != nil {
		fmt.Println("failed to save metadata into cache")
	}
	c.JSON(http.StatusOK, gin.H{"message": "Metadata for " + table + " found", "metadata": schemaDetails})
}
func handleData(c *gin.Context) {

	dbName := c.Query("db")
	if dbName == "" {
		errors.BadRequestError(nil, c, "mandatory query parameter db is missing in the url")
	}

	table := c.Query("table")
	if table == "" {
		errors.BadRequestError(nil, c, "mandatory query parameter table is missing in the url")
	}

	ctx, err := GetClientContext(c)
	if err != nil {
		errors.InternalServerError(err, c, "Failed to get handler context")
		return
	}

	clusterData, err := utils.GetClusterData(ctx.RedisClient, c.Param("id"))
	if err != nil {
		errors.InternalServerError(err, c, "Failed to get Cluster Data, Please reconnect again!")
		return
	}

	key := ctx.RedisClient.GenerateDataKey(c.Request.URL.RawQuery)
	res, err := ctx.RedisClient.GetMap(key)
	if err != nil {
		log.Printf("Cache hit miss for data")
	} else {
		c.JSON(http.StatusOK, gin.H{"messages": "Data found for table", "data": res["data"], "count": res["count"]})
		return
	}

	db, err := core.NewDatabase(core.DatabaseConfig{
		Driver:   clusterData.Cluster.Driver,
		Hostname: clusterData.Cluster.Host,
		Port:     clusterData.Cluster.Port,
		Username: clusterData.Cluster.Username,
		Password: clusterData.Cluster.Password,
		Database: dbName,
	})
	if err != nil {
		errors.InternalServerError(err, c, "Failed connecting due to wrong configuration")
		return
	}
	if err := db.Connect(); err != nil {
		errors.InternalServerError(err, c, "Failed connecting to the db cluster")
		return
	}
	defer db.Close()

	pageStr := c.DefaultQuery("page", "0")
	sizeStr := c.DefaultQuery("size", "50")
	sortBy := c.Query("sort")
	orderParam := c.DefaultQuery("order", "asc")
	filterParam := c.Query("filter")
	filterOperator := c.Query("operator")

	dbMap, err := db.Data(table, core.Filter{
		Page:     pageStr,
		Size:     sizeStr,
		Sort:     sortBy,
		Order:    orderParam,
		Filter:   filterParam,
		Operator: filterOperator,
	})
	if err != nil {
		errors.InternalServerError(err, c, "Failed to run query")
		return
	}
	if err := ctx.RedisClient.SetMap(key, dbMap, time.Duration(time.Hour)); err != nil {
		fmt.Println("failed to save table data into cache")
	}
	c.JSON(http.StatusOK, gin.H{"messages": "Data found for table", "data": dbMap["data"], "count": dbMap["count"]})
}

func handlePing(c *gin.Context) {

	ctx, err := GetClientContext(c)
	if err != nil {
		errors.InternalServerError(err, c, "Failed to get handler context")
		return
	}

	token := c.Request.Header.Get("Authorization")
	found := repository.CheckAccount(ctx.DBClient, token)
	clusterId := c.Param("id")
	if found {
		if err != nil {
			errors.UnAuthorizedError(err, c, "you are unauthorized to access this resource")
			return
		}
	}
	data, err := client.GetClusterAPI(clusterId)
	if err != nil {
		errors.InternalServerError(err, c, "Failed to get Cluster Data")
		return
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()

		byteData, err := json.Marshal(data)
		if err != nil {
			return
		}
		redisClient := ctx.RedisClient
		if err := redisClient.SetString(redisClient.GenerateClusterKey(strconv.Itoa(data.Cluster.ID)), string(byteData), time.Duration(24*time.Hour)); err != nil {
			fmt.Println("failed to save cluster data into cache")
		}
	}()
	db, err := core.NewDatabase(core.DatabaseConfig{
		Driver:   data.Cluster.Driver,
		Hostname: data.Cluster.Host,
		Port:     data.Cluster.Port,
		Username: data.Cluster.Username,
		Password: data.Cluster.Password,
	})
	if err != nil {
		errors.InternalServerError(err, c, "Failed connecting due to wrong configuration")
		return
	}
	if err := db.Connect(); err != nil {
		errors.InternalServerError(err, c, "Failed connecting to the db cluster")
		return
	}
	defer db.Close()
	wg.Wait()
	c.JSON(http.StatusOK, gin.H{"status": "success", "message": "Database server connected"})
}

func handleExecute(c *gin.Context) {
	type req struct {
		commits     []string `json:"commits"`
		executeType string   `json:"type"`
	}
	var request req
	if err := c.BindJSON(&request); err != nil {
		errors.InternalServerError(err, c, "failed to parse body")
		return
	}

	dbName := c.Query("db")
	if dbName == "" {
		errors.BadRequestError(nil, c, "mandatory query parameter db is missing in the url")
	}

	ctx, err := GetClientContext(c)
	if err != nil {
		errors.InternalServerError(err, c, "Failed to get handler context")
		return
	}
	clusterData, err := utils.GetClusterData(ctx.RedisClient, c.Param("id"))
	if err != nil {
		errors.InternalServerError(err, c, "Failed to get Cluster Data, Please reconnect again!")
		return
	}

	commits, err := commitRepository.GetCommitsByIds(request.commits)

	db, err := core.NewDatabase(core.DatabaseConfig{
		Driver:   clusterData.Cluster.Driver,
		Hostname: clusterData.Cluster.Host,
		Port:     clusterData.Cluster.Port,
		Username: clusterData.Cluster.Username,
		Password: clusterData.Cluster.Password,
		Database: dbName,
	})

	var commitIds []int
	for _, v := range commits {
		commitIds = append(commitIds, v.ID)
	}

	queryRecords, err := queryRepository.GetQueriesWithCommitIds(commitIds)
	if err != nil {
		errors.InternalServerError(err, c, "failed to fetch queries of the commits")
		return
	}

	commitMap := make(map[int][]string, 0)

	sort.Ints(commitIds)
	for _, query := range queryRecords {
		if query.Type == request.executeType {
			commitMap[query.CommitId] = append(commitMap[query.CommitId], query.Query)
		}
	}
	queries := make([]string, 0)
	for _, val := range commitIds {
		queries = append(queries, commitMap[val]...)
	}
	if err := db.Execute(queries); err != nil {
		errors.InternalServerError(err, c, "executing queries failed")
		return
	}
	var result bool
	if request.executeType == "default" {
		result = true
	} else {
		result = false
	}
	commitRepository.UpdateCommits(commits, result)

	c.JSON(http.StatusOK, gin.H{"status": "success", "message": "Executed commits"})

}
