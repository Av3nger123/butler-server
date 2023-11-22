package handlers

import (
	"butler-server/client"
	"errors"
	"log"

	"github.com/gin-gonic/gin"
)

type HandlerContext struct {
	DBClient    *client.Database
	RedisClient *client.RedisClient
}

const HandlerContextKey = "HandlerContext"

// NewHandlerContext creates a new HandlerContext instance
func NewHandlerContext(dbClient *client.Database, redisClient *client.RedisClient) *HandlerContext {
	return &HandlerContext{
		DBClient:    dbClient,
		RedisClient: redisClient,
	}
}

func StartServer(dbClient *client.Database, redisClient *client.RedisClient, port string) {
	r := gin.Default()

	r.Use(corsMiddleware())
	r.Use(setupHandlerContext(dbClient, redisClient))

	r.POST("/query", HandleQuery)
	r.POST("/databases", HandleDatabases)
	r.POST("/tables", HandleTables)
	r.POST("/metadata", HandleMetaData)
	r.POST("/data", HandleData)

	log.Fatal(r.Run())
}

func corsMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		c.Writer.Header().Set("Access-Control-Allow-Origin", "*")
		c.Writer.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE")
		c.Writer.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization")

		if c.Request.Method == "OPTIONS" {
			c.AbortWithStatus(204)
			return
		}

		c.Next()
	}
}

func setupHandlerContext(dbClient *client.Database, redisClient *client.RedisClient) gin.HandlerFunc {
	return func(c *gin.Context) {
		context := NewHandlerContext(dbClient, redisClient)
		c.Set(HandlerContextKey, context)
		c.Next()
	}
}

func GetClientContext(c *gin.Context) (*HandlerContext, error) {
	context, ok := c.Get(HandlerContextKey)

	if !ok {
		return nil, errors.New("failed to fetch handler context")

	}

	ctx, ok := context.(*HandlerContext)
	if !ok {
		return nil, errors.New("failed to fetch handler context")

	}
	return ctx, nil
}
