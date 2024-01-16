package handlers

import (
	"butler-server/client"
	"butler-server/config"
	"butler-server/internals"
	"encoding/json"
	"errors"
	"fmt"
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
	r.Use(decryptPayloadMiddleware())

	r.POST("/query", HandleQuery)
	r.GET("/databases/:id", HandleDatabases)
	r.GET("/tables/:id", HandleTables)
	r.GET("/metadata/:id", HandleMetaData)
	r.GET("/data/:id", HandleData)
	r.GET("/ping/:id", HandlePing)

	log.Fatal(r.Run())
}

func corsMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		c.Writer.Header().Set("Access-Control-Allow-Origin", "http://localhost:3000")
		c.Writer.Header().Set("Access-Control-Allow-Credentials", "true")
		c.Writer.Header().Set("Access-Control-Allow-Headers", "Content-Type, Content-Length, Accept-Encoding, X-CSRF-Token, Authorization, accept, origin, Cache-Control, X-Requested-With")
		c.Writer.Header().Set("Access-Control-Allow-Methods", "POST, OPTIONS, GET, PUT")

		if c.Request.Method == "OPTIONS" {
			c.AbortWithStatus(204)
			return
		}

		c.Next()
	}
}

func decryptPayloadMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		if c.Request.Method == "GET" {
			c.Next()
			return
		}
		var queryRequest internals.QueryRequest
		if config.GetString("ENCRYPTION") == "true" {
			fmt.Println("here")
			var data struct {
				EncryptedPayload string `json:"encryptedPayload"`
				Tag              string `json:"tag"`
				IV               string `json:"iv"`
			}

			if err := c.BindJSON(&data); err != nil {
				internals.BadRequestError(err, c, "Invalid request body")
				return
			}
			decrypted, err := internals.Decrypt(data.EncryptedPayload, []byte("your_secret_key_here_of_32_chars"), data.IV, data.Tag)
			if err != nil {
				internals.InternalServerError(err, c, "Decryption error")
				return
			}
			if err := json.Unmarshal(decrypted, &queryRequest); err != nil {
				internals.BadRequestError(err, c, "Invalid payload format")
				return
			}
		} else {
			if err := c.ShouldBindJSON(&queryRequest); err != nil {
				internals.BadRequestError(err, c, "Invalid payload format")
				return
			}
		}
		c.Set("queryRequest", queryRequest)
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
