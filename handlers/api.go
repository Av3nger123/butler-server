package handlers

import (
	"log"

	"github.com/gin-gonic/gin"
)

func StartServer() {
	r := gin.Default()

	r.Use(corsMiddleware())

	r.POST("/query", HandleQuery)
	r.POST("/databases", HandleDatabases)
	r.POST("/tables", HandleTables)
	r.POST("/schema", HandleSchema)
	r.POST("/data", HandleData)

	log.Fatal(r.Run(":8080"))
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
