package handlers

import (
	"log"

	"github.com/gin-gonic/gin"
)

func StartServer() {
	r := gin.Default()

	r.POST("/query", HandleQuery)
	r.GET("/databases", HandleDatabases)
	r.GET("/tables", HandleTables)
	r.GET("/schema", HandleSchema)

	log.Fatal(r.Run(":8080"))
}
