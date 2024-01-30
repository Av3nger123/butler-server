package errors

import (
	"log"
	"net/http"
	"os"

	"github.com/gin-gonic/gin"
)

func HandleError(err error) {
	if err != nil {
		log.Fatalln(err.Error())
		os.Exit(1)
	}
}

func InternalServerError(err error, c *gin.Context, message string) {
	c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error(), "message": message})
}

func BadRequestError(err error, c *gin.Context, message string) {
	c.JSON(http.StatusBadRequest, gin.H{"error": err.Error(), "message": message})
}

func UnAuthorizedError(err error, c *gin.Context, message string) {
	c.JSON(http.StatusUnauthorized, gin.H{"error": err.Error(), "message": message})
}
