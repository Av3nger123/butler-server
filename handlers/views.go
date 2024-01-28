package handlers

import (
	"butler-server/internals"
	"butler-server/repository"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
)

var viewRepository repository.ViewRepository

func InitViewHandlers(router *gin.Engine, repo repository.Repository) {
	viewRoutes := router.Group("/views")
	{
		viewRoutes.POST("", HandleSaveView)
		viewRoutes.GET("", HandleGetViews)
	}
	viewRepository = repository.NewViewRepository(repo)
}

func HandleSaveView(c *gin.Context) {

	var view repository.DataView

	if err := c.BindJSON(&view); err != nil {
		internals.BadRequestError(err, c, "unable to parse request body")
		return
	}
	view.CreatedAt = time.Now()
	err := viewRepository.SaveView(view)
	if err != nil {
		internals.InternalServerError(err, c, "failed to save view")
		return
	}
	c.JSON(http.StatusOK, gin.H{"message": "view successfully saved"})

}
func HandleGetViews(c *gin.Context) {
	clusterId := c.Query("clusterId")
	databaseId := c.Query("databaseId")

	views, err := viewRepository.GetViews(clusterId, databaseId)
	if err != nil {
		internals.InternalServerError(err, c, "unable to fetch views")
		return
	}
	c.JSON(http.StatusOK, gin.H{"messages": "Views found", "views": views})
}
