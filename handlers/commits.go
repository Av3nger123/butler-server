package handlers

import (
	"butler-server/internals/errors"
	"butler-server/internals/models"
	"butler-server/internals/utils"
	"butler-server/repository"
	"fmt"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
)

var repo repository.Repository
var commitRepository repository.CommitRepository
var queryRepository repository.QueryRepository

func InitCommitHandlers(router *gin.Engine, rep repository.Repository) {

	commitRoutes := router.Group("/commits")
	{
		commitRoutes.POST("", handleSaveCommits)
		commitRoutes.GET("", handleGetCommits)
	}
	repo = rep
	queryRepository = repository.NewQueryRepository(rep)
	commitRepository = repository.NewCommitRepository(rep)

}

func handleSaveCommits(c *gin.Context) {
	var commitReq models.CommitRequest
	if err := c.BindJSON(&commitReq); err != nil {
		errors.InternalServerError(err, c, "failed to parse body")
		return
	}
	tx := repo.Begin()
	defer func() {
		if r := recover(); r != nil {
			fmt.Println("rollback err:", r)
			tx.Rollback()
			errors.InternalServerError(fmt.Errorf("transaction rollback"), c, "failed to save commit")
		}
	}()

	commit := repository.Commit{Title: commitReq.Title, DatabaseId: commitReq.DatabaseId, ClusterId: commitReq.ClusterId, CreatedAT: time.Now()}

	commit, err := commitRepository.SaveCommitWithTx(tx, commit)
	if err != nil {
		tx.Rollback()
		errors.InternalServerError(err, c, "failed to save commit")
		return
	}

	processAndSaveQueries := func(queries []string, queryType string) error {
		groupedQueries, err := utils.ProcessQueries(queries)
		if err != nil {
			return err
		}
		for k, v := range groupedQueries {
			_, err := queryRepository.SaveQueriesWithTx(tx, v, k, commit.ID, queryType)
			if err != nil {
				return err
			}

		}
		return nil
	}

	if err := processAndSaveQueries(commitReq.Queries, "default"); err != nil {
		tx.Rollback()
		errors.InternalServerError(err, c, "failed to save sql queries")
		return
	}

	if err := processAndSaveQueries(commitReq.RevertQueries, "revert"); err != nil {
		tx.Rollback()
		errors.InternalServerError(err, c, "failed to save revert sql queries")
		return
	}

	tx.Commit()
	c.JSON(http.StatusOK, gin.H{"message": "commit saved successfully"})
}

func handleGetCommits(c *gin.Context) {
	databaseId := c.Query("databaseId")
	clusterId := c.Query("clusterId")

	commits, err := commitRepository.GetCommits(databaseId, clusterId)
	if err != nil {
		errors.InternalServerError(err, c, "failed to fetch commits")
		return
	}

	var commitIds []int
	for _, v := range commits {
		commitIds = append(commitIds, v.ID)
	}

	queries, err := queryRepository.GetQueriesWithCommitIds(commitIds)
	if err != nil {
		errors.InternalServerError(err, c, "failed to fetch queries of the commits")
		return
	}

	commitMap := make(map[int][]repository.Query, 0)

	for _, query := range queries {
		commitMap[query.CommitId] = append(commitMap[query.CommitId], query)
	}

	c.JSON(http.StatusOK, gin.H{"message": "commits found", "commits": commits, "queries": commitMap})
}
