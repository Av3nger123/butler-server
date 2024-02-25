package repository

import (
	"fmt"
	"time"

	"gorm.io/gorm"
)

type Query struct {
	ID         int       `gorm:"column:id" json:"id"`
	Query      string    `gorm:"column:query" json:"query"`
	CommitId   int       `gorm:"column:commitId" json:"commitId"`
	Type       string    `gorm:"column:type" json:"type"`
	CreatedAT  time.Time `gorm:"column:createdAt" json:"createdAt"`
	ExecutedAt time.Time `gorm:"column:executedAt;nullable" json:"executedAt"`
	TableId    string    `gorm:"column:tableId" json:"tableId"`
}

func (Query) TableName() string {
	return "queries"
}

type QueryRepository struct {
	Repository
}

var queryInstance *QueryRepository = new(QueryRepository)

func NewQueryRepository(repo Repository) QueryRepository {
	if queryInstance == nil {
		return QueryRepository{repo}
	}
	return *queryInstance
}

func (q QueryRepository) SaveQueries(queries []string, tableId string, commitId int, queryType string) ([]Query, error) {
	var queryEntities []Query
	fmt.Println("table:", tableId)
	for _, v := range queries {
		queryEntities = append(queryEntities, Query{Query: v, TableId: tableId, CreatedAT: time.Now(), CommitId: commitId, Type: queryType})
	}
	if err := q.Create(queryEntities).Error; err != nil {
		fmt.Println(err.Error())
		return nil, err
	}
	return queryEntities, nil
}

func (q QueryRepository) SaveQueriesWithTx(tx *gorm.DB, queries []string, tableId string, commitId int, queryType string) ([]Query, error) {
	var queryEntities []Query
	for _, v := range queries {
		queryEntities = append(queryEntities, Query{Query: v, TableId: tableId, CreatedAT: time.Now(), CommitId: commitId, Type: queryType})
	}
	if err := tx.Create(&queryEntities).Error; err != nil {
		return nil, err
	}
	return queryEntities, nil
}

func (q QueryRepository) GetQueriesWithCommitIds(commitIds []int) ([]Query, error) {

	var queries []Query

	if err := q.Where(`"commitId" IN (?)`, commitIds).Find(&queries).Error; err != nil {
		return queries, err
	}
	return queries, nil
}
