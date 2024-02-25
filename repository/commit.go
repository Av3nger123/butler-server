package repository

import (
	"time"

	"gorm.io/gorm"
)

type CommitRepository struct {
	Repository
}

var commitInstance *CommitRepository = new(CommitRepository)

func NewCommitRepository(repo Repository) CommitRepository {
	if commitInstance == nil {
		return CommitRepository{repo}

	}
	return *commitInstance
}

type Commit struct {
	ID         int       `gorm:"column:id" json:"id"`
	Title      string    `gorm:"column:title" json:"title"`
	CreatedAT  time.Time `gorm:"column:createdAt" json:"createdAt"`
	ExecutedAt time.Time `gorm:"column:executedAt;nullable" json:"executedAt"`
	ClusterId  string    `gorm:"column:clusterId" json:"clusterId"`
	IsExecuted bool      `gorm:"column:isExecuted" json:"isExecuted"`
	DatabaseId string    `gorm:"column:databaseId" json:"databaseId"`
}

func (Commit) TableName() string {
	return "commits"
}

func (c CommitRepository) SaveCommit(commit Commit) (Commit, error) {
	if err := c.Create(&commit).Error; err != nil {
		return commit, err
	}
	return commit, nil
}

func (c CommitRepository) GetCommits(databaseId, clusterId string) ([]Commit, error) {
	var commits []Commit
	query := c.DB
	if databaseId != "" {
		query = query.Where(`"databaseId" = ?`, databaseId)
	}
	if clusterId != "" {
		query = query.Where(`"clusterId" = ?`, clusterId)
	}
	if err := query.Find(&commits).Error; err != nil {
		return nil, err
	}
	return commits, nil
}

func (c CommitRepository) GetCommitsByIds(ids []string) ([]Commit, error) {
	var commits []Commit
	query := c.DB.Where(`"id" IN ?`, ids)
	if err := query.Find(&commits).Error; err != nil {
		return nil, err
	}
	return commits, nil
}

func (c CommitRepository) SaveCommitWithTx(tx *gorm.DB, commit Commit) (Commit, error) {
	if err := tx.Create(&commit).Error; err != nil {
		return commit, err
	}
	return commit, nil
}

func (c CommitRepository) UpdateCommits(commits []Commit, isExecuted bool) {
	c.DB.Model(&commits).Update("isExecuted", false)
}
