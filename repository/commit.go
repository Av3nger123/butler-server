package repository

import (
	"time"

	"gorm.io/gorm"
)

type CommitRepository struct {
	Repository
}

func NewCommitRepository(repo Repository) CommitRepository {
	return CommitRepository{repo}
}

type Commit struct {
	ID         int       `gorm:"column:id" json:"id"`
	Title      string    `gorm:"column:title" json:"title"`
	CreatedAT  time.Time `gorm:"column:createdAt" json:"createdAt"`
	ExecutedAt time.Time `gorm:"column:executedAt" json:"executedAt"`
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
	commits := make([]Commit, 0)
	created := make([]Commit, 0)
	executed := make([]Commit, 0)
	query := c.DB
	if databaseId != "" {
		query = query.Where(`"databaseId" = ?`, databaseId)
	}
	if clusterId != "" {
		query = query.Where(`"clusterId" = ?`, clusterId)
	}
	if err := query.Order(`"executedAt" DESC`).Where(`"isExecuted" = true`).Find(&executed).Error; err != nil {
		return nil, err
	}
	query = c.DB
	if databaseId != "" {
		query = query.Where(`"databaseId" = ?`, databaseId)
	}
	if clusterId != "" {
		query = query.Where(`"clusterId" = ?`, clusterId)
	}
	if err := query.Order(`"createdAt" DESC`).Where(`"isExecuted" = false`).Find(&created).Error; err != nil {
		return nil, err
	}
	commits = append(commits, created...)
	commits = append(commits, executed...)
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
	c.DB.Model(&commits).Update("isExecuted", isExecuted).Update("executedAt", time.Now())
}
