package repository

import (
	"time"

	_ "github.com/jinzhu/gorm/dialects/postgres"
)

type ViewRepository struct {
	repo Repository
}

func NewViewRepository(repo Repository) ViewRepository {
	return ViewRepository{repo: repo}
}

type DataView struct {
	ID         int       `gorm:"column:id" json:"id"`
	Title      string    `gorm:"column:title" json:"title"`
	CreatedAt  time.Time `gorm:"column:createdAt" json:"createdAt"`
	ClusterID  string    `gorm:"column:clusterId" json:"clusterId"`
	DatabaseID string    `gorm:"column:databaseId" json:"databaseId"`
	Query      string    `gorm:"column:query" json:"query"`
}

func (DataView) TableName() string {
	return "dataviews"
}

func (v ViewRepository) SaveView(view DataView) error {
	if err := v.repo.Create(&view).Error; err != nil {
		return err
	}
	return nil
}

func (v ViewRepository) GetViews(clusterId, databaseId string) ([]DataView, error) {
	var views []DataView

	query := v.repo.DB

	if clusterId != "" {
		query.Where("cluster_id = ?", clusterId)
	}
	if databaseId != "" {
		query.Where("database_id = ?", databaseId)
	}

	if err := query.Find(&views).Error; err != nil {
		return nil, err
	}
	return views, nil
}
