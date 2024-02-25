package repository

import (
	"time"

	_ "gorm.io/gorm"
)

type ViewRepository struct {
	Repository
}

func NewViewRepository(repo Repository) ViewRepository {
	return ViewRepository{repo}
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
	if err := v.Create(&view).Error; err != nil {
		return err
	}
	return nil
}

func (v ViewRepository) GetViews(clusterId, databaseId string) ([]DataView, error) {
	var views []DataView

	query := v.DB

	if clusterId != "" {
		query = query.Where(`"clusterId" = ?`, clusterId)
	}
	if databaseId != "" {
		query = query.Where(`"databaseId" = ?`, databaseId)
	}
	if err := query.Find(&views).Error; err != nil {
		return nil, err
	}
	return views, nil
}
