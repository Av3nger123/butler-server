package repository

import "github.com/jinzhu/gorm"

type Repository struct {
	*gorm.DB
}

func NewRepository(db *gorm.DB) Repository {
	return Repository{db}
}
