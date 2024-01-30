package repository

import "gorm.io/gorm"

type Repository struct {
	*gorm.DB
}

func NewRepository(db *gorm.DB) Repository {
	return Repository{db}
}
