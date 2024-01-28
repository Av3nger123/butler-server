package repository

import (
	"butler-server/client"
	"fmt"
)

type Account struct {
	UserID            string `gorm:"column:user_id"`
	Type              string
	Provider          string
	ProviderAccountID string `gorm:"column:provider_account_id"`
	RefreshToken      string `gorm:"column:refresh_token"`
	AccessToken       string `gorm:"column:access_token"`
	ExpiresAt         int
	TokenType         string `gorm:"column:token_type"`
	Scope             string
	IDToken           string `gorm:"column:id_token"`
	SessionState      string `gorm:"column:session_state"`
}

func CheckAccount(dbClient *client.Database, token string) bool {
	var account Account
	err := dbClient.Db.Where("access_token = ?", token).First(&account).Error
	if err != nil {
		// Handle error
		fmt.Println(err.Error())
		return false
	}
	return true
}
