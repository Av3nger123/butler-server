package repository

import (
	"butler-server/client"
	"fmt"
)

func CheckAccount(dbClient *client.Database, token string) bool {
	query := fmt.Sprintf(`SELECT * FROM Accounts WHERE "access_token" = '%s';`, token)
	result, err := dbClient.Execute(query)
	if err != nil {
		fmt.Println(err.Error())
		return false
	}
	if len(result) > 0 {
		return true
	}
	return false
}
