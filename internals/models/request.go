package models

type CommitRequest struct {
	Title         string   `json:"title"`
	DatabaseId    string   `json:"databaseId"`
	ClusterId     string   `json:"clusterId"`
	Queries       []string `json:"queries"`
	RevertQueries []string `json:"revertQueries"`
}
