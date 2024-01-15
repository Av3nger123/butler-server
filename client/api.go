package client

import (
	"butler-server/config"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

type ClusterData struct {
	Cluster struct {
		ID          int       `json:"id"`
		CreatedAt   time.Time `json:"createdAt"`
		Name        string    `json:"name"`
		Host        string    `json:"host"`
		Port        string    `json:"port"`
		Username    string    `json:"username"`
		Password    string    `json:"password"`
		Driver      string    `json:"type"`
		WorkspaceID int       `json:"workspace_id"`
	} `json:"cluster"`
}

func GetClusterAPI(clusterId string) (ClusterData, error) {
	url := fmt.Sprintf("%s/api/clusters/%s?admin=true", config.GetString("NEXT_CLIENT_URL"), clusterId)
	method := "GET"
	client := &http.Client{}
	req, err := http.NewRequest(method, url, nil)

	if err != nil {
		return ClusterData{}, fmt.Errorf("failed to create HTTP request: %v", err)
	}
	req.Header.Add("api-key", config.GetString("SECRET"))

	res, err := client.Do(req)
	if err != nil {
		return ClusterData{}, fmt.Errorf("failed to perform HTTP request: %v", err)
	}
	defer res.Body.Close()

	body, err := io.ReadAll(res.Body)
	if err != nil {
		return ClusterData{}, fmt.Errorf("failed to read response body: %v", err)
	}

	var clusterData ClusterData
	err = json.Unmarshal(body, &clusterData)
	if err != nil {
		return ClusterData{}, fmt.Errorf("failed to unmarshal JSON: %v", err)
	}

	fmt.Println("Cluster ID:", clusterData.Cluster.ID)
	fmt.Println("Cluster Name:", clusterData.Cluster.Name)
	// Add more fields as needed

	return clusterData, nil
}
