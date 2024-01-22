package utils

import (
	"butler-server/client"
	"encoding/json"
)

func GetClusterData(redisClient *client.RedisClient, clusterId string) (client.ClusterData, error) {
	clusterData, err := redisClient.GetString(redisClient.GenerateClusterKey(clusterId))
	if err != nil {
		return client.ClusterData{}, err
	}
	var data client.ClusterData
	if err := json.Unmarshal([]byte(clusterData), &data); err != nil {
		return client.ClusterData{}, err
	}
	return data, nil
}
