package utils

import (
	"butler-server/client"
	"encoding/json"
	"strings"

	"github.com/xwb1989/sqlparser"
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

func ProcessQueries(queires []string) (map[string][]string, error) {
	groupedQueries := make(map[string][]string, 0)

	for _, v := range queires {
		str := strings.ReplaceAll(v, "\"", "")
		statement, err := sqlparser.Parse(str)
		if err != nil {
			return nil, err
		}
		tableName := extractTableName(statement)
		groupedQueries[tableName] = append(groupedQueries[tableName], v)
	}
	return groupedQueries, nil
}

func extractTableName(stmt sqlparser.Statement) string {
	var tableName string

	switch s := stmt.(type) {
	case *sqlparser.Select:
		if s.From != nil {
			tableExprs := s.From
			tableName = sqlparser.String(tableExprs)
		}
	case *sqlparser.Insert:
		tableName = sqlparser.String(s.Table)
	case *sqlparser.Update:
		tableName = sqlparser.String(s.TableExprs)
	case *sqlparser.Delete:
		tableName = sqlparser.String(s.TableExprs)
	}

	return tableName
}
