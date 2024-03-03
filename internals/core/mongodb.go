package core

import (
	"butler-server/internals"
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type MongoDBDatabase struct {
	config DatabaseConfig
	conn   *mongo.Client
}

func (this *MongoDBDatabase) Connect() error {

	connectionString := fmt.Sprintf("mongodb://%s:%s@%s:%s",
		this.config.Username, this.config.Password, this.config.Hostname, this.config.Port)

	if this.config.Database != "" {
		connectionString += "/" + this.config.Database
	}

	clientOptions := options.Client().ApplyURI(connectionString)
	client, err := mongo.Connect(context.TODO(), clientOptions)
	if err != nil {
		return err
	}

	err = client.Ping(context.TODO(), nil)
	if err != nil {
		return err
	}

	this.conn = client
	fmt.Println("Connected to MongoDB database")
	return nil
}
func (this *MongoDBDatabase) Databases() ([]string, error) {
	ctx := context.TODO()
	databases, err := this.conn.ListDatabaseNames(ctx, nil)
	if err != nil {
		return nil, err
	}
	return databases, nil
}
func (this *MongoDBDatabase) Tables() ([]string, error) {
	ctx := context.TODO()
	collections, err := this.conn.Database(this.config.Database).ListCollectionNames(ctx, nil)
	if err != nil {
		return nil, err
	}
	return collections, nil
}

func (this *MongoDBDatabase) Metadata(table string) (map[string]internals.SchemaDetails, error) {
	return nil, nil
}

func (this *MongoDBDatabase) Data(table string, filter Filter) (map[string]interface{}, error) {
	filterBson, err := parseMongoDBFilters(filter.Filter)
	if err != nil {
		return nil, err
	}

	// parse the sort parameter into a BSON sort
	sortBson := parseMongoDBSort(filter.Sort, filter.Order)

	// Set up options for pagination
	skip, limit := parseMongoDBPagination(filter.Page, filter.Size)
	options := options.Find().SetSkip(skip).SetLimit(limit).SetSort(sortBson)

	// Perform the MongoDB find operation
	collection := this.conn.Database(this.config.Database).Collection(table)
	ctx := context.TODO()
	cursor, err := collection.Find(ctx, filterBson, options)
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)

	// Decode the results into a slice of maps
	var results []map[string]interface{}
	err = cursor.All(ctx, &results)
	if err != nil {
		return nil, err
	}

	// Get the total count of documents matching the filter
	count, err := collection.CountDocuments(ctx, filterBson)
	if err != nil {
		return nil, err
	}

	// Create the result map
	dbMap := make(map[string]interface{})
	dbMap["data"] = results
	dbMap["count"] = count
	return dbMap, nil
}

func (this *MongoDBDatabase) Query(query string, page int, size int) ([]map[string]interface{}, error) {
	return nil, nil
}

func (this *MongoDBDatabase) Close() error {
	if this.conn != nil {
		err := this.conn.Disconnect(context.TODO())
		if err != nil {
			return err
		}
		fmt.Println("Closed MongoDB database connection")
	}
	return nil
}

func parseMongoDBFilters(filters string) (bson.D, error) {
	if filters == "" {
		return bson.D{}, nil
	}

	// Split the filters string into individual filters
	filterStrings := strings.Split(filters, "|")

	// Create an array to store individual BSON filters
	var bsonFilters []bson.D

	// parse each filter string and convert it to BSON filter
	for _, filterStr := range filterStrings {
		bsonFilter, err := parseSingleMongoDBFilter(filterStr)
		if err != nil {
			return nil, err
		}
		bsonFilters = append(bsonFilters, bsonFilter)
	}

	// Combine individual BSON filters into a single BSON filter with "$and" operator
	result := bson.D{{Key: "$and", Value: bsonFilters}}

	return result, nil
}

func parseSingleMongoDBFilter(filter string) (bson.D, error) {
	// Split the filter string into its components
	parts := strings.Split(filter, ":")
	if len(parts) != 3 {
		return nil, errors.New("invalid filter format")
	}

	column := parts[0]
	operator := parts[1]
	value := parts[2]

	// Map the operator to MongoDB query format
	mongoOperator := map[string]string{
		"=":               "$eq",
		"!=":              "$ne",
		"<":               "$lt",
		">":               "$gt",
		">=":              "$gte",
		"<=":              "$lte",
		"in":              "$in",
		"not in":          "$nin",
		"is null":         "$eq",
		"is not null":     "$ne",
		"between":         "$gte",
		"not between":     "$lt",
		"contains":        "$regex",
		"not contains":    "$not_regex",
		"contains_ci":     "$regex",
		"not contains_ci": "$not_regex",
		"has suffix":      "$regex",
		"has prefix":      "$regex",
	}

	var bsonFilter bson.D
	switch operator {
	case "in", "not in":
		values := strings.Split(value, ",")
		bsonFilter = bson.D{{Key: column, Value: bson.M{mongoOperator[operator]: values}}}
	case "between", "not between":
		rangeValues := strings.Split(value, ",")
		bsonFilter = bson.D{{Key: column, Value: bson.M{mongoOperator[operator]: rangeValues}}}
	default:
		bsonFilter = bson.D{{Key: column, Value: bson.M{mongoOperator[operator]: value}}}
	}

	return bsonFilter, nil
}

func parseMongoDBSort(sort, order string) bson.D {
	if sort == "" {
		return bson.D{}
	}

	orderValue := 1
	if order == "desc" {
		orderValue = -1
	}

	return bson.D{{Key: sort, Value: orderValue}}
}

func parseMongoDBPagination(page, size string) (int64, int64) {
	pageNum := 0
	sizeNum := 10

	if page != "" {
		pageNum, _ = strconv.Atoi(page)
	}

	if size != "" {
		sizeNum, _ = strconv.Atoi(size)
	}

	skip := int64((pageNum) * sizeNum)
	limit := int64(sizeNum)

	return skip, limit
}

func (this *MongoDBDatabase) Execute(queries []string) error {
	return nil
}
