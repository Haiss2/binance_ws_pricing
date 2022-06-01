// "go.mongodb.org/mongo-driver/bson"
package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func main() {

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	client, err := mongo.Connect(ctx, options.Client().ApplyURI("mongodb://root:example@localhost:27017"))
	if err != nil {
		log.Fatal(err)
	}

	db := client.Database("pricing_sample")

	res, err := db.ListCollectionNames(context.TODO(), bson.D{})
	if err != nil {
		log.Fatal(err)
	}

	opts := options.CreateIndexes().SetMaxTime(10 * time.Second)
	for _, coll := range res {

		indexView := db.Collection(coll).Indexes()
		model := mongo.IndexModel{Keys: bson.D{{"timestamp", 1}}}

		names, err := indexView.CreateOne(context.TODO(), model, opts)
		if err != nil {
			log.Fatal(err)
		}

		fmt.Printf("created indexes %v\n", names)
	}

	/*
		// Code for counting document
		var sum int64 = 0
		for _, coll := range res {
			n, err := db.Collection(coll).CountDocuments(context.TODO(), bson.D{})
			if err != nil {
				log.Fatal(err)
			}
			sum += n
			// fmt.Println(coll, n)
		}
		fmt.Println("total: ", sum)
	*/
}
