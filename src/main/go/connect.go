package main

import (
	"fmt"

	"github.com/gocql/gocql"
)

func main() {
	cluster := gocql.NewCluster("127.0.0.1")
	_, err := cluster.CreateSession()
	if err != nil {
		panic(err)
	}
	fmt.Println("Connected to Cassandra")
}
