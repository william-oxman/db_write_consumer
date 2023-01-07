package main

import (
	"context"
	"db_write_consumer/db"
	"db_write_consumer/worker"
	"sync"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	mySQLClient, _ := db.NewMySQLDBClient("root", "", "localhost", 3306, "testbase")
	workers := worker.CreateGroup("localhost:9092", "testgroup", 5)
	var wg sync.WaitGroup
	for assignedPartition, w := range workers {
		w_ := w
		wg.Add(1)
		go func() {
			defer wg.Done()
			worker.StartWorker(ctx, w_, []string{"test-topic"}, mySQLClient, assignedPartition)
		}()
	}
	wg.Wait()
}
