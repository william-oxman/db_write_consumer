package worker

import (
	"context"
	"database/sql"
	pb "db_write_consumer/proto"
	"fmt"
	"os"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"google.golang.org/protobuf/proto"
)

func NewWorker(bootstrapServers string, groupId string) *kafka.Consumer {
	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": bootstrapServers,
		// Avoid connecting to IPv6 brokers:
		// This is needed for the ErrAllBrokersDown show-case below
		// when using localhost brokers on OSX, since the OSX resolver
		// will return the IPv6 addresses first.
		// You typically don't need to specify this configuration property.
		"broker.address.family":     "v4",
		"group.id":                  groupId,
		"session.timeout.ms":        6000,
		"auto.offset.reset":         "earliest",
		"enable.auto.offset.store":  false,
		"fetch.min.bytes":           5,
		"fetch.wait.max.ms":         700,
		"max.partition.fetch.bytes": 2097152,
		"fetch.max.bytes":           104857600,
		"message.max.bytes":         2000000,
	})

	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to create consumer: %s\n", err)
		os.Exit(1)
	}
	return c
}

func CreateGroup(bootstrapServers string, groupId string, numWorkers int) []*kafka.Consumer {
	consumers := []*kafka.Consumer{}
	for i := 0; i < numWorkers; i++ {
		c := NewWorker(bootstrapServers, groupId)
		consumers = append(consumers, c)
	}
	return consumers
}

func StartWorker(ctx context.Context, c *kafka.Consumer, topics []string, mySQLClient *sql.DB, assignedPartition int) {
	_ = c.SubscribeTopics(topics, nil)
	fmt.Printf("Consumer %s started\n", c)
	for {
		select {
		case <-ctx.Done():
			return
		default:
			ev, _ := c.ReadMessage(100)
			if ev == nil {
				continue
			}
			msg := &pb.Person{}
			proto.Unmarshal(ev.Value, msg)
			WriteStuff(mySQLClient, msg.Id, msg.Lastname, msg.Firstname, msg.Address, msg.City)
			if ev.Headers != nil {
				fmt.Printf("%% Headers: %v\n", ev.Headers)
			}
			_, err := c.StoreMessage(ev)
			if err != nil {
				fmt.Fprintf(os.Stderr, "%% Error storing offset after message %s:\n",
					ev.TopicPartition)
			}
		}
	}

	fmt.Printf("Closing consumer\n")
	c.Close()
}

func WriteStuff(client *sql.DB, personId string, lastName string, firstName string, address string, city string) {
	stmtIns, err := client.Prepare("INSERT INTO test_table VALUES( ?, ?, ?, ?, ? )")
	if err != nil {
		panic(err.Error())
	}
	defer stmtIns.Close()
	_, err = stmtIns.Exec(personId, lastName, firstName, address, city) // Insert tuples (PersonID, LastName, FirstName, Address, City)
	if err != nil {
		panic(err.Error())
	}
}
