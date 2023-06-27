package main

import (
	"database/sql"
	"fmt"
	"log"
	"os"
	"os/signal"
	"time"

	_ "github.com/ClickHouse/clickhouse-go"
	"github.com/Shopify/sarama"
)

type ServerEvent struct {
	Topic     string
	Timestamp time.Time
	Message   string
}

func main() {
	// Kafka broker addresses
	brokers := []string{"localhost:9092"}

	// Create a new Kafka consumer
	consumer, err := sarama.NewConsumer(brokers, nil)
	if err != nil {
		log.Fatal("Failed to create consumer:", err)
	}
	defer consumer.Close()

	// Create a new Kafka topic for streaming data
	topic := "server-001-health"

	// Create a new Kafka partition consumer
	partitionConsumer, err := consumer.ConsumePartition(topic, 0, sarama.OffsetNewest)
	if err != nil {
		log.Fatal("Failed to create partition consumer:", err)
	}
	defer partitionConsumer.Close()

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)

	// Handle interrupt signal to gracefully exit the consumer
	go func() {
		<-signals
		log.Println("Received interrupt signal. Closing consumer...")
		partitionConsumer.AsyncClose()
	}()

	// Process received messages
	for msg := range partitionConsumer.Messages() {
		event := ServerEvent{
			Topic:     topic,
			Timestamp: time.Now(),
			Message:   string(msg.Value),
		}

		// Save the topic and message with the timestamp into ClickHouse
		err := saveEventToClickHouse(event)
		if err != nil {
			log.Fatal(err)
		}

		log.Printf("Received message. Partition: %d, Offset: %d, Value: %s\n",
			msg.Partition, msg.Offset, string(msg.Value))

		time.Sleep(time.Second * 1)
	}
}

func saveEventToClickHouse(event ServerEvent) error {
	// Create a ClickHouse connection
	connect, err := sql.Open("clickhouse", "tcp://localhost:9000")
	if err != nil {
		return err
	}
	defer connect.Close()

	// Begin a new transaction
	tx, err := connect.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	// Prepare the INSERT statement
	stmt, err := tx.Prepare("INSERT INTO events (topic, timestamp, message) VALUES (?, ?, ?)")
	if err != nil {
		return err
	}
	defer stmt.Close()

	// Execute the INSERT statement with event data
	_, err = stmt.Exec(event.Topic, event.Timestamp, event.Message)
	if err != nil {
		return err
	}

	// Commit the transaction
	err = tx.Commit()
	if err != nil {
		return err
	}

	fmt.Println("Event saved to ClickHouse ")
	return nil
}
