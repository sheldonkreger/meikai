package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strings"

	"github.com/IBM/sarama"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/spf13/viper"
)

// Define a struct to represent the Kafka message payload
type KafkaMessage struct {
	Recipient       string  `json:"recipient"`
	Sender          string  `json:"sender"`
	BlockNumber     int     `json:"block_number"`
	BlockTimestamp  int     `json:"block_timestamp"`
	ConvertedValue  float64 `json:"converted_value"`
	ContractAddress string  `json:"contract_address"`
}

func load_conf() {
	viper.SetDefault("kafka_broker_address", "localhost:9092")
}

func main() {
	// Set up Kafka consumer configuration
	config := sarama.NewConfig()
	config.Consumer.Return.Errors = true

	// Define your Neo4j connection and initialization here

	// Define your Kafka consumer
	consumer, err := sarama.NewConsumer(strings.Split("localhost:9092", ","), config)
	if err != nil {
		log.Fatalf("Error creating Kafka consumer: %v", err)
	}
	defer func() {
		if err := consumer.Close(); err != nil {
			log.Fatalf("Error closing Kafka consumer: %v", err)
		}
	}()

	// Define the topic you want to consume messages from
	topic := "tron.tether.edges2"

	// Start consuming messages from Kafka
	partitionConsumer, err := consumer.ConsumePartition(topic, 0, sarama.OffsetNewest)
	if err != nil {
		log.Fatalf("Error consuming partition: %v", err)
	}
	defer func() {
		if err := partitionConsumer.Close(); err != nil {
			log.Fatalf("Error closing partition consumer: %v", err)
		}
	}()

	dbUri := "neo4j://localhost" // scheme://host(:port) (default port is 7687)
	neo4jDriver, err := neo4j.NewDriverWithContext(dbUri, neo4j.BasicAuth("neo4j", "letmein!", ""))
	if err != nil {
		panic(err)
	}
	// Starting with 5.0, you can control the execution of most neo4jDriver APIs
	// To keep things simple, we create here a never-cancelling context
	// Read https://pkg.go.dev/context to learn more about contexts
	ctx := context.Background()
	// Handle neo4jDriver lifetime based on your application lifetime requirements.
	// neo4jDriver's lifetime is usually bound by the application lifetime, which usually implies one neo4jDriver instance per
	// application

	defer neo4jDriver.Close(ctx)

	for {
		select {
		case msg := <-partitionConsumer.Messages():
			// Parse the JSON payload
			var kafkaMsg KafkaMessage
			if err := json.Unmarshal(msg.Value, &kafkaMsg); err != nil {
				log.Printf("Error unmarshaling Kafka message: %v", err)
				continue
			}

			// Insert data into Neo4j
			err := insertDataIntoNeo4j(neo4jDriver, kafkaMsg)
			if err != nil {
				log.Println(err.Error())
			}
		}
	}
}

func insertDataIntoNeo4j(driver neo4j.DriverWithContext, kafkaMsg KafkaMessage) error {
	ctx := context.Background()
	session := driver.NewSession(ctx, neo4j.SessionConfig{DatabaseName: "tron"})
	defer session.Close(ctx)
	query := `
		MERGE (sender:Account {id: $sender})
		MERGE (recipient:Account {id: $recipient})
		MERGE (sender)-[transaction:SENT_TO]->(recipient)
		SET transaction.convertedvalue = $convertedValue,
			transaction.block_number = $blockNumber,
			transaction.block_timestamp = $blockTimestamp
		RETURN sender
	`
	params := map[string]interface{}{
		"sender":         kafkaMsg.Sender,
		"recipient":      kafkaMsg.Recipient,
		"convertedValue": kafkaMsg.ConvertedValue,
		"blockNumber":    kafkaMsg.BlockNumber,
		"blockTimestamp": kafkaMsg.BlockTimestamp,
	}
	// Ignore result
	_, err := neo4j.ExecuteQuery(ctx, driver, query, params, neo4j.EagerResultTransformer)
	if err != nil {
		return fmt.Errorf("Error inserting data into Neo4j: %v Query: %v Params: %v", err, query, params)
	}
	return nil
}
