package main

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"github.com/IBM/sarama"
	"os"
	"os/signal"
	"syscall"
)

const (
	topic           = "tron.tether.edges0"
	csvBatchSize    = 1000000
	csvFilePrefix   = "edges"
	targetCSVNumber = 10000
)

type GraphEdge struct {
	Recipient      string
	Sender         string
	ConvertedValue string
	BlockTimestamp string
	BlockNumber    string
	EventType      string
}

func main() {
	config := sarama.NewConfig()
	config.Consumer.Return.Errors = true

	consumer, err := sarama.NewConsumer([]string{"localhost:9092"}, config)
	if err != nil {
		panic(err)
	}
	defer func() {
		if err := consumer.Close(); err != nil {
			fmt.Println("Error closing consumer:", err)
		}
	}()

	partitionConsumer, err := consumer.ConsumePartition(topic, 0, sarama.OffsetOldest)
	if err != nil {
		panic(err)
	}
	defer func() {
		if err := partitionConsumer.Close(); err != nil {
			fmt.Println("Error closing partition consumer:", err)
		}
	}()

	csvFileNumber := 1
	csvWriter, err := createCSVFile(csvFileNumber)
	if err != nil {
		panic(err)
	}
	defer csvWriter.Flush()

	messageCount := 0
	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)

ConsumerLoop:
	for {
		select {
		case msg := <-partitionConsumer.Messages():
			edge := parseMessage(msg.Value)
			writeEdgeToCSV(edge, csvWriter)

			messageCount++
			if messageCount%csvBatchSize == 0 {
				fmt.Printf("Wrote %d edges to CSV file %s.csv\n", csvBatchSize, csvFilePrefix)
				csvFileNumber++
				csvWriter.Flush()
				csvWriter, err = createCSVFile(csvFileNumber)
				if err != nil {
					fmt.Println("Error creating new CSV file:", err)
					break ConsumerLoop
				}
			}
			// Check if csvFileNumber reached the target number
			if csvFileNumber == targetCSVNumber {
				fmt.Println("Reached the target CSV file number. Exiting...")
				break ConsumerLoop
			}

		case err := <-partitionConsumer.Errors():
			fmt.Println("Error:", err)

		case <-sigchan:
			fmt.Println("Received termination signal. Exiting...")
			break ConsumerLoop
		}
	}
}

func parseMessage(value []byte) GraphEdge {
	var edge GraphEdge
	err := json.Unmarshal(value, &edge)
	if err != nil {
		fmt.Println("Error parsing JSON:", err)
		return GraphEdge{} // Return empty struct if JSON parsing fails
	}
	return edge
}

func createCSVFile(number int) (*csv.Writer, error) {
	fileName := fmt.Sprintf("%s_%d.csv", csvFilePrefix, number)
	file, err := os.Create(fileName)
	if err != nil {
		return nil, err
	}

	writer := csv.NewWriter(file)
	writer.Write([]string{"Recipient", "Sender", "ConvertedValue", "BlockTimestamp", "BlockNumber", "EventType"})
	return writer, nil
}

func writeEdgeToCSV(edge GraphEdge, writer *csv.Writer) {
	writer.Write([]string{
		edge.Recipient,
		edge.Sender,
		edge.ConvertedValue,
		edge.BlockTimestamp,
		edge.BlockNumber,
		edge.EventType,
	})
}
