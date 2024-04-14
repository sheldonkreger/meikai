package main

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/IBM/sarama"
)

// Response struct to parse the JSON response
type Response struct {
	Data []struct {
		Result         map[string]string `json:"result"`
		BlockNumber    int64             `json:"block_number"`
		BlockTimestamp int64             `json:"block_timestamp"`
	} `json:"data"`
	Meta struct {
		Links struct {
			Next string `json:"next"`
		} `json:"links"`
	} `json:"meta"`
}

// Message struct for Kafka message
type Message struct {
	Recipient      string `json:"recipient"`
	Sender         string `json:"sender"`
	BlockNumber    int64  `json:"block_number"`
	BlockTimestamp int64  `json:"block_timestamp"`
	ConvertedValue string `json:"converted_value"`
}

func main() {
	conf := defaultConf()
	decimalsCache := NewDecimalsCache()
	scanContract(conf, *decimalsCache, conf.tetherContractAddress)
}

func scanContract(conf Conf, decimalsCache DecimalsCache, contractAddress string) {
	var wg sync.WaitGroup
	defer wg.Wait()

	url := "https://api.trongrid.io/v1/contracts/" + contractAddress + "/events?only_confirmed=true&limit=200"
	producer, err := initKafkaProducer()
	if err != nil {
		fmt.Println("Error initializing Kafka producer:", err)
		return
	}
	defer producer.Close()

	for url != "" {
		req, err := http.NewRequest("GET", url, nil)
		if err != nil {
			fmt.Println("Error creating request:", err)
			return
		}
		req.Header.Add("accept", "application/json")
		req.Header.Add("TRON-PRO-API-KEY", conf.tronnetAPIKey)
		res, err := http.DefaultClient.Do(req)
		if err != nil {
			fmt.Println("Error:", err)
			return
		}

		body, err := io.ReadAll(res.Body)
		if err != nil {
			fmt.Println("Error reading response body:", err)
			return
		}

		var response Response
		err = json.Unmarshal(body, &response)
		if err != nil {
			fmt.Println("Error parsing JSON:", err)
			return
		}

		var messages []*sarama.ProducerMessage
		for _, event := range response.Data {
			result := event.Result
			recipient := result["to"]
			sender := result["from"]
			value := result["value"]
			blockNumber := event.BlockNumber
			blockTimestamp := event.BlockTimestamp * 1000 // Convert recipient milliseconds

			decimals, decimalErr := decimalsCache.GetDecimals(contractAddress, conf)
			if decimalErr != nil {
				decimals = 0
				fmt.Println("decimal lookup failed, using 0")
			}
			convertedValue := convertDecimalValue(value, decimals)
			// Handle 0 values
			if convertedValue == "0.000000" {
				convertedValue = value
			}
			// Create message struct
			message := Message{
				Recipient:      recipient,
				Sender:         sender,
				BlockNumber:    blockNumber,
				BlockTimestamp: blockTimestamp,
				ConvertedValue: convertedValue,
			}

			// Serialize message recipient JSON
			jsonMessage, err := json.Marshal(message)
			if err != nil {
				fmt.Println("Error serializing message recipient JSON:", err)
				return
			}

			// Create Kafka message
			msg := &sarama.ProducerMessage{
				Topic: conf.edgesTopic,
				Value: sarama.ByteEncoder(jsonMessage),
			}
			messages = append(messages, msg)
		}

		// Publish batch messages to Kafka
		if err := producer.SendMessages(messages); err != nil {
			fmt.Println("Error publishing messages to Kafka:", err)
			return
		}

		url = response.Meta.Links.Next
	}
}

func convertDecimalValue(value string, decimals int) string {
	// Pad the value with zeros to match the required length

	if len(value) < decimals {
		// Pad the value with leading zeros if necessary
		leadingZeros := decimals - len(value) + 1
		value = strings.Repeat("0", leadingZeros) + value
	}

	// Calculate the position of the decimal point
	pointPos := len(value) - decimals

	// If pointPos is negative or zero, we need to prepend zeros
	if pointPos <= 0 {
		for pointPos <= 0 {
			value = "0" + value
			pointPos++
		}
		pointPos = 1 // Set pointPos to 1 to ensure a valid slicing operation
	}

	// Insert the decimal point at the appropriate position
	result := value[:len(value)-decimals] + "." + value[len(value)-decimals:]
	return result
}

func initKafkaProducer() (sarama.SyncProducer, error) {
	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForLocal       // Only wait for the leader to ack
	config.Producer.Compression = sarama.CompressionSnappy   // Compress messages
	config.Producer.Flush.Frequency = 100 * time.Millisecond // Flush batches every 100ms
	config.Producer.Return.Successes = true

	producer, err := sarama.NewSyncProducer([]string{"localhost:9092"}, config)
	if err != nil {
		return nil, err
	}
	return producer, nil
}
