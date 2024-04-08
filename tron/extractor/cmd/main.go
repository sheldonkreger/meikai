package main

import (
	"encoding/json"
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"io"
	"net/http"
	"syscall"
	"time"
)

func main() {
	fmt.Println("initializing tron-extractor")
	conf := defaultConf()
	fmt.Printf("publishing to topics: %s and %s", conf.edgesTopic, conf.verticesTopic)
	//testTronApi(conf)
	// Initialize Kafka producers
	edgesProducer, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": "localhost:9092"})
	if err != nil {
		fmt.Printf("Failed to create edges producer: %s\n", err)
		return
	}
	defer edgesProducer.Close()

	verticesProducer, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": "localhost:9092"})
	if err != nil {
		fmt.Printf("Failed to create vertices producer: %s\n", err)
		return
	}
	defer verticesProducer.Close()

	err = fetchAndPublishTransactions(conf, edgesProducer, verticesProducer)
	if err != nil {
		fmt.Println("Error fetching and publishing transactions:", err)
		return
	}

	fmt.Println("All transactions fetched and published successfully.")
}

func fetchAndPublishTransactions(conf Conf, edgesProducer, verticesProducer *kafka.Producer) error {
	url := conf.tronnetURL + "accounts/TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t/transactions/trc20?only_confirmed=true&limit=200"
	for url != "" {
		transactions, nextURL, err := fetchTransactionsFromURL(url, conf.tronnetAPIKey)
		if err != nil {
			return err
		}

		err = publishTransactions(transactions, edgesProducer, verticesProducer, conf)
		if err != nil {
			return err
		}

		url = nextURL
	}
	return nil
}

func fetchTransactionsFromURL(url, apiKey string) ([]Transaction, string, error) {
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, "", err
	}
	req.Header.Add("accept", "application/json")
	req.Header.Add("TRON-PRO-API-KEY", apiKey)
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, "", err
	}
	defer res.Body.Close()
	body, err := io.ReadAll(res.Body)
	if err != nil {
		return nil, "", err
	}
	var result map[string]interface{}
	err = json.Unmarshal(body, &result)
	if err != nil {
		return nil, "", err
	}
	transactionsData, ok := result["data"].([]interface{})
	if !ok {
		return nil, "", fmt.Errorf("data not found in API response")
	}
	var transactions = extractTransactions(transactionsData)
	// Check if "meta" field exists
	meta, ok := result["meta"].(map[string]interface{})
	if !ok {
		return nil, "", fmt.Errorf("meta not found in API response")
	}

	links, ok := meta["links"].(map[string]interface{})
	if !ok {
		return nil, "", fmt.Errorf("links not found in meta field of API response")
	}
	nextURL, ok := links["next"].(string)
	if !ok {
		return nil, "", fmt.Errorf("next URL not found in links field of meta field")
	}
	url = nextURL
	return transactions, nextURL, nil
}

func publishTransactions(transactions []Transaction, edgesProducer, verticesProducer *kafka.Producer, conf Conf) error {
	for _, tx := range transactions {
		// Publish edges
		edge := GraphEdge{
			ToAddress:      tx.To,
			FromAddress:    tx.From,
			Value:          tx.Value,
			BlockTimestamp: tx.BlockTimestamp,
		}
		edgeJSON, err := json.Marshal(edge)
		if err != nil {
			return err
		}
		edgesProducer.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &conf.edgesTopic, Partition: kafka.PartitionAny},
			Value:          edgeJSON,
		}, nil)

		// Publish vertices
		vertexFrom := GraphVertex{Address: tx.From}
		vertexTo := GraphVertex{Address: tx.To}
		vertexFromJSON, err := json.Marshal(vertexFrom)
		if err != nil {
			return err
		}
		vertexToJSON, err := json.Marshal(vertexTo)
		if err != nil {
			return err
		}
		verticesProducer.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &conf.verticesTopic, Partition: kafka.PartitionAny},
			Value:          vertexFromJSON,
		}, nil)
		verticesProducer.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &conf.verticesTopic, Partition: kafka.PartitionAny},
			Value:          vertexToJSON,
		}, nil)
	}

	edgesProducer.Flush(100)
	verticesProducer.Flush(100)

	return nil
}

func extractTransactions(data []interface{}) []Transaction {
	transactions := make([]Transaction, 0, len(data))
	for _, item := range data {
		txMap, ok := item.(map[string]interface{})
		if !ok {
			fmt.Println("Error: 'data' item is not a map")
			continue
		}

		blockTimestampFloat, ok := txMap["block_timestamp"].(float64)
		if !ok {
			fmt.Println("Error: 'block_timestamp' field is not a float64")
			continue
		}
		blockTimestamp := time.Unix(int64(blockTimestampFloat/1000), 0)

		transaction := Transaction{
			TransactionID:  getStringFromMap(txMap, "transaction_id"),
			BlockTimestamp: blockTimestamp,
			From:           getStringFromMap(txMap, "from"),
			To:             getStringFromMap(txMap, "to"),
			Type:           getStringFromMap(txMap, "type"),
			Value:          getStringFromMap(txMap, "value"),
		}

		transactions = append(transactions, transaction)
	}
	return transactions
}

func getStringFromMap(m map[string]interface{}, key string) string {
	if val, ok := m[key]; ok && val != nil {
		return val.(string)
	}
	return ""
}

func testTronApi(conf Conf) {
	// URL includes the contract ID for Tether on Tron. Limit to only confirmed transactions with 200 results (max allowed).
	url := conf.tronnetURL + "accounts/TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t/transactions/trc20?only_confirmed=true&limit=200"
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		fmt.Println("Error creating request:", err)
	}
	req.Header.Add("accept", "application/json")
	req.Header.Add("TRON-PRO-API-KEY", conf.tronnetAPIKey)
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		fmt.Println("Error:", err)
	}
	defer res.Body.Close()
	body, err := io.ReadAll(res.Body)
	if err != nil {
		fmt.Println("Error reading response body:", err)
	} else {
		fmt.Println(string(body))
	}
	syscall.Exit(1)
}
