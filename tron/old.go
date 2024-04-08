package main

import (
	"encoding/json"
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/gocql/gocql"
	"io"
	"net/http"
	"syscall"
	"time"
)

func main() {
	fmt.Println("initializing tron-extractor")
	conf := defaultConf()
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
	transactions := fetchTronTetherTransactions(conf)
	edges := ExtractGraphEdges(transactions)
	vertices := ExtractGraphVertices(transactions)
	writeEdgesToKafka(edges, edgesProducer)
	writeVerticesToKafka(vertices, verticesProducer)

	// write edges to kafka topic called tron.tether.edges
	// write vertices to kafka topic called tron.tether.vertices

	syscall.Exit(1)
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

func fetchTronTetherTransactions(conf Conf) []Transaction {
	// URL includes the contract ID for Tether on Tron. Limit to only confirmed transactions with 200 results (max allowed).
	url := conf.tronnetURL + "accounts/TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t/transactions/trc20?only_confirmed=true"

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
	}

	// Unmarshal the JSON data into a map
	var result map[string]interface{}
	err = json.Unmarshal(body, &result)
	if err != nil {
		fmt.Println("Error unmarshalling JSON:", err)
	}

	// Check if request was successful
	success, ok := result["success"].(bool)
	if !ok || !success {
		fmt.Println("Error: Request was not successful")
	}

	// Extract transaction data
	data, ok := result["data"].([]interface{})
	if !ok {
		fmt.Println("Error: 'data' field is not an array")
	}

	// Extract Transaction structs
	transactions := make([]Transaction, 0, len(data))
	for _, item := range data {
		txMap, ok := item.(map[string]interface{})
		if !ok {
			fmt.Println("Error: 'data' item is not a map")
			continue
		}

		// Handle BlockTimestamp conversion
		blockTimestampFloat, ok := txMap["block_timestamp"].(float64)
		if !ok {
			fmt.Println("Error: 'block_timestamp' field is not a float64")
			continue
		}
		blockTimestamp := time.Unix(int64(blockTimestampFloat/1000), 0)

		transaction := Transaction{
			TransactionID:  txMap["transaction_id"].(string),
			BlockTimestamp: blockTimestamp,
			From:           txMap["from"].(string),
			To:             txMap["to"].(string),
			Type:           txMap["type"].(string),
			Value:          txMap["value"].(string),
		}

		// Extract TokenInfo
		tokenInfoMap, ok := txMap["token_info"].(map[string]interface{})
		if !ok {
			fmt.Println("Error: 'token_info' field is not a map")
			continue
		}
		tokenInfo := TokenInfo{
			Symbol:   tokenInfoMap["symbol"].(string),
			Address:  tokenInfoMap["address"].(string),
			Decimals: int(tokenInfoMap["decimals"].(float64)),
			Name:     tokenInfoMap["name"].(string),
		}
		transaction.TokenInfo = tokenInfo

		transactions = append(transactions, transaction)
	}
	return transactions
}

func ExtractGraphEdges(transactions []Transaction) []GraphEdge {
	graphEdges := make([]GraphEdge, 0, len(transactions))
	for _, txn := range transactions {
		graphEdge := GraphEdge{
			ToAddress:      txn.To,
			FromAddress:    txn.From,
			Value:          txn.Value,
			BlockTimestamp: txn.BlockTimestamp,
		}
		graphEdges = append(graphEdges, graphEdge)
	}
	return graphEdges
}

func ExtractGraphVertices(transactions []Transaction) []GraphVertex {
	uniqueAddresses := make(map[string]bool)
	for _, txn := range transactions {
		uniqueAddresses[txn.From] = true
		uniqueAddresses[txn.To] = true
	}

	graphVertices := make([]GraphVertex, 0, len(uniqueAddresses))
	for address := range uniqueAddresses {
		graphVertex := GraphVertex{
			Address: address,
		}
		graphVertices = append(graphVertices, graphVertex)
	}
	return graphVertices
}
func writeEdgesToKafka(edges []GraphEdge, producer *kafka.Producer) {
	topic := "tron.tether.edges"
	for _, edge := range edges {
		edgeJSON, err := json.Marshal(edge)
		if err != nil {
			fmt.Printf("Error marshalling edge to JSON: %s\n", err)
			continue
		}
		err1 := producer.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
			Value:          edgeJSON,
		}, nil)
		if err1 != nil {
			fmt.Println("Error publishing to "+topic+" Kafka: %s\n", err1)

		}
	}
	producer.Flush(15 * 1000)
}

func writeVerticesToKafka(vertices []GraphVertex, producer *kafka.Producer) {
	topic := "tron.tether.vertices"
	for _, vertex := range vertices {
		vertexJSON, err := json.Marshal(vertex)
		if err != nil {
			fmt.Printf("Error marshalling vertex to JSON: %s\n", err)
			continue
		}
		err1 := producer.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
			Value:          vertexJSON,
		}, nil)
		if err1 != nil {
			fmt.Println("Error publishing to "+topic+" Kafka: %s\n", err1)
		}
	}
	producer.Flush(15 * 1000)
}

func connectScylla(conf Conf) *gocql.Session {
	cluster := gocql.NewCluster("localhost")
	cluster.Authenticator = gocql.PasswordAuthenticator{Username: "scylla", Password: "your-awesome-password"}
	session, err := gocql.NewSession(*cluster)
	if err != nil {
		panic("Connection fail")
	}
	return session
}
