package main

import "time"

type Transaction struct {
	TransactionID string `json:"transaction_id"`
	//TokenInfo      TokenInfo `json:"token_info"`
	BlockTimestamp time.Time `json:"block_timestamp"`
	From           string    `json:"from"`
	To             string    `json:"to"`
	Type           string    `json:"type"`
	Value          string    `json:"value"`
}

type GraphEdge struct {
	ToAddress      string
	FromAddress    string
	Value          string
	BlockTimestamp time.Time
}

type GraphVertex struct {
	Address string
}
