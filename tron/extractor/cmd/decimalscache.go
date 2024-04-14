package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sync"
)

type DecimalsCache struct {
	cache map[string]int
	mu    sync.RWMutex
}

func NewDecimalsCache() *DecimalsCache {
	return &DecimalsCache{
		cache: make(map[string]int),
	}
}

func (c *DecimalsCache) GetDecimals(contractAddress string, conf Conf) (int, error) {
	c.mu.RLock()
	decimals, ok := c.cache[contractAddress]
	c.mu.RUnlock()
	if !ok {
		// If the decimals value is not in the cache, fetch it from the API
		decimalsFromAPI, err := getDecimalsFromAPI(contractAddress, conf)
		if err != nil {
			return 0, err
		}
		// Store the fetched decimals value in the cache
		c.mu.Lock()
		c.cache[contractAddress] = decimalsFromAPI
		c.mu.Unlock()
		return decimalsFromAPI, nil
	}
	return decimals, nil
}

func (c *DecimalsCache) SetDecimals(contractAddress string, decimals int) {
	c.mu.Lock()
	c.cache[contractAddress] = decimals
	c.mu.Unlock()
}

func getDecimalsFromAPI(contractAddress string, conf Conf) (int, error) {
	// URL includes the contract ID for Tether on Tron. Limit to only confirmed transactions with 200 results (max allowed).
	url := conf.tronnetURL + "accounts/" + contractAddress + "/transactions/trc20?only_confirmed=true&limit=1"
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
		return 0, err
	} else {
		fmt.Println(string(body))
	}
	var result map[string]interface{}
	json.Unmarshal(body, &result)

	decimals := result["data"].([]interface{})[0].(map[string]interface{})["token_info"].(map[string]interface{})["decimals"]
	decimalsInt, ok := decimals.(float64)
	if !ok {
		return 0, errors.New("Error converting contract decimals to int: " + contractAddress)
	}
	return int(decimalsInt), nil
}
