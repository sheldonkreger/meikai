package main

import (
	"fmt"
	"io"
	"net/http"
)

func testTronApi(conf Conf) {
	// URL includes the contract ID for Tether on Tron. Limit to only confirmed transactions with 200 results (max allowed).
	url := conf.tronnetURL + "accounts/TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t/transactions/trc20?only_confirmed=true&limit=1"
	//url := conf.tronnetURL + "contracts/TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t/transactions?only_confirmed=true&limit=200"
	//url := conf.tronnetURL + "v1/blocks/latest/events"
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
	// extract "decimals" here and return
}
