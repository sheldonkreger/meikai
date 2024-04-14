package main

type Conf struct {
	tetherContractAddress string
	tronnetAPIKey         string
	tronnetURL            string
	edgesTopic            string
	verticesTopic         string
}

func defaultConf() Conf {
	conf := Conf{
		tetherContractAddress: "TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t",
		tronnetAPIKey:         "",
		tronnetURL:            "https://api.trongrid.io/v1/",
		edgesTopic:            "tron.tether.edges0",
		verticesTopic:         "tron.tether.vertices0",
	}
	return conf
}
