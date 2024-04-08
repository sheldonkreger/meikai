package main

type Conf struct {
	tetherContractAddress string
	tronnetAPIKey         string
	tronnetURL            string
	neo4jPassword         string
	edgesTopic            string
	verticesTopic         string
}

func defaultConf() Conf {
	conf := Conf{
		tetherContractAddress: "TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t",
		tronnetAPIKey:         "dcc9834f-5bff-425a-bf2f-c23b141479ec",
		tronnetURL:            "https://api.trongrid.io/v1/",
		neo4jPassword:         "neo4j.admin",
		edgesTopic:            "tron.tether.edges0",
		verticesTopic:         "tron.tether.vertices",
	}
	return conf
}
