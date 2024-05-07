package cmd

import (
	"fmt"
	"kafkaconsumer/internal/consumer"
	"os"
)

// Defaults
const MIN_COMMIT_COUNT = 10
const MIN_BATCH_SIZE = 2
const MIN_BATCH_TIMEOUT = 100
const POLLING_INTERVAL = 100

func main() {

	// TODO START: read from config
	var pregion string = "1"
	var sregion string = "2"

	//var evtschemafld string = "/schemas"
	//var evtschemafile string = "sample_avro_schema.json"
	var evtschemafld string
	var evtschemafile string
	var endpointurl string
	var endpointtimeout int = 20

	bootstrapServers := "localhost:9092"
	topics := []string{"segment-messages"}
	consumer_group := "foo"
	// TODO END

	l, err := consumer.NewListener(bootstrapServers, topics, consumer_group,
		MIN_COMMIT_COUNT, MIN_BATCH_SIZE,
		MIN_BATCH_TIMEOUT, POLLING_INTERVAL,
		pregion, sregion, evtschemafld, evtschemafile,
		endpointurl, endpointtimeout)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to create consumer: %s\n", err)
		os.Exit(1)
	}

	// Start listening
	l.Run()

}
