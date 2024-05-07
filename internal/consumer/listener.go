package consumer

import (
	"encoding/json"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/raikhovn/threadpool"
)

func timeit(s string, startTime time.Time) {
	endTime := time.Now()
	fmt.Printf("Execution: ", s, " took (ms)", endTime.Sub(startTime)/time.Millisecond)
}

// Event batch request used for micro batches by the threadpool
type EventBatch struct {
	Events []json.RawMessage
}

// Event batch response used for micro batches by the threadpool
type EventBatchResponse struct {
	Events []json.RawMessage `json:"events"`
	Error  string            `json;"error"`
}

// Listener type defined
type Listener struct {
	Client            *kafka.Consumer
	CommitCount       int
	BatchSize         int
	BatchTimeout      int64
	PollingInterval   int
	EventSchemaFolder string
	EventSchemaFile   string
	PrRegion          string
	DrRegion          string
	Endpointurl       string
	Endpointtimeout   int
}

// New Listener implementation
func NewListener(bootServers string, topics []string, group string,
	commitcount int, batchsize int, batchtimeout int64,
	pollinginterval int, prregion string, drregion string,
	evtschemafld string, evtschemafile string,
	endpointurl string, endpointtimeout int) (*Listener, error) {

	// Create new instance
	l := new(Listener)

	// Initialize Kafka client
	var err error
	l.Client, err = kafka.NewConsumer(&kafka.ConfigMap{
		// Assign bootstrap servers and group id
		"bootstrap.servers": bootServers,
		"group.id":          group,
		// Allowed address formats (any, ipv4, ipv6)
		"broker.address.family": "v4",

		// Timeout allowed to detect "alive" client by kafka broker before rebalancing
		"session.timeout.ms": 6000,

		// Used for consumer with no commited offset for assigned partition
		// "earliest" - resume consuming before interuption
		// "latest" - consume latest offset from assigned partition
		"auto.offset.reset": "earliest",

		// Enable manual storing offsets (manual commit, rollback)
		"enable.auto.offset.store": false,

		// Assign partition asignment strategy
		// "range" - will join records from topics where # of partitions match # of consumers (drwback if # consumers <> # partitions..conusmer might sit idle)
		// "roundrobin"  - distribute records from partitions evenly across all consumer in consumer group (drawback when one consumer disconnects, part re-ssaignment = npart -1)
		// "sticky" - reduces part re assign in case of dropped consumer
		"partition.assignment.strategy": "range",

		// Allows longer buffering by Kafka. Default is 1 byte.. setting it to larger amount forces server to wait
		//"fetch.min.bytes":		200,
		//"fetch.wait.max.ms":		300,

		// Enables manual commit
		"enable.auto.commit": false,

		// TODO START: Need to fect tghe cert from chambe of secrets
		//"security.protocol": 			ssl_security_protocol,
		//"ssl.ca.location":			ssl_ca_location,
		//"ssl.certificate.location":	ssl_cert_location,
		//"ssl.key.location":			ssl_key_location
		// TODO END

	})
	if err != nil {
		return nil, err
	}

	// Subscribe to topics
	err = l.Client.SubscribeTopics(topics, nil)
	if err != nil {
		return nil, err
	}

	// Assign polling properties
	l.CommitCount = commitcount
	l.BatchSize = batchsize
	l.BatchTimeout = batchtimeout
	l.PollingInterval = pollinginterval

	// Assign region names
	l.PrRegion = prregion
	l.DrRegion = drregion

	// Assign path and name of folders of kafka payload schemas
	l.EventSchemaFile = evtschemafile
	l.EventSchemaFolder = evtschemafld

	// Assign endpoint http settings
	l.Endpointurl = endpointurl
	l.Endpointtimeout = endpointtimeout

	return l, err
}

func (l *Listener) Run() {

	// Set up channel for termination
	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)

	run := true

	// Init msg count
	msgcount := 0
	// Init msg batch
	var batch [][]byte
	// Init message batch timeout (allows to continue reading even after min msg count is not met)
	batchstart := time.Now()

	// Init threadpool
	// number of threads = MIN_COMMIT_COUNT / MIN_BATCH_SIZE
	// queue size = MIN_COMMIT_COUNT * 2( double size of the one read of kafka messages)
	tp := threadpool.NewThreadPool(l.CommitCount/l.BatchSize, int64(l.CommitCount*2))

	for run {
		// Check for exit signal
		select {
		case sig := <-sigchan:
			fmt.Printf("Caught signal %v: terminating...\n", sig)
			run = false
		default:
			t := time.Now()
			elapsed := t.Sub(batchstart)

			if msgcount == l.CommitCount || (elapsed.Milliseconds() > l.BatchTimeout && len(batch) > 0) {

				var batchsize int

				// Reached full count of the batch size
				if msgcount == l.CommitCount {
					batchsize = l.CommitCount
					// Batch timeout elapsed (batchsize is partial)
				} else {
					batchsize = len(batch)
				}

				// Split batch to smaller tasks and send them to the threadpool
				startTime := time.Now()
				var ret []*threadpool.Task

				if l.BatchSize < len(batch) {

					for l.BatchSize < len(batch) {
						// Create a task and submit smaller batch to the pool
						req := map[string]interface{}{
							"batch":           batch[0:l.BatchSize],
							"region":          l.PrRegion,
							"batchurl":        l.Endpointurl,
							"httptimeout":     l.Endpointtimeout,
							"evtschemafolder": l.EventSchemaFolder,
							"evtschemafile":   l.EventSchemaFile,
						}
						t := threadpool.Task{
							Request:  req,
							Function: NewHandlerTask(),
						}
						err := tp.Execute(&t)
						if err != nil {
							fmt.Printf("Error occured executing task: %s\n", err.Error())
						}

						// Add to the results array
						ret = append(ret, &t)
						// Adjust the batch size
						batch = batch[l.BatchSize:]
					}

				}
				// Process leftover that is beyond l.BatchSize
				if len(batch) > 0 {
					req := map[string]interface{}{
						"batch":           batch,
						"region":          l.PrRegion,
						"batchurl":        l.Endpointurl,
						"httptimeout":     l.Endpointtimeout,
						"evtschemafolder": l.EventSchemaFolder,
						"evtschemafile":   l.EventSchemaFile,
					}
					t := threadpool.Task{
						Request:  req,
						Function: NewHandlerTask(),
					}
					err := tp.Execute(&t)
					if err != nil {
						fmt.Printf("Error occured executing task: %s\n", err.Error())
					}

					// Add to the results array
					ret = append(ret, &t)
				}

				// Wait for all the tasks to finish (or timeout)
				tp.Wait()

				// Analyze results from the threadpool execution
				fmt.Printf("**** Listener completed processing ****\n")
				fmt.Printf("Processed  %d requests. Each request %d items\n, len(ret), l.Batchsize")
				for _, t := range ret {
					// Analyze response here
					if t.Response != nil {
						fmt.Printf("Worker thread execution error: %s\n", t.Response.(error).Error())
						fmt.Printf("Placing task into retry SQS and commiting offsets...\n")
					}
				}
				fmt.Printf("***************************************\n")

				// Reset batch timeout
				batchstart = time.Now()

				// Clear buffer for the next batch
				batch = batch[:0]

				// Commit read messages out of the queue
				_, err := l.Client.Commit()
				if err != nil {
					fmt.Printf("Error commiting offsets: %s\n", err.Error())
				} else {
					fmt.Printf("Commited batch of %d messages\n", batchsize)
				}

				// Poll buffer for messages
				ev := l.Client.Poll(l.PollingInterval)

				// Continue loop if there are no messages
				if ev == nil {
					continue
				}

				// If message read analyze type
				switch e := ev.(type) {
				case *kafka.Message:
					// Count messages
					msgcount += 1

					// Add to the payload
					batch = append(batch, e.Value)

					// Store offsets to be committed
					_, err := l.Client.StoreMessage(e)
					if err != nil {
						fmt.Printf("Error storing offfset after message: %s partition: %s", err.Error(), e.TopicPartition)

					}
				case *kafka.Error:
					// Errors can be ignored except if brokers are down
					fmt.Printf("Kafka client error occured: %v: %v\n", e.Code(), e)
					if e.Code() == kafka.ErrAllBrokersDown {
						run = false
					}
				default:
					fmt.Printf("Ignored %v\n", e)

				}

			}

		}
	}

	fmt.Printf("**** Closing consumer ****\n")
	tp.Close()
	l.Client.Close()
}
