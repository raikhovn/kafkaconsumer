package consumer

import (
	"errors"
	"fmt"
	"time"
)

// Error result type
type EventErr struct {
	Error error
	Event []byte
}

// Handler task implements "type Executable interface" expected by Threadpool
type HandlerTask struct{}

func NewHandlerTask() *HandlerTask {
	return &HandlerTask{}
}

func (t *HandlerTask) Execute(input interface{}) interface{} {
	startTime := time.Now()

	// Formatting errors
	var ret []*EventErr

	// Expect input interface{} type to be map[string]interface{}
	inmap, ok := input.(map[string]interface{})
	if ok == false {
		return errors.New("Expecting inout type: map[string]interface{}")
	}

	// Get batch to process
	bv, ok := inmap["batch"]
	if ok == false {
		return errors.New("Failed to find input 'batch'")
	}

	// Get region
	rv, ok := inmap["region"]
	if ok == false {
		fmt.Printf("Failed to find input 'region'")
	} else {
		fmt.Printf("Processing events from the reagion: %s", rv.(string))
	}

	// Get event schema folder
	fldv, ok := inmap["evtschemafolder"]
	if ok == false {
		fmt.Printf("Missing 'evtschemafolder' assumes batch payload is not formatted.")
	} else {
		fmt.Printf("'evtschemafolder' points to folder: %s, assumes batch payload is formatted.", fldv.(string))
		// Get event schema file
		_, ok := inmap["evtschemafile"]
		if ok == false {
			return errors.New("Failed to find input 'evtschemafile' while 'evtschemafolder' was specified.")
		}
	}

	// TODO: deal with other params like 'batchurl', 'httptimeout' or other handler params

	// Expect batch of events
	batch := bv.([][]byte)
	fmt.Printf("Reading started...\n")

	endTime := time.Now()
	for _, ev := range batch {
		time.Sleep(2 * time.Second)
		fmt.Printf("Reading data: %s\n", string(ev))
	}
	fmt.Printf("Reading completed. Time(ms): %d\n", endTime.Sub(startTime)/time.Millisecond)
	return endTime.Sub(startTime) / time.Millisecond

}
