package main

import (
	"log"
)

var (
	command KafkaGenDataCommand
)

func main() {
	// For signalling termination from main to go-routine
	command.DoneChan = make(chan bool)
	// For signalling that termination is done from go-routine to main
	command.ErrorChan = make(chan string, 8)
	// For capturing errors from the go-routine
	command.TermChan = make(chan bool, 1)

	if err := command.Validate(); err != nil {
		log.Fatalln(err)
		return
	}
	if err := command.Process(); err != nil {
		log.Fatalln(err)
		return
	}
}
