package main

import (
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/confluentinc/confluent-kafka-go/schemaregistry"
	"github.com/confluentinc/confluent-kafka-go/schemaregistry/serde"
	"github.com/confluentinc/confluent-kafka-go/schemaregistry/serde/avro"
)

type KafkaGenDataProducer struct {
	producer *kafka.Producer
	client   schemaregistry.Client
	avroSer  *avro.GenericSerializer
}

func (parallelProducer *KafkaGenDataProducer) String() string {
	return "Nothing to show"
}

// User is a simple record example
type User struct {
	Name           string `json:"name"`
	FavoriteNumber int64  `json:"favorite_number"`
	FavoriteColor  string `json:"favorite_color"`
}

func (user User) String() string {
	return fmt.Sprintf("Name: %s, FavoriteNumber: %d, FavoriteColor: %s", user.Name, user.FavoriteNumber, user.FavoriteColor)
}

func (kafkaGenDataProducer *KafkaGenDataProducer) maybeCreateClient(srConfig *schemaregistry.Config) (schemaregistry.Client, error) {
	return schemaregistry.NewClient(srConfig)
}

func (kafkaGenDataProducer *KafkaGenDataProducer) maybeCreateProducer(config *kafka.ConfigMap) error {
	if kafkaGenDataProducer.producer != nil {
		return nil // already created
	}
	var err error
	kafkaGenDataProducer.producer, err = kafka.NewProducer(config)

	return err
}

func (kafkaGenDataProducer *KafkaGenDataProducer) maybeSerialize(topic string, value User, srConfig *schemaregistry.Config) ([]byte, error) {
	var payload []byte
	var err error
	if srConfig != nil {
		payload, err = kafkaGenDataProducer.avroSer.Serialize(topic, value)
		if err != nil {
			return nil, err
		}
	} else {
		payload = []byte(value.String())
	}

	return payload, nil
}

func (kafkaGenDataProducer *KafkaGenDataProducer) GenData(config *kafka.ConfigMap, srConfig *schemaregistry.Config, topic string, recordCount int, tps int, sampleDataType string) error {
	fmt.Println("GenData")
	var err error

	// Produce a new record to the topic...
	if err = kafkaGenDataProducer.maybeCreateProducer(config); err != nil {
		return err
	}

	defer kafkaGenDataProducer.producer.Close() // TODO fix this later. May need to move to main()

	if srConfig != nil {
		kafkaGenDataProducer.client, err = kafkaGenDataProducer.maybeCreateClient(srConfig)
		if err != nil {
			return err
		}

		kafkaGenDataProducer.avroSer, err = avro.NewGenericSerializer(kafkaGenDataProducer.client, serde.ValueSerde, avro.NewSerializerConfig())
		if err != nil {
			return err
		}
	}

	// Handle any events that we get
	go func() {
		doTerm := false
		for !doTerm {
			// The `select` blocks until one of the `case` conditions
			// are met - therefore we run it in a Go Routine.
			select {
			case ev := <-kafkaGenDataProducer.producer.Events():
				// Look at the type of Event we've received
				switch ev.(type) {

				case *kafka.Message:
					// It's a delivery report
					km := ev.(*kafka.Message)
					if km.TopicPartition.Error != nil {
						command.ErrorChan <- fmt.Sprintf("\n**☠️ Failed to send message '%v' to topic '%v'\n\tErr: %v",
							string(km.Value),
							string(*km.TopicPartition.Topic),
							km.TopicPartition.Error)

					} else {
						log.Printf("✅ Message '%v' delivered to topic '%v' (partition %d at offset %d)\n",
							string(km.Value),
							string(*km.TopicPartition.Topic),
							km.TopicPartition.Partition,
							km.TopicPartition.Offset)
					}

				case kafka.Error:
					// It's an error
					em := ev.(kafka.Error)
					command.ErrorChan <- fmt.Sprintf("\n**☠️ Uh oh, caught an error:\n\t%v\n", em)

				default:
					// It's not anything we were expecting
					command.ErrorChan <- fmt.Sprintf("\n**Got an event that's not a Message or Error 👻\n\t%v\n", ev)

				}
			case <-command.TermChan:
				doTerm = true

			}
		}
		close(command.ErrorChan)
		close(command.DoneChan)
	}()

	// TODO handle sample data type
	// TODO handle format
	// TODO handle schema file
	var payload []byte
	for i := 0; i < recordCount; i++ {
		value := User{
			Name:           fmt.Sprintf("user id %d", i),
			FavoriteNumber: int64(i),
			FavoriteColor:  "blue",
		}

		payload, err = kafkaGenDataProducer.maybeSerialize(topic, value, srConfig)
		if err != nil {
			return err
		}

		if err := kafkaGenDataProducer.producer.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &topic,
				Partition: kafka.PartitionAny},
			Value: payload}, nil); err != nil {
			fmt.Printf("Failed to produce message: %s\n", err)
		}
		if tps > 0 {
			log.Printf("Sleeping %d milliseconds\n", (1000 / tps))
			time.Sleep(time.Duration(1000/tps) * time.Millisecond)
		}
	}

	// --
	// Flush the Producer queue
	t := 1000
	if r := kafkaGenDataProducer.producer.Flush(t); r > 0 {
		command.ErrorChan <- fmt.Sprintf("\n--\n⚠️ Failed to flush all messages after %d milliseconds. %d message(s) remain", t, r)

	} else {
		fmt.Println("\n--\n✨ All messages flushed from the queue")
	}
	// --
	// Stop listening to events and close the producer
	// We're ready to finish
	command.TermChan <- true
	// wait for go-routine to terminate
	<-command.DoneChan
	// Now we can get ready to exit
	fmt.Printf("\n-=-=\nWrapping up…\n")

	// When we're ready to return, check if the go routine has sent errors
	// Note that we're relying on the Go routine to close the channel, otherwise
	// we deadlock.
	// If there are no errors then the channel is simply closed and we read no values.
	done := false
	var e string
	for !done {
		if t, o := <-command.ErrorChan; !o {
			// o is false if we've read all the values and the channel is closed
			// If that's the case, then we're done here
			done = true
		} else {
			// We've read a value so let's concatenate it with the others
			// that we've got
			e += t
		}
	}

	if len(e) > 0 {
		// If we've got any errors, then return an error to the caller

		fmt.Printf("❌ … returning an error\n")
		return errors.New(e)
	}

	// assuming everything has gone ok return no error
	fmt.Printf("👋 … and we're done.\n")
	return nil
}
