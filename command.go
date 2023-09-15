package main

import (
	"fmt"
	"os"

	"github.com/akamensky/argparse"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/confluentinc/confluent-kafka-go/schemaregistry"

	"github.com/magiconair/properties"
)

type SubCommand string

type DataFormat string

const (
	Produce SubCommand = "produce"
	Consume SubCommand = "consume"

	String   DataFormat = "STRING"
	Avro     DataFormat = "AVRO"
	Json     DataFormat = "JSON"
	Protobuf DataFormat = "PROTOBUF"
)

type KafkaGenDataCommand struct {
	TermChan  chan bool
	DoneChan  chan bool
	ErrorChan chan string

	SubCommand
	Format          DataFormat
	BootstrapServer string
	ConfigFile      string
	SRConfigFile    string
	SchemaFile      string
	Topic           string
	SampleData      string
	RecordCount     int
	TPS             int
	Earliest        bool
	Latest          bool
	Properties      *properties.Properties
}

func (k *KafkaGenDataCommand) String() string {
	return fmt.Sprintf("SubCommand: %s, Format: %s, BootstrapServer: %s, ConfigFile: %s, SRConfigFile: %s, Topic: %s, SampleData: %s, RecordCount: %d, TPS: %d, Earliest: %v, Latest: %v",
		k.SubCommand, k.Format,
		k.BootstrapServer, k.ConfigFile, k.SRConfigFile, k.Topic,
		k.SampleData, k.RecordCount, k.TPS,
		k.Earliest, k.Latest)
}

func (k *KafkaGenDataCommand) getClientConfig() (kafka.ConfigMap, error) {
	configMap := make(kafka.ConfigMap)

	for _, key := range k.Properties.Keys() {
		configMap.SetKey(key, k.Properties.MustGet(key))
	}
	configMap.SetKey("bootstrap.servers", k.BootstrapServer)
	return configMap, nil
}

func (k *KafkaGenDataCommand) getSRClientConfig() (schemaregistry.Config, bool) {
	var ok bool
	configMap := schemaregistry.Config{}
	configMap.SchemaRegistryURL, ok = k.Properties.Get("schema.registry.url")
	if ok {
		configMap.BasicAuthCredentialsSource, _ = k.Properties.Get("basic.auth.credentials.source")
		configMap.BasicAuthUserInfo, _ = k.Properties.Get("basic.auth.user.info")
		// TODO add more properties
		return configMap, true
	}

	return configMap, false
}

func (k *KafkaGenDataCommand) getProducerConfig() (kafka.ConfigMap, error) {
	// TODO remove irrelevant producer config entries
	return k.getClientConfig()
}

func (k *KafkaGenDataCommand) doProduce() error {
	fmt.Println("doProduce")

	parallelProducer := KafkaGenDataProducer{}
	config, err := k.getProducerConfig()
	if err == nil {
		srConfig, ok := k.getSRClientConfig()
		if ok {
			parallelProducer.GenData(&config, &srConfig, k.Topic, k.RecordCount, k.TPS, k.SampleData)
		} else {
			parallelProducer.GenData(&config, nil, k.Topic, k.RecordCount, k.TPS, k.SampleData)
		}
	}
	return err
}

func (k *KafkaGenDataCommand) doConsume() error {
	fmt.Println("doConsume")
	return nil
}

func (k *KafkaGenDataCommand) Process() error {
	k.Properties = properties.MustLoadFile(k.ConfigFile, properties.UTF8)

	switch k.SubCommand {
	case Produce:
		return k.doProduce()
	case Consume:
		return k.doConsume()
	default:
		return fmt.Errorf("bad subcommand")
	}
}

func (k *KafkaGenDataCommand) Validate() error {
	parser := argparse.NewParser("v-worklog", "Generates worklog for the current quarter")

	producer := parser.NewCommand("produce", "Produce records to Kafka")
	consumer := parser.NewCommand("consume", "Consume records from Kafka")

	format := parser.String("f", "format", &argparse.Options{Help: "Data Format. STRING, JSON, AVRO, PROTOBUF, BYTE", Default: "STRING"})

	bootstrapServer := parser.String("b", "bootstrap-server", &argparse.Options{Required: true, Help: "Bootstrap Server"})
	configFile := parser.String("c", "command-config", &argparse.Options{Required: false, Help: "Command Config File"})
	srConfigFile := parser.String("s", "sr-config", &argparse.Options{Required: false, Help: "Schema Registry Config File"})
	schemaFile := parser.String("", "schema-file", &argparse.Options{Required: false, Help: "Schema File"})
	topic := parser.String("t", "topic", &argparse.Options{Required: true, Help: "Topic Name"})

	sampleDataType := producer.String("d", "sample-datatype", &argparse.Options{Required: false, Help: "Sample Data Type. users, pageviews", Default: "users"})
	recordCount := producer.Int("n", "record-count", &argparse.Options{Required: false, Help: "# of records", Default: 100})
	tps := producer.Int("p", "tps", &argparse.Options{Required: false, Help: "Transaction per second. -1 = no throttle", Default: -1})

	earliest := consumer.Flag("e", "from-earliest", &argparse.Options{Required: false, Help: "From Earliest", Default: false})
	latest := consumer.Flag("l", "from-latest", &argparse.Options{Required: false, Help: "From Latest", Default: true})

	if err := parser.Parse(os.Args); err != nil {
		return fmt.Errorf(parser.Usage(err))
	}

	k.Format = DataFormat(*format)
	k.BootstrapServer = *bootstrapServer
	k.ConfigFile = *configFile
	k.SRConfigFile = *srConfigFile
	k.SchemaFile = *schemaFile
	k.Topic = *topic

	switch {
	case producer.Happened():
		k.SubCommand = Produce
		k.SampleData = *sampleDataType
		k.RecordCount = *recordCount
		k.TPS = *tps
	case consumer.Happened():
		k.SubCommand = Consume
		k.Earliest = *earliest
		k.Latest = *latest
	default:
		return fmt.Errorf(parser.Usage("bad command"))
	}

	switch k.Format {
	case String, Avro, Json, Protobuf:
		// do nothing
	default:
		return fmt.Errorf(parser.Usage("bad format"))
	}
	fmt.Printf("k => %s\n", k.String())

	return nil
}
