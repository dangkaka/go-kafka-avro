package main

import (
	"flag"
	"fmt"
	"time"

	"github.com/dangkaka/go-kafka-avro"
	"github.com/linkedin/goavro/v2"
)

var kafkaServers = []string{"localhost:9092"}
var schemaRegistryServers = []string{"http://localhost:8081"}
var topic = "test"

func main() {
	var n int
	schema := `{
				"type": "record",
				"name": "Example",
				"fields": [
					{"name": "Id", "type": "string"},
					{"name": "Type", "type": "string"},
					{"name": "Data", "type": "string"}
				]
			}`
	producer, err := kafka.NewAvroProducer(kafkaServers, schemaRegistryServers[0])
	if err != nil {
		fmt.Printf("Could not create avro producer: %s", err)
	}
	flag.IntVar(&n, "n", 1, "number")
	flag.Parse()
	for i := 0; i < n; i++ {
		fmt.Println(i)
		addMsg(producer, schema)
	}
}

func addMsg(producer *kafka.AvroProducer, schema string) {
	value := `{
		"Id": "1",
		"Type": "example_type",
		"Data": "example_data"
	}`
	key := time.Now().String()
	codec, err := goavro.NewCodec(schema)
	if err != nil {
		fmt.Printf("Could not create avro codec: %s", err)
		return
	}
	err = producer.Add(codec, topic, 1, []byte(key), []byte(value))
	fmt.Println(key)
	if err != nil {
		fmt.Printf("Could not add a msg: %s", err)
	}
}
