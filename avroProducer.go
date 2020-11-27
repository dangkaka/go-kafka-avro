package kafka

import (
	"encoding/binary"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/Shopify/sarama"
	"github.com/linkedin/goavro/v2"
	"github.com/riferrei/srclient"
	"github.com/toventang/eklog/scram"
)

type AvroProducer struct {
	producer             sarama.SyncProducer
	schemaRegistryClient *srclient.SchemaRegistryClient
}

func defaultAvroProducerConfig() *sarama.Config {
	config := sarama.NewConfig()
	config.Version = sarama.V2_0_1_0
	config.Producer.Partitioner = sarama.NewHashPartitioner
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Return.Successes = true
	config.Producer.Compression = sarama.CompressionNone
	config.Producer.MaxMessageBytes = 10000000
	config.Producer.Retry.Max = 10
	config.Producer.Retry.Backoff = 1000 * time.Millisecond
	return config
}

// NewAvroProducerWithSASL uses the SASL config of the given sarama.Config object to connect to the kafka cluster, see examples
func NewAvroProducerWithSASL(kafkaServers []string, schemaRegistryServer string, saslConfig *sarama.Config) (*AvroProducer, error) {
	config := defaultAvroProducerConfig()
	config.Net.SASL = saslConfig.Net.SASL
	producer, err := sarama.NewSyncProducer(kafkaServers, config)
	if err != nil {
		return nil, err
	}

	schemaRegistryClient := srclient.CreateSchemaRegistryClient(schemaRegistryServer)
	schemaRegistryClient.CachingEnabled(true)
	schemaRegistryClient.CodecCreationEnabled(true)
	return &AvroProducer{producer, schemaRegistryClient}, nil
}

// NewAvroProducer is a basic producer to interact with schema registry, avro and kafka
func NewAvroProducer(kafkaServers []string, schemaRegistryServer string) (*AvroProducer, error) {
	return NewAvroProducerWithSASL(kafkaServers, schemaRegistryServer, sarama.NewConfig())
}

// GetSchemaID return the value SchemaID for the given topic
// Will attempt to create a new subject if a schema is provided and subject doesn't exist
func (ap *AvroProducer) GetSchemaID(topic, schema string) (int, error) {
	srclientSchema, err := ap.schemaRegistryClient.GetLatestSchema(topic, false)
	if err == nil {
		id := srclientSchema.ID()
		return id, nil
	}

	notFoundText := fmt.Sprintf("Subject '%s-value' not found", topic)
	if !strings.Contains(err.Error(), notFoundText) || schema == "" {
		return -1, err
	}

	srclientSchema, err = ap.schemaRegistryClient.CreateSchema(topic, schema, srclient.Avro, false)
	id := srclientSchema.ID()
	return id, err
}

// PrepareMessage for submission to Kafka
// sarama.StringEncoder is used for key
func (ap *AvroProducer) PrepareMessage(codec *goavro.Codec, topic string, schemaID int, key, value []byte) (*sarama.ProducerMessage, error) {
	native, _, err := codec.NativeFromTextual(value)
	if err != nil {
		return nil, err
	}

	binaryValue, err := codec.BinaryFromNative(nil, native)
	if err != nil {
		return nil, err
	}

	binaryMsg := &AvroEncoder{
		SchemaID: schemaID,
		Content:  binaryValue,
	}

	msg := &sarama.ProducerMessage{
		Topic: topic,
		Key:   sarama.StringEncoder(key),
		Value: binaryMsg,
	}
	return msg, nil
}

// SendMessages calls the sarama producer's SendMessages method
func (ap *AvroProducer) SendMessages(msgs []*sarama.ProducerMessage) error {
	return ap.producer.SendMessages(msgs)
}

// Add is a utility method that calls PrepareMessage and sends it right away, not recommended for performance
func (ap *AvroProducer) Add(codec *goavro.Codec, topic string, schemaID int, key, value []byte) error {
	msg, err := ap.PrepareMessage(codec, topic, schemaID, key, value)
	if err != nil {
		return err
	}

	return ap.SendMessages([]*sarama.ProducerMessage{msg})
}

// Close calls the sarama producer's Close method
func (ac *AvroProducer) Close() {
	ac.producer.Close()
}

// AvroEncoder encodes schemaId and Avro message.
type AvroEncoder struct {
	SchemaID int
	Content  []byte
}

// Notice: the Confluent schema registry has special requirements for the Avro serialization rules,
// not only need to serialize the specific content, but also attach the Schema ID and Magic Byte.
// Ref: https://docs.confluent.io/current/schema-registry/serializer-formatter.html#wire-format
func (a *AvroEncoder) Encode() ([]byte, error) {
	var binaryMsg []byte
	// Confluent serialization format version number; currently always 0.
	binaryMsg = append(binaryMsg, byte(0))
	// 4-byte schema ID as returned by Schema Registry
	binarySchemaId := make([]byte, 4)
	binary.BigEndian.PutUint32(binarySchemaId, uint32(a.SchemaID))
	binaryMsg = append(binaryMsg, binarySchemaId...)
	// Avro serialized data in Avro's binary encoding
	binaryMsg = append(binaryMsg, a.Content...)
	return binaryMsg, nil
}

// Length of schemaId and Content.
func (a *AvroEncoder) Length() int {
	return 5 + len(a.Content)
}

func ExampleNewAvroProducerWithSASL_Plain() {
	mySASLConfig := sarama.NewConfig()
	mySASLConfig.Net.SASL.Enable = true
	mySASLConfig.Net.SASL.Handshake = true
	mySASLConfig.Net.SASL.User = "username"
	mySASLConfig.Net.SASL.Password = "password"

	_, err := NewAvroProducerWithSASL([]string{"0.0.0.0:9092"}, "http://0.0.0.0:8081", mySASLConfig)
	if err == nil {
		log.Println(err.Error())
	} else {
		log.Println("success")
	}
	// Output: success
}

func ExampleNewAvroProducerWithSASL_SCRAMSHA256() {
	mySASLConfig := sarama.NewConfig()
	mySASLConfig.Net.SASL.Enable = true
	mySASLConfig.Net.SASL.Mechanism = sarama.SASLTypeSCRAMSHA256
	mySASLConfig.Net.SASL.User = "username"
	mySASLConfig.Net.SASL.Password = "password"
	// Use github.com/toventang/eklog/scram for a standardized approach
	mySASLConfig.Net.SASL.SCRAMClientGeneratorFunc = func() sarama.SCRAMClient { return &scram.XDGSCRAMClient{HashGeneratorFcn: scram.SHA256} }
	mySASLConfig.Net.SASL.Mechanism = sarama.SASLMechanism(sarama.SASLTypeSCRAMSHA256)

	producer, err := NewAvroProducerWithSASL([]string{"0.0.0.0:9092"}, "http://0.0.0.0:8081", mySASLConfig)
	_, _ = producer, err //
}
