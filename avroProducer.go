package kafka

import (
	"encoding/binary"
	"github.com/Shopify/sarama"
	"github.com/linkedin/goavro/v2"
	"time"
)

type AvroProducer struct {
	producer             sarama.SyncProducer
	schemaRegistryClient *CachedSchemaRegistryClient
}

// NewAvroProducer is a basic producer to interact with schema registry, avro and kafka
func NewAvroProducer(kafkaServers []string, schemaRegistryServers []string) (*AvroProducer, error) {
	config := sarama.NewConfig()
	config.Version = sarama.V2_0_1_0
	config.Producer.Partitioner = sarama.NewHashPartitioner
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Return.Successes = true
	config.Producer.Compression = sarama.CompressionNone
	config.Producer.MaxMessageBytes = 10000000
	config.Producer.Retry.Max = 10
	config.Producer.Retry.Backoff = 1000 * time.Millisecond
	producer, err := sarama.NewSyncProducer(kafkaServers, config)
	if err != nil {
		return nil, err
	}
	schemaRegistryClient := NewCachedSchemaRegistryClient(schemaRegistryServers)
	return &AvroProducer{producer, schemaRegistryClient}, nil
}

//GetSchemaId get schema id from schema-registry service
func (ap *AvroProducer) GetSchemaId(topic string, avroCodec *goavro.Codec) (int, error) {
	schemaId, err := ap.schemaRegistryClient.CreateSubject(topic+"-value", avroCodec)
	if err != nil {
		return 0, err
	}
	return schemaId, nil
}

func (ap *AvroProducer) Add(topic string, schema string, key []byte, value []byte) error {
	avroCodec, err := goavro.NewCodec(schema)
	schemaId, err := ap.GetSchemaId(topic, avroCodec)
	if err != nil {
		return err
	}

	native, _, err := avroCodec.NativeFromTextual(value)
	if err != nil {
		return err
	}
	// Convert native Go form to binary Avro data
	binaryValue, err := avroCodec.BinaryFromNative(nil, native)
	if err != nil {
		return err
	}

	binaryMsg := &AvroEncoder{
		SchemaID: schemaId,
		Content:  binaryValue,
	}
	msg := &sarama.ProducerMessage{
		Topic: topic,
		Key:   sarama.StringEncoder(key),
		Value: binaryMsg,
	}
	_, _, err = ap.producer.SendMessage(msg)
	return err
}

func (ac *AvroProducer) Close() {
	ac.producer.Close()
}

// AvroEncoder encodes schemaId and Avro message.
type AvroEncoder struct {
	SchemaID int
	Content  []byte
}

// 此处无数坑，因为Confluent schema registry正对Avro序列化规则有特殊要求，不光需要序列化具体的内容，还要附加上Schema ID以及Magic Byte
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
