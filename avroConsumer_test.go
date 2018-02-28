package kafka

import (
	"encoding/binary"
	"github.com/Shopify/sarama"
	"github.com/linkedin/goavro"
	"testing"
)

var testData = `{"val":1}`

func getTestAvroMsg(t *testing.T, codec *goavro.Codec) []byte {
	native, _, err := codec.NativeFromTextual([]byte(testData))
	binaryValue, err := codec.BinaryFromNative(nil, native)
	if err != nil {
		t.Errorf("Error get binary from native: %v", err)
	}
	var binaryMsg []byte
	binaryMsg = append(binaryMsg, byte(0))
	binarySchemaId := make([]byte, 4)
	binary.BigEndian.PutUint32(binarySchemaId, uint32(1))
	binaryMsg = append(binaryMsg, binarySchemaId...)
	binaryMsg = append(binaryMsg, binaryValue...)
	return binaryMsg
}

func TestAvroConsumer_ProcessAvroMsg(t *testing.T) {
	schemaRegistryTestObject := createSchemaRegistryTestObject(t, "test", 1)
	schemaRegistryMock := NewCachedSchemaRegistryClient([]string{schemaRegistryTestObject.MockServer.URL})
	callbacks := &ConsumerCallbacks{}
	avroConsumer := &avroConsumer{nil, schemaRegistryMock, *callbacks}
	consumerMsg := &sarama.ConsumerMessage{
		Value:     getTestAvroMsg(t, schemaRegistryTestObject.Codec),
		Key:       []byte("key"),
		Topic:     "test",
		Partition: 0,
		Offset:    1,
	}
	msg, err := avroConsumer.ProcessAvroMsg(consumerMsg)
	if err != nil {
		t.Errorf("Error process avro msg: %v", err)
	}
	if msg.Value != testData {
		t.Errorf("Wrong data")
	}
}
