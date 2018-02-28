package kafka

import (
	"github.com/Shopify/sarama/mocks"
	"testing"
)

func TestAvroProducer_Add(t *testing.T) {
	producerMock := mocks.NewSyncProducer(t, nil)
	producerMock.ExpectSendMessageAndSucceed()
	schemaRegistryTestObject := createSchemaRegistryTestObject(t, "test", 1)
	schemaRegistryMock := NewCachedSchemaRegistryClient([]string{schemaRegistryTestObject.MockServer.URL})
	avroProducer := &AvroProducer{producerMock, schemaRegistryMock}
	defer avroProducer.Close()
	err := avroProducer.Add("test", schemaRegistryTestObject.Codec.Schema(), []byte("key"), []byte(`{"val":1}`))
	if nil != err {
		t.Errorf("Error adding msg: %v", err)
	}
}
