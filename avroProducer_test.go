package kafka

import (
	"testing"

	"github.com/Shopify/sarama/mocks"
	"github.com/riferrei/srclient"
)

func TestAvroProducer_Add(t *testing.T) {
	producerMock := mocks.NewSyncProducer(t, nil)
	producerMock.ExpectSendMessageAndSucceed()
	schemaRegistryTestObject := createSchemaRegistryTestObject(t, "test", 1)
	schemaRegistryMock := srclient.CreateSchemaRegistryClient(schemaRegistryTestObject.MockServer.URL)
	avroProducer := &AvroProducer{producerMock, schemaRegistryMock}
	defer avroProducer.Close()
	err := avroProducer.Add(schemaRegistryTestObject.Codec, "test", 1, []byte("key"), []byte(`{"val":1}`))
	if nil != err {
		t.Errorf("Error adding msg: %v", err)
	}
}
