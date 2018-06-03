package kafka

import (
	"testing"
)

func TestCachedSchemaRegistryClient_GetSchema(t *testing.T) {
	testObject := createSchemaRegistryTestObject(t, "test", 1)
	mockServer := testObject.MockServer
	defer mockServer.Close()
	client := NewCachedSchemaRegistryClient([]string{mockServer.URL})
	client.GetSchema(1)
	responseCodec, err := client.GetSchema(1)
	if nil != err {
		t.Errorf("Error getting schema: %s", err.Error())
	}
	if responseCodec.Schema() != testObject.Codec.Schema() {
		t.Errorf("Schemas do not match. Expected: %s, got: %s", testObject.Codec.Schema(), responseCodec.Schema())
	}
	if testObject.Count > 1 {
		t.Errorf("Expected call count of 1, got %d", testObject.Count)
	}
}

func TestCachedSchemaRegistryClient_GetSubjects(t *testing.T) {
	testObject := createSchemaRegistryTestObject(t, "test", 1)
	mockServer := testObject.MockServer
	defer mockServer.Close()
	client := NewCachedSchemaRegistryClient([]string{mockServer.URL})
	subjects, err := client.GetSubjects()
	if nil != err {
		t.Errorf("Error getting subjects: %v", err)
	}
	if !containsStr(subjects, testObject.Subject) {
		t.Errorf("Could not find subject")
	}
}

func TestCachedSchemaRegistryClient_GetVersions(t *testing.T) {
	testObject := createSchemaRegistryTestObject(t, "test", 1)
	mockServer := testObject.MockServer
	defer mockServer.Close()
	client := NewCachedSchemaRegistryClient([]string{mockServer.URL})
	versions, err := client.GetVersions(testObject.Subject)
	if nil != err {
		t.Errorf("Error getting versions: %v", err)
	}
	if !containsInt(versions, testObject.Id) {
		t.Errorf("Could not find version")
	}
}

func TestCachedSchemaRegistryClient_GetSchemaByVersion(t *testing.T) {
	testObject := createSchemaRegistryTestObject(t, "test", 1)
	mockServer := testObject.MockServer
	defer mockServer.Close()
	client := NewCachedSchemaRegistryClient([]string{mockServer.URL})
	responseCodec, err := client.GetSchemaByVersion(testObject.Subject, 1)
	if nil != err {
		t.Errorf("Error getting schema versions: %v", err)
	}
	if responseCodec.Schema() != testObject.Codec.Schema() {
		t.Errorf("Schemas do not match. Expected: %s, got: %s", testObject.Codec.Schema(), responseCodec.Schema())
	}
}

func TestCachedSchemaRegistryClient_GetLatestSchema(t *testing.T) {
	testObject := createSchemaRegistryTestObject(t, "test", 1)
	mockServer := testObject.MockServer
	defer mockServer.Close()
	client := NewCachedSchemaRegistryClient([]string{mockServer.URL})
	responseCodec, err := client.GetLatestSchema(testObject.Subject)
	if nil != err {
		t.Errorf("Error getting latest schema: %v", err)
	}
	if responseCodec.Schema() != testObject.Codec.Schema() {
		t.Errorf("Schemas do not match. Expected: %s, got: %s", testObject.Codec.Schema(), responseCodec.Schema())
	}
}

func TestCachedSchemaRegistryClient_CreateSubject(t *testing.T) {
	testObject := createSchemaRegistryTestObject(t, "test", 1)
	mockServer := testObject.MockServer
	defer mockServer.Close()
	client := NewCachedSchemaRegistryClient([]string{mockServer.URL})
	id, err := client.CreateSubject(testObject.Subject, testObject.Codec)
	if nil != err {
		t.Errorf("Error getting schema: %s", err.Error())
	}
	if id != testObject.Id {
		t.Errorf("Ids do not match. Expected: 1, got: %d", id)
	}
	sameid, err := client.CreateSubject(testObject.Subject, testObject.Codec)
	if nil != err {
		t.Errorf("Error getting schema: %s", err.Error())
	}
	if sameid != id {
		t.Errorf("Ids do not match. Expected: %d, got: %d", id, sameid)
	}
	if testObject.Count > 1 {
		t.Errorf("Expected call count of 1, got %d", testObject.Count)
	}
}

func TestCachedSchemaRegistryClient_IsSchemaRegistered(t *testing.T) {
	testObject := createSchemaRegistryTestObject(t, "test", 1)
	mockServer := testObject.MockServer
	defer mockServer.Close()
	client := NewCachedSchemaRegistryClient([]string{mockServer.URL})
	id, err := client.IsSchemaRegistered(testObject.Subject, testObject.Codec)
	if nil != err {
		t.Errorf("Error getting schema id: %v", err)
	}
	if nil != err {
		t.Errorf("Error getting schema: %s", err.Error())
	}
	if id != testObject.Id {
		t.Errorf("Ids do not match. Expected: 1, got: %d", id)
	}
}

func TestCachedSchemaRegistryClient_DeleteSubject(t *testing.T) {
	testObject := createSchemaRegistryTestObject(t, "test", 1)
	mockServer := testObject.MockServer
	defer mockServer.Close()
	client := NewCachedSchemaRegistryClient([]string{mockServer.URL})
	err := client.DeleteSubject(testObject.Subject)
	if nil != err {
		t.Errorf("Error delete subject: %v", err)
	}
}

func TestCachedSchemaRegistryClient_DeleteVersion(t *testing.T) {
	testObject := createSchemaRegistryTestObject(t, "test", 1)
	mockServer := testObject.MockServer
	defer mockServer.Close()
	client := NewCachedSchemaRegistryClient([]string{mockServer.URL})
	err := client.DeleteVersion(testObject.Subject, 1)
	if nil != err {
		t.Errorf("Error delete version: %v", err)
	}
}
