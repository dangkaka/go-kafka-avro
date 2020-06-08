package kafka

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"

	"github.com/linkedin/goavro/v2"
)

type TestObject struct {
	MockServer *httptest.Server
	Codec      *goavro.Codec
	Subject    string
	Id         int
	Count      int
}

func createSchemaRegistryTestObject(t *testing.T, topic string, id int) *TestObject {
	subject := topic + "-value"
	testObject := &TestObject{}
	testObject.Subject = subject
	testObject.Id = id
	testObject.Count = 0
	codec, err := goavro.NewCodec(`{"type": "record", "name": "test", "fields" : [{"name": "val", "type": "int", "default": 0}]}`)
	if err != nil {
		t.Errorf("Could not create codec %v", err)
	}
	testObject.Codec = codec

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		testObject.Count++
		if r.Method == "POST" {
			switch r.URL.String() {
			case fmt.Sprintf(subjectVersions, subject), fmt.Sprintf(deleteSubject, subject):
				response := idResponse{id}
				str, _ := json.Marshal(response)
				fmt.Fprintf(w, string(str))
			}
		} else if r.Method == "GET" {
			switch r.URL.String() {
			case fmt.Sprintf(schemaByID, id):
				escapedSchema := strings.Replace(codec.Schema(), "\"", "\\\"", -1)
				fmt.Fprintf(w, `{"schema": "%s"}`, escapedSchema)
			case subjects:
				response := []string{subject}
				str, _ := json.Marshal(response)
				fmt.Fprintf(w, string(str))
			case fmt.Sprintf(subjectVersions, subject):
				response := []int{id}
				str, _ := json.Marshal(response)
				fmt.Fprintf(w, string(str))
			case fmt.Sprintf(subjectByVersion, subject, "1"), fmt.Sprintf(subjectByVersion, subject, "latest"):
				response := schemaVersionResponse{subject, 1, codec.Schema(), id}
				str, _ := json.Marshal(response)
				fmt.Fprintf(w, string(str))
			}
		} else if r.Method == "DELETE" {
			switch r.URL.String() {
			case fmt.Sprintf(deleteSubject, subject),
				fmt.Sprintf(subjectByVersion, subject, fmt.Sprintf("%d", 1)):
				fmt.Fprintf(w, "1")
			}
		}

	}))
	testObject.MockServer = server
	return testObject
}

func containsStr(array []string, value string) bool {
	for _, v := range array {
		if v == value {
			return true
		}
	}
	return false
}

func containsInt(array []int, value int) bool {
	for _, v := range array {
		if v == value {
			return true
		}
	}
	return false
}

func TestSchemaRegistryClient_Retries(t *testing.T) {
	count := 0
	response := []string{"test"}
	mockServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		count++
		w.Header().Set("Content-Type", contentType)
		if count < 3 {
			http.Error(w, `{"error_code": 500, "message": "Error in the backend datastore"}`, 500)
		} else {
			str, _ := json.Marshal(response)
			fmt.Fprintf(w, string(str))
		}
	}))
	SchemaRegistryClient := NewSchemaRegistryClientWithRetries([]string{mockServer.URL}, 2)
	subjects, err := SchemaRegistryClient.GetSubjects()
	if err != nil {
		t.Errorf("Found error %s", err)
	}
	if !reflect.DeepEqual(subjects, response) {
		t.Errorf("Subjects did not match expected %s, got %s", response, subjects)
	}
	expectedCallCount := 3
	if count != expectedCallCount {
		t.Errorf("Expected error count to be %d, got %d", expectedCallCount, count)
	}
}

func TestSchemaRegistryClient_Error(t *testing.T) {
	mockServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, `{"error_code": 500, "message": "Error in the backend datastore"}`, 500)
	}))
	SchemaRegistryClient := NewSchemaRegistryClient([]string{mockServer.URL})
	_, err := SchemaRegistryClient.GetSubjects()
	expectedErr := Error{500, "Error in the backend datastore"}
	if err.Error() != expectedErr.Error() {
		t.Errorf("Expected error to be %s, got %s", expectedErr.Error(), err.Error())
	}
}
