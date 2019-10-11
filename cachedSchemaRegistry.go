package kafka

import (
	"github.com/linkedin/goavro/v2"
	"sync"
)

// CachedSchemaRegistryClient is a schema registry client that will cache some data to improve performance
type CachedSchemaRegistryClient struct {
	SchemaRegistryClient *SchemaRegistryClient
	schemaCache          map[int]*goavro.Codec
	schemaCacheLock      sync.RWMutex
	schemaIdCache        map[string]int
	schemaIdCacheLock    sync.RWMutex
}

func NewCachedSchemaRegistryClient(connect []string) *CachedSchemaRegistryClient {
	SchemaRegistryClient := NewSchemaRegistryClient(connect)
	return &CachedSchemaRegistryClient{SchemaRegistryClient: SchemaRegistryClient, schemaCache: make(map[int]*goavro.Codec), schemaIdCache: make(map[string]int)}
}

func NewCachedSchemaRegistryClientWithRetries(connect []string, retries int) *CachedSchemaRegistryClient {
	SchemaRegistryClient := NewSchemaRegistryClientWithRetries(connect, retries)
	return &CachedSchemaRegistryClient{SchemaRegistryClient: SchemaRegistryClient, schemaCache: make(map[int]*goavro.Codec), schemaIdCache: make(map[string]int)}
}

// GetSchema will return and cache the codec with the given id
func (client *CachedSchemaRegistryClient) GetSchema(id int) (*goavro.Codec, error) {
	client.schemaCacheLock.RLock()
	cachedResult := client.schemaCache[id]
	client.schemaCacheLock.RUnlock()
	if nil != cachedResult {
		return cachedResult, nil
	}
	codec, err := client.SchemaRegistryClient.GetSchema(id)
	if err != nil {
		return nil, err
	}
	client.schemaCacheLock.Lock()
	client.schemaCache[id] = codec
	client.schemaCacheLock.Unlock()
	return codec, nil
}

// GetSubjects returns a list of subjects
func (client *CachedSchemaRegistryClient) GetSubjects() ([]string, error) {
	return client.SchemaRegistryClient.GetSubjects()
}

// GetVersions returns a list of all versions of a subject
func (client *CachedSchemaRegistryClient) GetVersions(subject string) ([]int, error) {
	return client.SchemaRegistryClient.GetVersions(subject)
}

// GetSchemaByVersion returns the codec for a specific version of a subject
func (client *CachedSchemaRegistryClient) GetSchemaByVersion(subject string, version int) (*goavro.Codec, error) {
	return client.SchemaRegistryClient.GetSchemaByVersion(subject, version)
}

// GetLatestSchema returns the highest version schema for a subject
func (client *CachedSchemaRegistryClient) GetLatestSchema(subject string) (*goavro.Codec, error) {
	return client.SchemaRegistryClient.GetLatestSchema(subject)
}

// CreateSubject will return and cache the id with the given codec
func (client *CachedSchemaRegistryClient) CreateSubject(subject string, codec *goavro.Codec) (int, error) {
	schemaJson := codec.Schema()
	client.schemaIdCacheLock.RLock()
	cachedResult, found := client.schemaIdCache[schemaJson]
	client.schemaIdCacheLock.RUnlock()
	if found {
		return cachedResult, nil
	}
	id, err := client.SchemaRegistryClient.CreateSubject(subject, codec)
	if err != nil {
		return 0, err
	}
	client.schemaIdCacheLock.Lock()
	client.schemaIdCache[schemaJson] = id
	client.schemaIdCacheLock.Unlock()
	return id, nil
}

// IsSchemaRegistered checks if a specific codec is already registered to a subject
func (client *CachedSchemaRegistryClient) IsSchemaRegistered(subject string, codec *goavro.Codec) (int, error) {
	return client.SchemaRegistryClient.IsSchemaRegistered(subject, codec)
}

// DeleteSubject deletes the subject, should only be used in development
func (client *CachedSchemaRegistryClient) DeleteSubject(subject string) error {
	return client.SchemaRegistryClient.DeleteSubject(subject)
}

// DeleteVersion deletes the a specific version of a subject, should only be used in development.
func (client *CachedSchemaRegistryClient) DeleteVersion(subject string, version int) error {
	return client.SchemaRegistryClient.DeleteVersion(subject, version)
}
