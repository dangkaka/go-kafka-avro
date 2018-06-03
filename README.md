[![codecov](https://codecov.io/gh/dangkaka/go-kafka-avro/branch/master/graph/badge.svg)](https://codecov.io/gh/dangkaka/go-kafka-avro) [![CircleCI](https://circleci.com/gh/dangkaka/go-kafka-avro.svg?style=svg)](https://circleci.com/gh/dangkaka/go-kafka-avro)

# go-kafka-avro

A library provides consumer/producer to work with kafka, avro and schema registry

## Installation

```
$ go get github.com/dangkaka/go-kafka-avro
```

### Usage

[Examples](./examples)


* Setup kafka, schema-registry
    ```
    docker-compose up -d
    ```
    
* Add test messages
    ```
    go run producer/main.go -n 10
    ```
    
* Run consumer
    ```
    go run consumer.main.go
    ```
    
* SSL support?
    Not supported


