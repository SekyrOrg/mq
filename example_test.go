package mq

import (
	"bytes"
	"fmt"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/assert"
	"testing"
)

var exampleSchema = []byte(`
exchanges:
  exchange0:
    durable: true
    type: topic
queues:
  queue0:
    durable: true
    bindings:
      - exchange: "exchange0"
        key: "key1"
`)

func Test_Example(t *testing.T) {
	// Create a new channel
	conn, err := NewConnection("amqp://user:bitnami@localhost:5672/")
	assert.NoError(t, err)
	defer conn.Close()

	// Create a new scheme
	schema, err := NewSchemaFromYaml(bytes.NewReader(exampleSchema))
	assert.NoError(t, err)
	err = schema.Create(conn)
	assert.NoError(t, err)

	defer schema.Delete(conn)
	go conn.Queue("queue0").Consume(func(ch *amqp.Channel, msg *amqp.Delivery) {
		fmt.Println(string(msg.Body))
	})

	// Create a new exchange
	ex0 := conn.Exchange(WithName("exchange0"))
	// Create a new publishing
	publishing := NewPublishing([]byte("Hello World!"), WithContentType(ContentText))
	// Send a message
	err = ex0.Send("key1", publishing)
	assert.NoError(t, err)
	// Send a plain message
	err = ex0.SendText("key1", "Hello Plain!")
	assert.NoError(t, err)
	// Send a blob
	err = ex0.SendBinary("key1", []byte("Hello BLOB!"))
	assert.NoError(t, err)

}
