package examples

import (
	"bytes"
	"fmt"
	"github.com/SekyrOrg/mq"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
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
	// Create a new connection
	conn, err := mq.NewConnection("amqp://user:bitnami@localhost:5672/")
	assert.NoError(t, err)
	defer conn.Close()

	// Create a new scheme
	schema, err := mq.NewSchemaFromYaml(bytes.NewReader(exampleSchema))
	assert.NoError(t, err)
	err = schema.Create(conn)
	assert.NoError(t, err)
	defer schema.Delete(conn)

	// Create a new channel
	channel, err := conn.Channel()
	assert.NoError(t, err)
	defer channel.Close()

	// Consume messages from queue0
	channel.Queue("queue0").ConsumeFunc(func(msg *mq.Message) {
		fmt.Println(msg)
	})

	// Create a new publishing to send
	publishing := mq.NewPublishing([]byte("Hello World!"), mq.WithContentType(mq.ContentText))

	// Create a new exchange
	ex0 := channel.Exchange(mq.WithName("exchange0"))
	// Publish a message
	err = ex0.Publish("key1", publishing)
	assert.NoError(t, err)
	// Publish a plain message
	err = ex0.PublishText("key1", "Hello Plain!")
	assert.NoError(t, err)
	// Publish a blob
	err = ex0.PublishBinary("key1", []byte("Hello BLOB!"))
	assert.NoError(t, err)

	time.Sleep(1 * time.Second) // Wait for the consumer receive and print the messages
}
