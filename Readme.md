# mq - RabbitMQ client library for Go

## Usage example

```go
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
	conn, err := NewConnection("amqp://user:bitnami@localhost:5672/")
	assert.NoError(t, err)
	defer conn.Close()

	// Create a new scheme
	schema, err := NewSchemaFromYaml(bytes.NewReader(exampleSchema))
	assert.NoError(t, err)
	err = schema.Create(conn)
	assert.NoError(t, err)
	defer schema.Delete(conn)

	// Create a new channel
	channel, err := conn.Channel()
	assert.NoError(t, err)
	defer channel.Close()

	// Consume messages from queue0
	channel.Queue("queue0").ConsumeFunc(func(ch *Channel, msg *amqp.Delivery) {
		fmt.Println(string(msg.Body))
	})

	// Create a new publishing to send
	publishing := NewPublishing([]byte("Hello World!"), WithContentType(ContentText))

	// Create a new exchange
	ex0 := channel.Exchange(WithName("exchange0"))
	// Send a message
	err = ex0.Send("key1", publishing)
	assert.NoError(t, err)
	// Send a plain message
	err = ex0.SendText("key1", "Hello Plain!")
	assert.NoError(t, err)
	// Send a blob
	err = ex0.SendBinary("key1", []byte("Hello BLOB!"))
	assert.NoError(t, err)

	time.Sleep(1 * time.Second) // Wait for the consumer receive and print the messages
}
```
