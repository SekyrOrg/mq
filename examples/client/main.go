package main

import (
	"bytes"
	"fmt"
	"github.com/SekyrOrg/mq"
)

var dsn = "amqp://user:bitnami@localhost:5672/"

var data = []byte(`
exchanges:
  beacon.event:
    durable: true
    type: direct 
queues:
  beacon.event.new:
    durable: true
    bindings:
      - exchange: "beacon.event"
        key: "new"
  beacon.event.created:
    durable: true
    bindings:
      - exchange: "beacon.event"
        key: "created"
`)

func main() {

	// Create a new connection
	conn, err := mq.NewConnection("amqp://user:bitnami@localhost:5672/")
	if err != nil {
		panic(err)
	}
	defer conn.Close()

	if err := conn.Schema(bytes.NewReader(data)); err != nil {
		panic(err)
	}
	ch, err := conn.Channel()
	if err != nil {
		panic(err)
	}
	defer ch.Close()

	p := mq.NewPublishing([]byte("Hello World!"), mq.WithContentType(mq.ContentText))
	err = ch.Exchange(mq.WithName("beacon.event")).
		SendWithDirectReply("new", p, func(msg *mq.Message) {
			fmt.Printf("Received a directReply: %s\n", msg)
		})
	if err != nil {
		fmt.Printf("Error: %s", err)
		return
	}

	fmt.Println("waiting forever")
	forever := make(chan bool)
	<-forever

}
