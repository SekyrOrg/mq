package main

import (
	"bytes"
	"fmt"
	"github.com/SekyrOrg/mq"
	amqp "github.com/rabbitmq/amqp091-go"
	"math/rand"
	"strconv"
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
	ch.Queue("beacon.event.new",
		mq.WithConsumer("ReplyToConsumer"),
		mq.WithAutoAck(false),
	).ConsumeFunc(func(ch *mq.Channel, msg *amqp.Delivery) {
		fmt.Println("recieved Event", msg)
		p := mq.NewPublishing(
			[]byte("World!"),
			mq.WithContentType(mq.ContentText),
			mq.WithCorrelationId(getCorrelationId()),
		)
		fmt.Println("Sending response", p)
		if err := ch.Exchange().Send(msg.ReplyTo, p); err != nil {
			fmt.Printf("error sending, %s", err)
		}
		fmt.Println("Acknowledging the delivery")
		if err = ch.RawChannel().Ack(msg.DeliveryTag, false); err != nil {
			fmt.Printf("err ack, err: %s ", err)
		}
	})
	if err != nil {
		fmt.Printf("Error: %s", err)
		return
	}

	fmt.Println("waiting forever")
	forever := make(chan bool)
	<-forever

}
func getCorrelationId() string {
	return strconv.Itoa(rand.Intn(9999999999))
}
