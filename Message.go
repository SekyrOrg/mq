package mq

import (
	"bytes"
	amqp "github.com/rabbitmq/amqp091-go"
)

type Message struct {
	*amqp.Delivery
	ch  *Channel
	buf *bytes.Buffer
}

func NewMessage(ch *Channel, delivery *amqp.Delivery) *Message {
	return &Message{
		Delivery: delivery,
		ch:       ch,
		buf:      bytes.NewBuffer(delivery.Body),
	}
}

func (m *Message) Ack() error {
	return m.ch.RawChannel().Ack(m.Delivery.DeliveryTag, false)
}

func (m *Message) Read(b []byte) (int, error) {
	return m.buf.Read(b)
}

func (m *Message) Bytes() []byte {
	return m.Body
}

func (m *Message) Reply(p *Publishing) error {
	return m.ch.Exchange().Send(m.ReplyTo, p)
}

func (m *Message) String() string {
	return string(m.Body)
}
