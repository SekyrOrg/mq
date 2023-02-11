package mq

import (
	"context"
	amqp "github.com/rabbitmq/amqp091-go"
)

type ExchangeOption func(p *Exchange)

type Exchange struct {
	connection *Connection
	name       string
	mandatory  bool
	immediate  bool
	channel    *amqp.Channel
}

func NewExchange(connection *Connection, options ...ExchangeOption) *Exchange {
	e := &Exchange{
		connection: connection,
	}
	for _, option := range options {
		option(e)
	}
	return e
}

func (e *Exchange) Send(key string, p *Publishing) error {
	var ch *amqp.Channel
	// Check if channel is set, if not create new one
	if e.channel != nil {
		ch = e.channel
	} else {
		var err error
		ch, err = e.connection.Channel()
		if err != nil {
			return err
		}
	}
	// Publish message
	if err := ch.PublishWithContext(context.Background(), e.name, key, e.mandatory, e.immediate, p.Publishing); err != nil {
		return err
	}
	// Close channel if it was created by this function
	if e.channel == nil {
		if err := ch.Close(); err != nil {
			return err
		}
	}
	return nil
}

// SendText publishes plain text message to an exchange with specific routing key
func (e *Exchange) SendText(key, msg string) error {
	return e.Send(key, NewPublishing([]byte(msg), WithContentType("text/plain"), WithDeliveryMode(Persistent)))
}

// SendBinary publishes byte blob message to an exchange with specific routing key
func (e *Exchange) SendBinary(key string, msg []byte) error {
	return e.Send(key, NewPublishing(msg, WithContentType("application/octet-stream"), WithDeliveryMode(Persistent)))
}

func (e *Exchange) SendWithDirectReply(key string, p *Publishing, f ReplyFunc) error {
	ch, err := e.connection.Channel()
	if err != nil {
		return err
	}
	//defer ch.Close()
	err = NewQueue("amq.rabbitmq.reply-to", e.connection, WithConsumer("ReplyToCustomer")).
		ConsumeOnChannel(ch, f)
	if err != nil {
		return err
	}
	e.channel = ch
	p.ReplyTo = "amq.rabbitmq.reply-to"
	p.CorrelationId = RandomCorrelationId()
	return e.Send(key, p)
}

func WithMandatorySet() ExchangeOption {
	return func(e *Exchange) {
		e.mandatory = true
	}
}

func WithImmediateSet() ExchangeOption {
	return func(e *Exchange) {
		e.immediate = true
	}
}

func WithName(name string) ExchangeOption {
	return func(e *Exchange) {
		e.name = name
	}
}

func WithExternalChannel(channel *amqp.Channel) ExchangeOption {
	return func(e *Exchange) {
		e.channel = channel
	}
}
