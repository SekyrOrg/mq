package mq

import (
	"context"
)

type ExchangeOption func(p *Exchange)

type Exchange struct {
	channel   *Channel
	name      string
	mandatory bool
	immediate bool
}

func NewExchange(ch *Channel, options ...ExchangeOption) *Exchange {
	e := &Exchange{
		channel: ch,
	}
	for _, option := range options {
		option(e)
	}
	return e
}

func (e *Exchange) Send(key string, p *Publishing) error {

	// Publish message
	if err := e.channel.RawChannel().PublishWithContext(context.Background(), e.name, key, e.mandatory, e.immediate, p.Publishing); err != nil {
		return err
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
	NewQueue("amq.rabbitmq.reply-to", e.channel, WithConsumer("ReplyToCustomer")).ConsumeFunc(f)
	p.ReplyTo = "amq.rabbitmq.reply-to"
	p.CorrelationId = RandomCorrelationId()
	return e.Send(key, p)
}

/*
func (e *Exchange) SendWithDirectReply(key string, p *Publishing, f ReplyFunc) error {

		//defer ch.Close()
		err = NewQueue("amq.rabbitmq.reply-to", e., WithConsumer("ReplyToCustomer")).
			ConsumeOnChannel(ch, f)
		if err != nil {
			return err
		}
		e.channel = ch
		p.ReplyTo = "amq.rabbitmq.reply-to"
		p.CorrelationId = RandomCorrelationId()
		return e.Send(key, p)
	}
*/
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
