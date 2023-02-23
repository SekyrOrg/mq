package mq

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
)

type ExchangeOption func(p *Exchange)

type Exchange struct {
	channel   *Channel
	name      string
	mandatory bool
	immediate bool
	context   context.Context
}

func NewExchange(ch *Channel, options ...ExchangeOption) *Exchange {
	e := &Exchange{
		channel: ch,
		context: context.Background(),
	}
	for _, option := range options {
		option(e)
	}
	return e
}

func (e *Exchange) Publish(key string, p *Publishing) error {
	if e.channel.channel.IsClosed() {
		return fmt.Errorf("channel is closed")
	}
	// Publish message
	if err := e.channel.RawChannel().PublishWithContext(e.context, e.name, key, e.mandatory, e.immediate, p.Publishing); err != nil {
		return fmt.Errorf("failed to publish message to exchange: '%s', key: '%s', err: %w", e.name, key, err)
	}
	return nil
}

// PublishText publishes plain text message to an exchange with specific routing key
func (e *Exchange) PublishText(key, msg string) error {
	return e.Publish(key, NewPublishing([]byte(msg), WithContentType("text/plain"), WithDeliveryMode(Persistent)))
}

// PublishBinary publishes byte blob message to an exchange with specific routing key
func (e *Exchange) PublishBinary(key string, msg []byte) error {
	return e.Publish(key, NewPublishing(msg, WithContentType("application/octet-stream"), WithDeliveryMode(Persistent)))
}

func (e *Exchange) PublishJson(key string, obj any) error {
	buf := &bytes.Buffer{}
	if err := json.NewEncoder(buf).Encode(obj); err != nil {
		return err
	}
	return e.Publish(key, NewPublishing(buf.Bytes(), WithContentType("application/json"), WithDeliveryMode(Persistent)))

}

func (e *Exchange) PublishWithDirectReply(key string, p *Publishing) (*Message, error) {
	defer e.channel.Close()
	msgs, err := NewQueue("amq.rabbitmq.reply-to", e.channel, WithConsumer("ReplyToCustomer")).Consume()
	if err != nil {
		return nil, err
	}
	p.ReplyTo = "amq.rabbitmq.reply-to"
	p.CorrelationId = RandomCorrelationId()
	if err := e.Publish(key, p); err != nil {
		return nil, err
	}
	select {
	case msg := <-msgs:
		return NewMessage(e.channel, &msg), nil
	case <-e.context.Done():
		return nil, e.context.Err()
	}
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

func WithContext(ctx context.Context) ExchangeOption {
	return func(e *Exchange) {
		e.context = ctx
	}
}
