package mq

import (
	"fmt"
	amqp "github.com/rabbitmq/amqp091-go"
)

type QueueOption func(p *Queue)

type Queue struct {
	channel   *Channel
	name      string
	consumer  string
	autoAck   bool
	exclusive bool
	noLocal   bool
	noWait    bool
	args      amqp.Table
	closeChan chan bool
	errChan   chan error
}

func NewQueue(name string, channel *Channel, options ...QueueOption) *Queue {
	q := &Queue{
		channel:   channel,
		name:      name,
		autoAck:   true,
		closeChan: make(chan bool),
		errChan:   make(chan error),
	}
	for _, option := range options {
		option(q)
	}
	return q
}

func (q *Queue) Close() error {
	q.closeChan <- true
	return q.channel.Close()
}

func (q *Queue) consumeBlocking(msgs <-chan amqp.Delivery, f ReplyFunc) error {
	for {
		select {
		case d := <-msgs:
			f(NewMessage(q.channel, &d))
		case <-q.closeChan:
			return nil
		}
	}
}

func (q *Queue) Consume() (<-chan amqp.Delivery, error) {
	msgs, err := q.channel.channel.Consume(q.name, q.consumer, q.autoAck, q.exclusive, q.noLocal, q.noWait, q.args)
	if err != nil {
		return nil, fmt.Errorf("failed to consume from queue %s: %w", q.name, err)
	}
	return msgs, nil
}

func (q *Queue) ConsumeFunc(f ReplyFunc) {
	msgs, err := q.Consume()
	if err != nil {
		q.errChan <- fmt.Errorf("failed to consume from queue %s: %w", q.name, err)
	}
	go func() {
		if err := q.consumeBlocking(msgs, f); err != nil {
			q.errChan <- fmt.Errorf("failed to consume from queue %s: %w", q.name, err)
		}
	}()
}

func WithAutoAck(autoAck bool) QueueOption {
	return func(p *Queue) {
		p.autoAck = autoAck
	}
}

func WithExclusive(exclusive bool) QueueOption {
	return func(p *Queue) {
		p.exclusive = exclusive
	}
}

func WithNoLocal(noLocal bool) QueueOption {
	return func(p *Queue) {
		p.noLocal = noLocal
	}
}

func WithNoWait(noWait bool) QueueOption {
	return func(p *Queue) {
		p.noWait = noWait
	}
}

func WithArgs(args amqp.Table) QueueOption {
	return func(p *Queue) {
		p.args = args
	}
}

func WithConsumer(consumer string) QueueOption {
	return func(p *Queue) {
		p.consumer = consumer
	}
}

func WithErrorChannel(errChan chan error) QueueOption {
	return func(p *Queue) {
		p.errChan = errChan
	}
}
