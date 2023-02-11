package mq

import amqp "github.com/rabbitmq/amqp091-go"

type ReplyFunc func(ch *amqp.Channel, msg *amqp.Delivery)
type QueueOption func(p *Queue)

type Queue struct {
	channel   *amqp.Channel
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

func NewQueue(name string, channel *amqp.Channel, options ...QueueOption) *Queue {
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

func (q *Queue) consumeBlocking(ch *amqp.Channel, msgs <-chan amqp.Delivery, f ReplyFunc) error {
	for {
		select {
		case d := <-msgs:
			f(ch, &d)
		case <-q.closeChan:
			ch.Close()
			return nil
		}
	}
}

func (q *Queue) Consume(f ReplyFunc) error {
	ch, err := q.channel.Channel()
	if err != nil {
		q.errChan <- err
	}
	return q.ConsumeOnChannel(ch, f)

}

func (q *Queue) ConsumeOnChannel(ch *amqp.Channel, f ReplyFunc) error {
	// start consuming
	msgs, err := ch.Consume(q.name, q.consumer, q.autoAck, q.exclusive, q.noLocal, q.noWait, q.args)
	if err != nil {
		return err
	}
	go func(msgs <-chan amqp.Delivery, f ReplyFunc, ch *amqp.Channel, errChan chan error) {
		for msg := range msgs {
			f(ch, &msg)
		}
	}(msgs, f, ch, q.errChan)
	return nil
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
