package mq

import amqp "github.com/rabbitmq/amqp091-go"

type Channel struct {
	channel   *amqp.Channel
	closeChan chan bool
}

func NewChannel(connection *amqp.Connection) (*Channel, error) {
	channel, err := connection.Channel()
	if err != nil {
		return nil, err
	}
	return &Channel{
		channel:   channel,
		closeChan: make(chan bool),
	}, nil
}

func (c *Channel) Queue(name string, options ...QueueOption) *Queue {
	q := &Queue{
		name:      name,
		channel:   c,
		closeChan: c.closeChan,
	}
	for _, option := range options {
		option(q)
	}
	return q
}

func (c *Channel) Exchange(options ...ExchangeOption) *Exchange {
	return NewExchange(c, options...)
}

func (c *Channel) Close() error {
	c.closeChan <- true
	return c.channel.Close()
}

func (c *Channel) RawChannel() *amqp.Channel {
	return c.channel
}
