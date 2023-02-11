package mq

import (
	amqp "github.com/rabbitmq/amqp091-go"
	"io"
)

// Connection is a struct which holds all necessary data for RabbitMQ channel
type Connection struct {
	c *amqp.Connection
}

// NewConnection connects to RabbitMQ by dsn and return Connection object which uses openned channel during function calls issued later in code
func NewConnection(dsn string) (*Connection, error) {
	conn, err := amqp.Dial(dsn)
	if err != nil {
		return nil, err
	}
	return &Connection{conn}, nil
}

func (c *Connection) Channel() (*amqp.Channel, error) {
	return c.c.Channel()

}

// Close closes channel to RabbitMQ
func (c *Connection) Close() error {
	return c.c.Close()
}

func (c *Connection) Schema(new io.Reader) error {
	settings, err := NewSchemaFromYaml(new)
	if err != nil {
		return err
	}
	return settings.Create(c)
}

func (c *Connection) Exchange(options ...ExchangeOption) *Exchange {
	return NewExchange(c, options...)
}

func (c *Connection) Queue(name string, options ...QueueOption) (*Queue, error) {
	ch, err := c.Channel()
	if err != nil {
		return nil, err
	}
	return NewQueue(name, ch, options...)
}
