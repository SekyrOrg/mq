package mq

import (
	amqp "github.com/rabbitmq/amqp091-go"
	"io"
)

type ReplyFunc func(msg *Message)

// Connection is a struct which holds all necessary data for RabbitMQ channel
type Connection struct {
	c *amqp.Connection
}

// NewConnection connects to RabbitMQ by dsn and return Connection object which uses channel during function calls issued later in code
func NewConnection(dsn string) (*Connection, error) {
	conn, err := amqp.Dial(dsn)
	if err != nil {
		return nil, err
	}
	return &Connection{conn}, nil
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

func (c *Connection) Channel() (*Channel, error) {
	ch, err := NewChannel(c.c)
	if err != nil {
		return nil, err
	}
	return ch, nil

}
