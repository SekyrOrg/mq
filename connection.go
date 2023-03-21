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

func NewConnectionFromConfig(config ConnectionConfig) (*Connection, error) {
	return NewConnection("amqp://" + config.Username + ":" + config.Password + "@" + config.Host + ":" + config.Port + "/")
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

func (c *Connection) CreateSchema(schema Schema) error {
	return schema.Create(c)
}
func (c *Connection) CreateSchemaFromYaml(new io.Reader) error {
	schema, err := NewSchemaFromYaml(new)
	if err != nil {
		return err
	}
	return schema.Create(c)
}

func (c *Connection) NotifyClose(close chan *amqp.Error) chan *amqp.Error {
	return c.c.NotifyClose(close)
}

func (c *Connection) Channel() (*Channel, error) {
	ch, err := NewChannel(c.c)
	if err != nil {
		return nil, err
	}
	return ch, nil
}

type ConnectionConfig struct {
	Host     string
	Port     string
	Username string
	Password string
}
