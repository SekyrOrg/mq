package mq

import (
	"gopkg.in/yaml.v3"
	"io"
)

// Schema is a specification of all queues and exchanges together with all bindings.
type Schema struct {
	Exchanges map[string]ExchangeDefinition `yaml:"exchanges"`
	Queues    map[string]QueueSpec          `yaml:"queues"`
}

// ExchangeDefinition is structure with specification of properties of RabbitMQ exchange
type ExchangeDefinition struct {
	Durable    bool                `yaml:"durable"`
	Autodelete bool                `yaml:"autodelete"`
	Internal   bool                `yaml:"internal"`
	Nowait     bool                `yaml:"nowait"`
	Type       string              `yaml:"type"`
	Bindings   []BindingDefinition `yaml:"bindings"`
}

// BindingDefinition specifies to which exchange should be an exchange or a queue binded
type BindingDefinition struct {
	Exchange string `yaml:"exchange"`
	Key      string `yaml:"key"`
	Nowait   bool   `yaml:"nowait"`
}

// QueueSpec is a specification of properties of RabbitMQ queue
type QueueSpec struct {
	Durable    bool                `yaml:"durable"`
	Autodelete bool                `yaml:"autodelete"`
	Nowait     bool                `yaml:"nowait"`
	Exclusive  bool                `yaml:"exclusive"`
	Bindings   []BindingDefinition `yaml:"bindings"`
}

// NewSettingsFromYaml reads yaml with specification of all exchanges and queues from io.Reader
func NewSchemaFromYaml(r io.Reader) (*Schema, error) {
	var s Schema

	if err := yaml.NewDecoder(r).Decode(&s); err != nil {
		return nil, err
	}
	return &s, nil
}

// Create creates all exchanges, queues and bindinges between them as specified by Schema
func (s *Schema) Create(c *Connection) error {
	ch, err := c.Channel()
	if err != nil {
		return err
	}
	rawCh := ch.RawChannel()

	// Create exchanges according to settings
	for name, e := range s.Exchanges {
		if err = rawCh.ExchangeDeclarePassive(name, e.Type, e.Durable, e.Autodelete, e.Internal, e.Nowait, nil); err == nil {
			continue
		}
		rawCh, err = c.c.Channel()
		if err != nil {
			return err
		}

		err = rawCh.ExchangeDeclare(name, e.Type, e.Durable, e.Autodelete, e.Internal, e.Nowait, nil)
		if err != nil {
			return err
		}
	}

	// Create queues according to settings
	for name, q := range s.Queues {
		if _, err := rawCh.QueueDeclarePassive(name, q.Durable, q.Autodelete, q.Exclusive, q.Nowait, nil); err == nil {
			continue
		}

		rawCh, err = c.c.Channel()
		if err != nil {
			return err
		}

		if _, err = rawCh.QueueDeclare(name, q.Durable, q.Autodelete, q.Exclusive, q.Nowait, nil); err != nil {
			return err
		}
	}

	// Create bindings only now that everything is setup.
	// (If the bindings were created in one run together with exchanges and queues,
	// it would be possible to create binding to not yet existent queue.
	// This way it's still possible but now is an error on the user side)
	for name, e := range s.Exchanges {
		for _, b := range e.Bindings {
			if err = rawCh.ExchangeBind(name, b.Key, b.Exchange, b.Nowait, nil); err != nil {
				return err
			}
		}
	}

	for name, q := range s.Queues {
		for _, b := range q.Bindings {
			if err = rawCh.QueueBind(name, b.Key, b.Exchange, b.Nowait, nil); err != nil {
				return err
			}
		}
	}

	rawCh.Close()
	return nil
}

// Delete deletes all queues and exchanges (together with bindings) as specified by Schema
func (s *Schema) Delete(c *Connection) error {
	ch, err := c.c.Channel()
	if err != nil {
		return err
	}

	for name := range s.Exchanges {
		if err = ch.ExchangeDelete(name, false, false); err != nil {
			return err
		}
	}

	for name := range s.Queues {
		if _, err = ch.QueueDelete(name, false, false, false); err != nil {
			return err
		}
	}
	ch.Close()
	return nil
}
