package mq

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"fmt"
	amqp "github.com/rabbitmq/amqp091-go"
	"io"
	"math/rand"
	"strconv"
	"time"
)

type DeliveryMethod uint8

const (
	Transient  DeliveryMethod = 1
	Persistent DeliveryMethod = 2

	ContentText = "text/plain"
	ContentJSON = "application/json"
	ContentBlob = "application/octet-stream"
)

type PublishingOption func(p *Publishing)
type Publishing struct {
	amqp.Publishing
}

func NewPublishing(body []byte, options ...PublishingOption) *Publishing {
	p := &Publishing{Publishing: amqp.Publishing{
		ContentType:  "application/json", // set JSON as default
		DeliveryMode: uint8(Persistent),
		Body:         body,
	}}
	for _, option := range options {
		option(p)
	}
	return p
}

func NewPublishingFromReader(bodyReader io.Reader, options ...PublishingOption) (*Publishing, error) {
	body, err := io.ReadAll(bodyReader)
	if err != nil {
		return nil, fmt.Errorf("unable to read from bodyReader, err: %w", err)
	}
	return NewPublishing(body, options...), nil
}

func NewPublishingJsonGzip(obj any, options ...PublishingOption) (*Publishing, error) {
	buf := &bytes.Buffer{}
	gz := gzip.NewWriter(buf)
	defer gz.Close()
	if err := json.NewEncoder(buf).Encode(obj); err != nil {
		return nil, fmt.Errorf("unable to encode json, err: %w", err)
	}
	opts := append(options, WithContentType(ContentJSON), WithContentEncoding("gzip"))
	return NewPublishing(buf.Bytes(), opts...), nil
}

func NewPublishingWithGzip(bodyReader io.Reader, options ...PublishingOption) (*Publishing, error) {
	r, err := gzip.NewReader(bodyReader)
	if err != nil {
		return nil, fmt.Errorf("unable to create gzip reader, err: %w", err)
	}
	defer r.Close()
	body, err := io.ReadAll(r)
	if err != nil {
		return nil, fmt.Errorf("unable to read from gzip reader, err: %w", err)
	}
	opts := append(options, WithContentEncoding("gzip"))
	return NewPublishing(body, opts...), nil
}

func WithPriority(priority uint8) PublishingOption {
	return func(p *Publishing) {
		p.Priority = priority
	}
}

func WithContentType(contentType string) PublishingOption {
	return func(p *Publishing) {
		p.ContentType = contentType
	}
}

func WithAppId(appId string) PublishingOption {
	return func(p *Publishing) {
		p.AppId = appId
	}
}

func WithUserId(userId string) PublishingOption {
	return func(p *Publishing) {
		p.UserId = userId
	}
}

func WithTimestamp(timestamp time.Time) PublishingOption {
	return func(p *Publishing) {
		p.Timestamp = timestamp
	}
}

func WithMessageId(messageId string) PublishingOption {
	return func(p *Publishing) {
		p.MessageId = messageId
	}
}

func WithExpiration(expiration string) PublishingOption {
	return func(p *Publishing) {
		p.Expiration = expiration
	}
}

func WithReplyTo(replyTo string) PublishingOption {
	return func(p *Publishing) {
		p.ReplyTo = replyTo
	}
}

func WithDeliveryMode(deliveryMode DeliveryMethod) PublishingOption {
	return func(p *Publishing) {
		p.DeliveryMode = uint8(deliveryMode)
	}
}

func WithCorrelationId(correlationId string) PublishingOption {
	return func(p *Publishing) {
		p.CorrelationId = correlationId
	}
}

func WithHeaders(headers amqp.Table) PublishingOption {
	return func(p *Publishing) {
		p.Headers = headers
	}
}

func WithContentEncoding(contentEncoding string) PublishingOption {
	return func(p *Publishing) {
		p.ContentEncoding = contentEncoding
	}
}

func WithType(msgType string) PublishingOption {
	return func(p *Publishing) {
		p.Type = msgType
	}
}
func RandomCorrelationId() string {
	return strconv.Itoa(rand.Intn(9999999999))
}
