package broker

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/segmentio/kafka-go"
)

type Handler func(ctx context.Context, key string, value []byte) error

type Consumer struct {
	reader  *kafka.Reader
	handler Handler
}

func NewConsumer(brokers []string, topic string, groupID string, handler Handler) *Consumer {
	return &Consumer{
		reader: kafka.NewReader(kafka.ReaderConfig{
			Brokers: brokers,
			Topic:   topic,
			GroupID: groupID,
		}),
		handler: handler,
	}
}

func (c *Consumer) Run(ctx context.Context) error {
	for {
		msg, err := c.reader.FetchMessage(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return nil
			}
			return err
		}
		if err := c.handler(ctx, string(msg.Key), msg.Value); err != nil {
			return err
		}
		if err := c.reader.CommitMessages(ctx, msg); err != nil {
			return err
		}
	}
}

func (c *Consumer) Close() error {
	return c.reader.Close()
}

func Decode[T any](value []byte, out *T) error {
	return json.Unmarshal(value, out)
}
