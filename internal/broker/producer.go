package broker

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/segmentio/kafka-go"
)

type Producer struct {
	writers map[string]*kafka.Writer
}

func NewProducer(brokers []string) *Producer {
	mkWriter := func(topic string) *kafka.Writer {
		return &kafka.Writer{
			Addr:     kafka.TCP(brokers...),
			Topic:    topic,
			Balancer: &kafka.LeastBytes{},
		}
	}
	return &Producer{
		writers: map[string]*kafka.Writer{
			TopicJobs:       mkWriter(TopicJobs),
			TopicResults:    mkWriter(TopicResults),
			TopicHeartbeats: mkWriter(TopicHeartbeats),
			TopicDeadLetter: mkWriter(TopicDeadLetter),
		},
	}
}

func (p *Producer) Publish(ctx context.Context, topic string, key string, v any) error {
	writer, ok := p.writers[topic]
	if !ok {
		return fmt.Errorf("unknown topic %q", topic)
	}

	body, err := json.Marshal(v)
	if err != nil {
		return err
	}

	return writer.WriteMessages(ctx, kafka.Message{
		Key:   []byte(key),
		Value: body,
	})
}

func (p *Producer) Close() error {
	var firstErr error

	for _, writer := range p.writers {
		if err := writer.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}

	return firstErr
}
