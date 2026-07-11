package worker

import (
	"context"

	"github.com/google/uuid"
	"github.com/rs/zerolog"

	"github.com/ayushsarode/distributed-job-scheduler/internal/broker"
	"github.com/ayushsarode/distributed-job-scheduler/internal/models"
)

type KafkaJobConsumer struct {
	consumer *broker.Consumer
	jobChan  chan<- *models.Job
	workerID uuid.UUID
	log zerolog.Logger
}

func NewKafkaJobConsumer(brokers []string, workerID uuid.UUID, jobChan chan<- *models.Job, log zerolog.Logger) *KafkaJobConsumer{
	kc := &KafkaJobConsumer{jobChan: jobChan, workerID: workerID, log: log}
	kc.consumer = broker.NewConsumer(brokers, broker.TopicJobs, workerID.String(), kc.handle)
	return kc
}

func (kc *KafkaJobConsumer) handle(ctx context.Context, key string, value []byte) error {
    var msg broker.JobMessage
    if err := broker.Decode(value, &msg); err != nil {
        return err
    }
    // only consume jobs assigned to this worker
    if msg.WorkerID != kc.workerID {
        return nil
    }
    select {
    case kc.jobChan <- &models.Job{
        ID:       msg.JobID,
        Type:     msg.Type,
        Payload:  msg.Payload,
        Priority: msg.Priority,
        Attempts: msg.Attempts,
    }:
        return nil
    case <-ctx.Done():
        return ctx.Err()
    }
}
func (kc *KafkaJobConsumer) Run(ctx context.Context) error {
    return kc.consumer.Run(ctx)
}