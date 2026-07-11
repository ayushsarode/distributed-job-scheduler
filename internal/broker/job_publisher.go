package broker

import (
	"context"
)

type JobPublisher struct {
	producer *Producer
}

func NewJobPublisher(p *Producer) *JobPublisher {
	return &JobPublisher{producer: p}
}

func (j *JobPublisher) PublishJob(ctx context.Context, msg JobMessage) error {
	return j.producer.Publish(ctx, TopicJobs, msg.WorkerID.String(), msg)
}
