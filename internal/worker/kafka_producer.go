package worker

import (
	"context"
	"time"

	"github.com/ayushsarode/distributed-job-scheduler/internal/broker"
	"github.com/ayushsarode/distributed-job-scheduler/internal/models"
	"github.com/google/uuid"
)

type KafkaReporter struct {
	producer *broker.Producer
	workerID uuid.UUID
	hostname string
}

func NewKafkaReporter(producer *broker.Producer, workerID uuid.UUID, hostname string) *KafkaReporter {
	return &KafkaReporter{producer: producer, workerID: workerID, hostname: hostname}
}

func (kr *KafkaReporter) ReportResult(ctx context.Context, job *models.Job, success bool, errMsg string) (string, error) {
	msg := broker.ResultMessage{
		JobID:    job.ID,
		WorkerID: kr.workerID,
		Success:  success,
		Error:    errMsg,
		JobType:  job.Type,
		Payload:  job.Payload,
		Attempts: job.Attempts,
	}
	if err := kr.producer.Publish(ctx, broker.TopicResults, job.ID.String(), msg); err != nil {
		return "", err
	}
	if success {
		return "COMPLETED", nil
	}
	return "REPORTED", nil // actual status is decided by the result collector
}

func (kr *KafkaReporter) SendHeartbeat(ctx context.Context, cpu, memory float64, runningJobs int) error {
	msg := broker.HeartbeatMessage{
		WorkerID:    kr.workerID,
		Hostname:    kr.hostname,
		CPU:         cpu,
		Memory:      memory,
		RunningJobs: runningJobs,
		Timestamp:   time.Now(),
	}
	return kr.producer.Publish(ctx, broker.TopicHeartbeats, kr.workerID.String(), msg)
}
