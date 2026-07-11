package worker

import (
	"time"

	"github.com/ayushsarode/distributed-job-scheduler/internal/broker"
	"github.com/google/uuid"
	"context"
)

type KafkaReporter struct {
	producer *broker.Producer
	workerID uuid.UUID
}

func NewKafkaReporter(producer *broker.Producer, workerID uuid.UUID) *KafkaReporter {
    return &KafkaReporter{producer: producer, workerID: workerID}
}


func (kr *KafkaReporter) ReportResult(ctx context.Context, jobID uuid.UUID, success bool, errMsg string) (string, error) {
    msg := broker.ResultMessage{
        JobID:    jobID,
        WorkerID: kr.workerID,
        Success:  success,
        Error:    errMsg,
    }
    if err := kr.producer.Publish(ctx, broker.TopicResults, jobID.String(), msg); err != nil {
        return "", err
    }
    if success {
        return "COMPLETED", nil
    }
    return "RETRYING", nil // actual status decided by collector
}
func (kr *KafkaReporter) SendHeartbeat(ctx context.Context, cpu, memory float64, runningJobs int) error {
    msg := broker.HeartbeatMessage{
        WorkerID:    kr.workerID,
        CPU:         cpu,
        Memory:      memory,
        RunningJobs: runningJobs,
        Timestamp:   time.Now(),
    }
    return kr.producer.Publish(ctx, broker.TopicHeartbeats, kr.workerID.String(), msg)
}