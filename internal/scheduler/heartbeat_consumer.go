package scheduler

import (
	"context"

	"github.com/ayushsarode/distributed-job-scheduler/internal/broker"
	"github.com/ayushsarode/distributed-job-scheduler/internal/repository"
	"github.com/rs/zerolog"
)

type HeartbeatConsumer struct {
	Workers repository.WorkersRepository
	Log     zerolog.Logger

}

func (h *HeartbeatConsumer) Handle(ctx context.Context, key string, value []byte) error {
    var msg broker.HeartbeatMessage
    if err := broker.Decode(value, &msg); err != nil {
        return err
    }
    return h.Workers.Heartbeat(ctx, msg.WorkerID, msg.CPU, msg.Memory, msg.RunningJobs)
}