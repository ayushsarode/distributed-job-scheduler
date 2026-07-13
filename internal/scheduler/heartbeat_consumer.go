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
	if err := h.Workers.UpsertHeartbeat(ctx, msg.WorkerID, msg.Hostname, msg.CPU, msg.Memory, msg.RunningJobs); err != nil {
		return err
	}
	h.Log.Info().Str("worker_id", msg.WorkerID.String()).Str("host", msg.Hostname).Int("running_jobs", msg.RunningJobs).Msg("worker heartbeat consumed")
	return nil
}
