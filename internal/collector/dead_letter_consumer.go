package collector

import (
	"context"

	"github.com/ayushsarode/distributed-job-scheduler/internal/broker"
	"github.com/ayushsarode/distributed-job-scheduler/internal/repository"
	"github.com/rs/zerolog"
)

type DeadLetterConsumer struct {
	Repo repository.DeadLettersRepository
	Log  zerolog.Logger
}

func (d *DeadLetterConsumer) Handle(ctx context.Context, key string, value []byte) error {
	var msg broker.ResultMessage
	if err := broker.Decode(value, &msg); err != nil {
		d.Log.Error().Err(err).Str("key", key).Msg("dead-letter: failed to decode message")
		return err
	}

	dl := &repository.DeadLetter{
		JobID:    msg.JobID,
		WorkerID: &msg.WorkerID,
		JobType:  msg.JobType,
		Payload:  msg.Payload,
		Attempts: msg.Attempts,
		Error:    msg.Error,
	}

	if err := d.Repo.Insert(ctx, dl); err != nil {
		d.Log.Error().Err(err).Str("job_id", msg.JobID.String()).Msg("dead-letter: failed to persist")
		return err
	}

	d.Log.Warn().
		Str("job_id", msg.JobID.String()).
		Str("worker_id", msg.WorkerID.String()).
		Str("error", msg.Error).
		Msg("dead letter persisted")

	return nil
}
