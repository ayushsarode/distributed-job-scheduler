package scheduler

import (
	"context"
	"time"

	"github.com/ayushsarode/distributed-job-scheduler/internal/broker"
	"github.com/ayushsarode/distributed-job-scheduler/internal/repository"
	"github.com/rs/zerolog"
)

const maxJobsPerWorker = 5

type JobPublisher interface {
	PublishJob(ctx context.Context, msg broker.JobMessage) error
}

type Dispatcher struct {
	Jobs      repository.JobsRepository
	Workers   repository.WorkersRepository
	Publisher JobPublisher
	Interval  time.Duration
	Log       zerolog.Logger
}

func NewDispatcher(jobs repository.JobsRepository, workers repository.WorkersRepository, log zerolog.Logger) *Dispatcher {
	return &Dispatcher{
		Jobs:     jobs,
		Workers:  workers,
		Interval: 2 * time.Second,
		Log:      log,
	}
}

func (d *Dispatcher) Run(ctx context.Context) {
	ticker := time.NewTicker(d.Interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			d.Log.Info().Msg("dispatcher stopped")
			return
		case <-ticker.C:
			if err := d.tick(ctx); err != nil {
				d.Log.Error().Err(err).Msg("dispatch tick failed")
			}
		}

	}
}

func (d *Dispatcher) tick(ctx context.Context) error {
	workers, err := d.Workers.FetchHealthy(ctx, 15)

	if err != nil {
		return err
	}

	if len(workers) == 0 {
		return nil
	}

	var totalAssigned int
	for _, w := range workers {
		capacity := maxJobsPerWorker - w.RunningJobs
		if capacity <= 0 {
			continue
		}

		jobs, err := d.Jobs.FetchPending(ctx, w.ID, capacity)

		if err != nil {
			d.Log.Error().Err(err).Str("worker_id", w.ID.String()).Msg("fetch pending failed")
			continue
		}

		if len(jobs) > 0 {
			totalAssigned += len(jobs)
			d.Log.Info().Str("worker_id", w.ID.String()).Int("count", len(jobs)).Msg("assigned jobs")

			for _, j := range jobs {
				msg := broker.JobMessage{
					JobID:    j.ID,
					WorkerID: w.ID,
					Type:     j.Type,
					Payload:  j.Payload,
					Priority: j.Priority,
					Attempts: j.Attempts,
				}
				if err := d.Publisher.PublishJob(ctx, msg); err != nil {
					d.Log.Error().Err(err).Str("job_id", j.ID.String()).Msg("kafka publish failed")
					if rbErr := d.Jobs.RollbackToQueued(ctx, j.ID); rbErr != nil {
						d.Log.Error().Err(rbErr).Str("job_id", j.ID.String()).Msg("rollback to queued failed")
					} else {
						d.Log.Warn().Str("job_id", j.ID.String()).Msg("rolled back job to QUEUED after publish failure")
					}
					continue
				}
			}

		}
	}
	if totalAssigned > 0 {
		d.Log.Info().Int("total", totalAssigned).Int("workers", len(workers)).Msg("dispatch tick complete")
	}

	return nil

}
