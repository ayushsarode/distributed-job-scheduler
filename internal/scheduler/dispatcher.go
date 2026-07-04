package scheduler

import (
	"context"
	"time"

	"github.com/ayushsarode/distributed-job-scheduler/internal/repository"
	"github.com/rs/zerolog"
)

const maxJobsPerWorker = 5

type Dispatcher struct {
	Jobs     repository.JobsRepository
	Workers  repository.WorkersRepository
	Interval time.Duration
	Log      zerolog.Logger
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

			// TODO: once the Worker Pool + gRPC exist, push these jobs to the worker here instead of relying on it to notice the RUNNING+worker_id assignment on its own.

		}
	}
	if totalAssigned > 0 {
		d.Log.Info().Int("total", totalAssigned).Int("workers", len(workers)).Msg("dispatch tick complete")
	}

	return nil

}
