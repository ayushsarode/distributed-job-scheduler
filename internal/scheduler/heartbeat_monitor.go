package scheduler

import (
	"context"
	"time"

	"github.com/ayushsarode/distributed-job-scheduler/internal/repository"
	"github.com/rs/zerolog"
)

const staleSeconds = 15

type HearbeatMonitor struct {
	Jobs     repository.JobsRepository
	Workers  repository.WorkersRepository
	Interval time.Duration
	Log      zerolog.Logger
}

func NewHeartbeatMonitor(jobs repository.JobsRepository, workers repository.WorkersRepository, log zerolog.Logger) *HearbeatMonitor {
	return &HearbeatMonitor{
		Jobs: jobs,
		Workers: workers,
		Interval: 10 * time.Second,
		Log: log,
	}
}

func (m *HearbeatMonitor) Run(ctx context.Context) {
	ticker := time.NewTicker(m.Interval)
	defer ticker.Stop()
 
	for {
		select {
		case <-ctx.Done():
			m.Log.Info().Msg("heartbeat monitor stopped")
			return
		case <-ticker.C:
			if err := m.tick(ctx); err != nil {
				m.Log.Error().Err(err).Msg("heartbeat sweep failed")
			}
		}
	}
}

func (m *HearbeatMonitor) tick(ctx context.Context) error {
	unhealthy, err := m.Workers.MarkUnhealthy(ctx, staleSeconds)
	if err != nil {
		return err
	}
 
	requeued, err := m.Jobs.RequeueStale(ctx, staleSeconds)
	if err != nil {
		return err
	}
 
	if unhealthy > 0 || requeued > 0 {
		m.Log.Warn().
			Int64("workers_marked_unhealthy", unhealthy).
			Int64("jobs_requeued", requeued).
			Msg("stale worker sweep")
	}
	return nil
}

