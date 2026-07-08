package worker

import (
	"context"
	"os"
	"runtime"
	"time"

	"github.com/ayushsarode/distributed-job-scheduler/internal/repository"
	"github.com/google/uuid"
	"github.com/rs/zerolog"
)

type HeartbeatSender struct {
	workers   repository.WorkersRepository
	WorkerID  uuid.UUID
	host      string
	interval  time.Duration
	runningFn func() int
	log       zerolog.Logger
}

func NewHeartbeatSender(workers repository.WorkersRepository, runningFn func() int, log zerolog.Logger) *HeartbeatSender {
	host, _ := os.Hostname()
	return &HeartbeatSender{
		workers:   workers,
		host:      host,
		interval:  5 * time.Second,
		runningFn: runningFn,
		log:       log,
	}
}

func (h *HeartbeatSender) Register(ctx context.Context) error {
	w, err := h.workers.Register(ctx, h.host)
	if err != nil {
		return err
	}

	h.WorkerID = w.ID
	h.log.Info().Str("worker_id", w.ID.String()).Msg("worker registered")
	return nil
}

func (h *HeartbeatSender) Run (ctx context.Context) {
	ticker := time.NewTicker(h.interval)
	defer ticker.Stop()

	for {
		select{
		case <-ctx.Done():
			h.log.Info().Msg("heartbeat sender stopped")
			return
		case <-ticker.C:
			var memStats runtime.MemStats
			runtime.ReadMemStats(&memStats)

			cpu := 0.0 //TODO: not implemented yet
			memory := float64(memStats.Alloc) / 1024 / 1024
			running := h.runningFn()

			if err := h.workers.Heartbeat(ctx, h.WorkerID, cpu, memory, running); err != nil {
				h.log.Error().Err(err).Msg("heartbeat failed")
			}

		}
	}
}