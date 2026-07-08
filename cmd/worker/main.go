package main

import (
	"context"
	"encoding/json"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/ayushsarode/distributed-job-scheduler/internal/config"
	"github.com/ayushsarode/distributed-job-scheduler/internal/db"
	"github.com/ayushsarode/distributed-job-scheduler/internal/logger"
	"github.com/ayushsarode/distributed-job-scheduler/internal/models"
	"github.com/ayushsarode/distributed-job-scheduler/internal/repository"
	"github.com/ayushsarode/distributed-job-scheduler/internal/worker"
	"github.com/rs/zerolog"
)

func main() {
	log := logger.New("worker")

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	cfg, err := config.Load()
	if err != nil {
		log.Fatal().Err(err).Msg("config load failed")
	}

	database, err := db.New(ctx, db.Config{DSN: cfg.PostgresDSN})
	if err != nil {
		log.Fatal().Err(err).Msg("db connect failed")
	}
	defer database.Close()

	jobsRepo := repository.NewJobsRepo(database)
	workersRepo := repository.NewWorkerRepo(database)

	// --- register job type runners ---
	runners := map[string]worker.JobRunner{
		"echo":  &EchoRunner{log: log},
		"sleep": &SleepRunner{log: log},
	}

	// --- build worker components ---
	jobChan := make(chan *models.Job, 10)

	// executor first so we can pass RunningJobs to heartbeat
	executor := worker.NewExecutor(jobsRepo, jobChan, runners, log)

	heartbeat := worker.NewHeartbeatSender(workersRepo, executor.RunningJobs, log)
	if err := heartbeat.Register(ctx); err != nil {
		log.Fatal().Err(err).Msg("worker registration failed")
	}

	consumer := worker.NewConsumer(jobsRepo, heartbeat.WorkerID, jobChan, log)

	// --- launch ---
	go heartbeat.Run(ctx)
	go consumer.Run(ctx)
	go executor.Run(ctx)

	log.Info().Str("worker_id", heartbeat.WorkerID.String()).Msg("worker started")
	<-ctx.Done()

	log.Info().Msg("worker shutdown complete")
}

// --- demo runners (move to internal/worker/runners/ later) ---

type EchoRunner struct {
	log zerolog.Logger
}

func (r *EchoRunner) Run(ctx context.Context, payload json.RawMessage) error {
	r.log.Info().RawJSON("payload", payload).Msg("echo job executed")
	return nil
}

type SleepRunner struct {
	log zerolog.Logger
}

func (r *SleepRunner) Run(ctx context.Context, payload json.RawMessage) error {
	var p struct {
		Duration string `json:"duration"`
	}
	if err := json.Unmarshal(payload, &p); err != nil {
		return err
	}

	d, err := time.ParseDuration(p.Duration)
	if err != nil {
		d = 2 * time.Second
	}

	r.log.Info().Str("duration", d.String()).Msg("sleep job started")

	select {
	case <-time.After(d):
		r.log.Info().Msg("sleep job finished")
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
