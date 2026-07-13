package main

import (
	"context"
	"encoding/json"
	"os"
	"os/signal"
	"runtime"
	"syscall"
	"time"

	"github.com/ayushsarode/distributed-job-scheduler/internal/broker"
	"github.com/ayushsarode/distributed-job-scheduler/internal/config"
	"github.com/ayushsarode/distributed-job-scheduler/internal/logger"
	"github.com/ayushsarode/distributed-job-scheduler/internal/models"
	"github.com/ayushsarode/distributed-job-scheduler/internal/worker"
	"github.com/google/uuid"
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

	workerID := uuid.New()
	hostname, _ := os.Hostname()

	runners := map[string]worker.JobRunner{
		"echo":  &EchoRunner{log: log},
		"sleep": &SleepRunner{log: log},
	}

	producer := broker.NewProducer(cfg.KafkaBrokers)
	defer producer.Close()

	reporter := worker.NewKafkaReporter(producer, workerID, hostname)
	jobChan := make(chan *models.Job, 10)
	executor := worker.NewExecutor(reporter, jobChan, runners, log)

	consumer := worker.NewKafkaJobConsumer(cfg.KafkaBrokers, workerID, jobChan, log)

	go func() {
		if err := consumer.Run(ctx); err != nil {
			log.Error().Err(err).Msg("job consumer failed")
			stop()
		}
	}()
	go executor.Run(ctx)

	go func() {
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()

		sendHeartbeat := func() {
			var m runtime.MemStats
			runtime.ReadMemStats(&m)
			memMB := float64(m.Alloc) / 1024 / 1024
			if err := reporter.SendHeartbeat(ctx, 0, memMB, executor.RunningJobs()); err != nil {
				log.Error().Err(err).Msg("failed to send heartbeat")
			}
		}

		sendHeartbeat()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				sendHeartbeat()
			}
		}
	}()

	log.Info().Str("worker_id", workerID.String()).Msg("worker started (event-driven mode)")
	<-ctx.Done()
	log.Info().Msg("worker shutdown complete")
}

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
