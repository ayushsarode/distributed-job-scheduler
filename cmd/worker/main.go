package main

import (
	"context"
	"encoding/json"
	"os"
	"os/signal"
	"runtime"
	"syscall"
	"time"

	"github.com/ayushsarode/distributed-job-scheduler/internal/logger"
	"github.com/ayushsarode/distributed-job-scheduler/internal/models"
	"github.com/ayushsarode/distributed-job-scheduler/internal/worker"
	"github.com/rs/zerolog"
)

func main() {
	log := logger.New("worker")

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	grpcClient, err := worker.NewGRPCClient("localhost:9090", log)
	if err != nil {
		log.Fatal().Err(err).Msg("grpc connect failed")
	}
	defer grpcClient.Close()

	host, _ := os.Hostname()
	if err := grpcClient.Register(ctx, host); err != nil {
		log.Fatal().Err(err).Msg("registration failed")
	}

	runners := map[string]worker.JobRunner{
		"echo":  &EchoRunner{log: log},
		"sleep": &SleepRunner{log: log},
	}


	jobChan := make(chan *models.Job, 10)
	executor := worker.NewExecutor(grpcClient, jobChan, runners, log)

	go func() {
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				var m runtime.MemStats
				runtime.ReadMemStats(&m)
				grpcClient.SendHeartbeat(ctx, 0.0, float64(m.Alloc)/1024/1024, executor.RunningJobs())
			}
		}
	}()

	go grpcClient.SubscribeJobs(ctx, jobChan)

	go executor.Run(ctx)

	log.Info().Str("worker_id", grpcClient.WorkerID.String()).Msg("worker started (gRPC mode)")
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
