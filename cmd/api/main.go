package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/ayushsarode/distributed-job-scheduler/internal/config"
	"github.com/ayushsarode/distributed-job-scheduler/internal/db"
	"github.com/ayushsarode/distributed-job-scheduler/internal/logger"
	"github.com/ayushsarode/distributed-job-scheduler/internal/repository"
	"github.com/ayushsarode/distributed-job-scheduler/internal/scheduler"
)

func main() {
	log := logger.New("scheduler-service")

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

	elector := scheduler.NewLeaderElector(database.Pool, log)
	dispatcher := scheduler.NewDispatcher(jobsRepo, workersRepo, log)
	monitor := scheduler.NewHeartbeatMonitor(jobsRepo, workersRepo, log)

	campaignTicker := time.NewTicker(3 * time.Second)
	defer campaignTicker.Stop()

	var running bool
	loopCtx, cancelLoops := context.WithCancel(ctx)
	defer cancelLoops()

	for {
		select {
		case <-ctx.Done():
			log.Info().Msg("shutting down...")
			cancelLoops()
			if err := elector.Resign(context.Background()); err != nil {
				log.Error().Err(err).Msg("resign failed")
			}
			log.Info().Msg("shutdown complete")
			return

		case <-campaignTicker.C:
			if running {
				continue // already leader and looping, nothing to do
			}
			isLeader, err := elector.Campaign(ctx)
			if err != nil {
				log.Error().Err(err).Msg("campaign failed")
				continue
			}
			if isLeader {
				running = true
				go dispatcher.Run(loopCtx)
				go monitor.Run(loopCtx)
			}
		}
	}
}