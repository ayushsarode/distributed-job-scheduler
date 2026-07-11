package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/ayushsarode/distributed-job-scheduler/internal/broker"
	"github.com/ayushsarode/distributed-job-scheduler/internal/config"
	"github.com/ayushsarode/distributed-job-scheduler/internal/db"
	"github.com/ayushsarode/distributed-job-scheduler/internal/logger"
	"github.com/ayushsarode/distributed-job-scheduler/internal/repository"
	"github.com/ayushsarode/distributed-job-scheduler/internal/scheduler"
)

func main() {
	log := logger.New("scheduler")

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

	producer := broker.NewProducer(cfg.KafkaBrokers)
	defer producer.Close()

	dispatcher := scheduler.NewDispatcher(jobsRepo, workersRepo, log)
	dispatcher.Publisher = broker.NewJobPublisher(producer)

	monitor := scheduler.NewHeartbeatMonitor(jobsRepo, workersRepo, log)
	elector := scheduler.NewLeaderElector(database.Pool, log)

	loopCtx, cancelLoops := context.WithCancel(ctx)
	defer cancelLoops()

	heartbeatHandler := &scheduler.HeartbeatConsumer{Workers: workersRepo, Log: log}
	heartbeatConsumer := broker.NewConsumer(
		cfg.KafkaBrokers,
		broker.TopicHeartbeats,
		"scheduler-heartbeats",
		heartbeatHandler.Handle,
	)
	defer heartbeatConsumer.Close()

	go func() {
		if err := heartbeatConsumer.Run(loopCtx); err != nil {
			log.Error().Err(err).Msg("heartbeat consumer failed")
			cancelLoops()
		}
	}()

	campaignTicker := time.NewTicker(3 * time.Second)
	defer campaignTicker.Stop()

	var running bool
	for {
		select {
		case <-ctx.Done():
			log.Info().Msg("scheduler shutting down")
			cancelLoops()
			if err := elector.Resign(context.Background()); err != nil {
				log.Error().Err(err).Msg("resign failed")
			}
			return
		case <-loopCtx.Done():
			log.Info().Msg("scheduler loop stopped")
			return
		case <-campaignTicker.C:
			if running {
				continue
			}
			isLeader, err := elector.Campaign(ctx)
			if err != nil {
				log.Error().Err(err).Msg("campaign failed")
				continue
			}
			if isLeader {
				running = true
				log.Info().Msg("scheduler leadership acquired")
				go dispatcher.Run(loopCtx)
				go monitor.Run(loopCtx)
			}
		}
	}
}
