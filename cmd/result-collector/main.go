package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"github.com/ayushsarode/distributed-job-scheduler/internal/broker"
	"github.com/ayushsarode/distributed-job-scheduler/internal/cache"
	"github.com/ayushsarode/distributed-job-scheduler/internal/collector"
	"github.com/ayushsarode/distributed-job-scheduler/internal/config"
	"github.com/ayushsarode/distributed-job-scheduler/internal/db"
	"github.com/ayushsarode/distributed-job-scheduler/internal/logger"
	"github.com/ayushsarode/distributed-job-scheduler/internal/metrics"
	"github.com/ayushsarode/distributed-job-scheduler/internal/repository"
)

func main() {
	log := logger.New("result-collector")
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

	metrics.Register()
	metrics.StartServer(ctx, 9102, log)

	jobsRepo := repository.NewJobsRepo(database)
	producer := broker.NewProducer(cfg.KafkaBrokers)
	defer producer.Close()

	statusCache := cache.NewStatusCache(cfg.RedisAddr)
	defer statusCache.Close()

	rc := &collector.ResultCollector{Jobs: jobsRepo, Producer: producer, Log: log, StatusCache: statusCache}
	resultConsumer := broker.NewConsumer(cfg.KafkaBrokers, broker.TopicResults, "result-collector", rc.HandleResult)
	defer resultConsumer.Close()

	deadLettersRepo := repository.NewDeadLettersRepo(database)
	dlc := &collector.DeadLetterConsumer{Repo: deadLettersRepo, Log: log}
	deadLetterConsumer := broker.NewConsumer(cfg.KafkaBrokers, broker.TopicDeadLetter, "dead-letter-collector", dlc.Handle)
	defer deadLetterConsumer.Close()

	log.Info().Msg("result collector started")
	errCh := make(chan error, 2)
	go func() {
		errCh <- resultConsumer.Run(ctx)
	}()
	go func() {
		errCh <- deadLetterConsumer.Run(ctx)
	}()

	if err := <-errCh; err != nil {
		log.Fatal().Err(err).Msg("consumer failed")
	}
}
