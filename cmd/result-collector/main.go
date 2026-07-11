package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"github.com/ayushsarode/distributed-job-scheduler/internal/broker"
	"github.com/ayushsarode/distributed-job-scheduler/internal/collector"
	"github.com/ayushsarode/distributed-job-scheduler/internal/config"
	"github.com/ayushsarode/distributed-job-scheduler/internal/db"
	"github.com/ayushsarode/distributed-job-scheduler/internal/logger"
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

	jobsRepo := repository.NewJobsRepo(database)
	producer := broker.NewProducer(cfg.KafkaBrokers)
	defer producer.Close()

	rc := &collector.ResultCollector{Jobs: jobsRepo, Producer: producer, Log: log}
	consumer := broker.NewConsumer(cfg.KafkaBrokers, broker.TopicResults, "result-collector", rc.HandleResult)

	log.Info().Msg("result collector started")
	if err := consumer.Run(ctx); err != nil {
		log.Fatal().Err(err).Msg("consumer failed")
	}
}
