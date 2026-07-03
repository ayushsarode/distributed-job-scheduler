package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"
	"time"

	apihttp "github.com/ayushsarode/distributed-job-scheduler/internal/api/http"
	"github.com/ayushsarode/distributed-job-scheduler/internal/config"
	"github.com/ayushsarode/distributed-job-scheduler/internal/db"
	"github.com/ayushsarode/distributed-job-scheduler/internal/logger"
	"github.com/ayushsarode/distributed-job-scheduler/internal/repository"
)

func main() {
	log := logger.New("scheduler-api")

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

	srv := apihttp.NewServer(cfg.HTTPPort, jobsRepo, log)

	go func() {
		log.Info().Int("port", cfg.HTTPPort).Msg("scheduler api listening")
		if err := srv.Start(); err != nil {
			log.Fatal().Err(err).Msg("server error")
		}

	}()

	<-ctx.Done()
	log.Info().Msg("shutting down...")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Fatal().Err(err).Msg("graceful shutdown failed")
	}

	log.Info().Msg("shutdown complete")

}
