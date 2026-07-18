package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"
	"time"

	httpServer "github.com/ayushsarode/distributed-job-scheduler/internal/api/http"
	"github.com/ayushsarode/distributed-job-scheduler/internal/cache"
	"github.com/ayushsarode/distributed-job-scheduler/internal/config"
	"github.com/ayushsarode/distributed-job-scheduler/internal/db"
	"github.com/ayushsarode/distributed-job-scheduler/internal/logger"
	"github.com/ayushsarode/distributed-job-scheduler/internal/metrics"
	"github.com/ayushsarode/distributed-job-scheduler/internal/repository"
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

	metrics.Register()

	jobsRepo := repository.NewJobsRepo(database)
	deadLettersRepo := repository.NewDeadLettersRepo(database)
	_ = repository.NewWorkerRepo(database)

	idem := cache.NewIdempotencyStore(cfg.RedisAddr)
	defer idem.Close()

	limiter := cache.NewRateLimiter(cfg.RedisAddr, 100, 1*time.Minute) // 100 req/min per IP
	defer limiter.Close()

	statusCache := cache.NewStatusCache(cfg.RedisAddr)
	defer statusCache.Close()

	http := httpServer.NewServer(cfg.HTTPPort, jobsRepo, deadLettersRepo, idem, limiter, statusCache, cfg.APIKey, log)
	go func() {
		log.Info().Int("port", cfg.HTTPPort).Msg("HTTP server listening")
		if err := http.Start(); err != nil {
			log.Fatal().Err(err).Msg("http serve failed")
		}
	}()

	<-ctx.Done()
	log.Info().Msg("shutting down...")
	http.Shutdown(context.Background())
	log.Info().Msg("shutdown complete")
}
