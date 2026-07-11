package main

import (
	"context"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	grpcServer "github.com/ayushsarode/distributed-job-scheduler/internal/api/grpc"
	"github.com/ayushsarode/distributed-job-scheduler/internal/config"
	"github.com/ayushsarode/distributed-job-scheduler/internal/db"
	"github.com/ayushsarode/distributed-job-scheduler/internal/logger"
	"github.com/ayushsarode/distributed-job-scheduler/internal/repository"
	"github.com/ayushsarode/distributed-job-scheduler/internal/scheduler"
	pb "github.com/ayushsarode/distributed-job-scheduler/proto"
	"google.golang.org/grpc"
	httpServer "github.com/ayushsarode/distributed-job-scheduler/internal/api/http"

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

	schedulerGRPC := grpcServer.NewSchedulerServer(jobsRepo, workersRepo, log)

	lis, err := net.Listen("tcp", ":9090")
	if err != nil {
		log.Fatal().Err(err).Msg("failed to listen")
	}
	srv := grpc.NewServer()
	pb.RegisterSchedulerServiceServer(srv, schedulerGRPC)
	go func() {
		log.Info().Msg("gRPC server listening on :9090")
		if err := srv.Serve(lis); err != nil {
			log.Fatal().Err(err).Msg("gRPC serve failed")
		}
	}()

	http := httpServer.NewServer(8080, jobsRepo, log)
	go func() {
		log.Info().Msg("HTTP server listening on :8080")
		if err := http.Start(); err != nil {
			log.Fatal().Err(err).Msg("http serve failed")
		}
	}()

	elector := scheduler.NewLeaderElector(database.Pool, log)
	dispatcher := scheduler.NewDispatcher(jobsRepo, workersRepo, log)
	dispatcher.Pusher = schedulerGRPC
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
			http.Shutdown(context.Background())
			srv.GracefulStop()
			if err := elector.Resign(context.Background()); err != nil {
				log.Error().Err(err).Msg("resign failed")
			}
			log.Info().Msg("shutdown complete")
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
				go dispatcher.Run(loopCtx)
				go monitor.Run(loopCtx)
			}
		}
	}
}
