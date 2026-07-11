package main

import (
	"context"
	"net"
	"os"
	"os/signal"
	"strconv"
	"syscall"

	grpcServer "github.com/ayushsarode/distributed-job-scheduler/internal/api/grpc"
	httpServer "github.com/ayushsarode/distributed-job-scheduler/internal/api/http"
	"github.com/ayushsarode/distributed-job-scheduler/internal/config"
	"github.com/ayushsarode/distributed-job-scheduler/internal/db"
	"github.com/ayushsarode/distributed-job-scheduler/internal/logger"
	"github.com/ayushsarode/distributed-job-scheduler/internal/repository"
	pb "github.com/ayushsarode/distributed-job-scheduler/proto"
	"google.golang.org/grpc"
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

	controlGRPC := grpcServer.NewControlServer(workersRepo, log)

	lis, err := net.Listen("tcp", ":"+strconv.Itoa(cfg.GRPCPort))
	if err != nil {
		log.Fatal().Err(err).Msg("failed to listen")
	}
	srv := grpc.NewServer()
	pb.RegisterWorkerControlServiceServer(srv, controlGRPC)
	go func() {
		log.Info().Int("port", cfg.GRPCPort).Msg("gRPC server listening")
		if err := srv.Serve(lis); err != nil {
			log.Fatal().Err(err).Msg("gRPC serve failed")
		}
	}()

	http := httpServer.NewServer(cfg.HTTPPort, jobsRepo, log)
	go func() {
		log.Info().Int("port", cfg.HTTPPort).Msg("HTTP server listening")
		if err := http.Start(); err != nil {
			log.Fatal().Err(err).Msg("http serve failed")
		}
	}()

	<-ctx.Done()
	log.Info().Msg("shutting down...")
	http.Shutdown(context.Background())
	srv.GracefulStop()
	log.Info().Msg("shutdown complete")
}
