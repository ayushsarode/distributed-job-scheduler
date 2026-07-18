package http

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/rs/zerolog"

	"github.com/ayushsarode/distributed-job-scheduler/internal/cache"
	"github.com/ayushsarode/distributed-job-scheduler/internal/repository"
)

// Server wraps http.Server so that main.go can start/shutdown without knowing internals.
type Server struct {
	httpServer *http.Server
}

func NewServer(
	port int,
	jobs repository.JobsRepository,
	deadLetters repository.DeadLettersRepository,
	idem *cache.IdempotencyStore,
	limiter *cache.RateLimiter,
	statusCache *cache.StatusCache,
	apiKey string,
	log zerolog.Logger,
) *Server {
	return &Server{
		httpServer: &http.Server{
			Addr:         fmt.Sprintf(":%d", port),
			Handler:      NewRouter(jobs, deadLetters, idem, limiter, statusCache, apiKey, log),
			ReadTimeout:  10 * time.Second,
			WriteTimeout: 15 * time.Second,
			IdleTimeout:  60 * time.Second,
		},
	}
}

func (s *Server) Start() error {
	if err := s.httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		return fmt.Errorf("http server: %w", err)
	}
	return nil
}

func (s *Server) Shutdown(ctx context.Context) error {
	return s.httpServer.Shutdown(ctx)
}
