package http

import (
	"net/http"
	"time"

	"github.com/ayushsarode/distributed-job-scheduler/internal/api/http/handler"
	appmw "github.com/ayushsarode/distributed-job-scheduler/internal/api/http/middleware"
	"github.com/ayushsarode/distributed-job-scheduler/internal/cache"
	"github.com/ayushsarode/distributed-job-scheduler/internal/repository"
	"github.com/go-chi/chi/v5"
	chimw "github.com/go-chi/chi/v5/middleware"
	"github.com/rs/zerolog"
	promhttp "github.com/prometheus/client_golang/prometheus/promhttp"
)

func NewRouter(
	jobs repository.JobsRepository,
	deadLetters repository.DeadLettersRepository,
	idem *cache.IdempotencyStore,
	limiter *cache.RateLimiter,
	statusCache *cache.StatusCache,
	apiKey string,
	log zerolog.Logger,
) http.Handler {
	r := chi.NewRouter()

	r.Use(chimw.RequestID)
	r.Use(appmw.Logging(log))
	r.Use(chimw.Recoverer)
	r.Use(chimw.Timeout(30 * time.Second))
	r.Use(appmw.RateLimit(limiter))

	r.Get("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("ok"))
	})

	r.Handle("/metrics", promhttp.Handler())

	r.Group(func(r chi.Router) {
		r.Use(appmw.APIKeyAuth(apiKey))

		jobHandler := handler.NewJobHandler(jobs, idem, statusCache)
		r.Route("/jobs", func(r chi.Router) {
			r.Post("/", jobHandler.SubmitJob)
			r.Get("/", jobHandler.ListJobs)
			r.Get("/{id}", jobHandler.GetJobStatus)
			r.Delete("/{id}", jobHandler.CancelJob)
		})

		dlqHandler := handler.NewDeadLetterHandler(deadLetters, jobs)
		r.Route("/dead-letters", func(r chi.Router) {
			r.Get("/", dlqHandler.List)
			r.Get("/{id}", dlqHandler.Get)
			r.Post("/{id}/replay", dlqHandler.Replay)
			r.Delete("/{id}", dlqHandler.Delete)
		})
	})

	return r
}
