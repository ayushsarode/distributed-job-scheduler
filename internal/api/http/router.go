package http

import (
	"net/http"
	"time"

	"github.com/ayushsarode/distributed-job-scheduler/internal/api/http/handler"
	appmw "github.com/ayushsarode/distributed-job-scheduler/internal/api/http/middleware"
	"github.com/ayushsarode/distributed-job-scheduler/internal/repository"
	"github.com/go-chi/chi/v5"
	chimw "github.com/go-chi/chi/v5/middleware"
	"github.com/rs/zerolog"
)

func NewRouter(jobs repository.JobsRepository, log zerolog.Logger) http.Handler {
	r := chi.NewRouter()

	r.Use(chimw.RequestID)
	r.Use(appmw.Logging(log))
	r.Use(chimw.Recoverer)
	r.Use(chimw.Timeout(30 * time.Second))

	jobHandler := handler.NewJobHandler(jobs)

	r.Route("/jobs", func(r chi.Router) {
		r.Post("/", jobHandler.SubmitJob)
		r.Get("/", jobHandler.ListJobs)
		r.Get("/{id}", jobHandler.GetJobStatus)
		r.Delete("/{id}", jobHandler.CancelJob)
	})

	r.Get("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("ok"))
	})

	return r
}

