package metrics

import (
	"context"
	"fmt"
	"net/http"
	"sync"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/rs/zerolog"
)

var JobsSubmittedTotal = prometheus.NewCounter(prometheus.CounterOpts{
	Name: "jobs_submitted_total",
	Help: "Total number of jobs submitted.",
})

var JobsCompletedTotal = prometheus.NewCounter(prometheus.CounterOpts{
	Name: "jobs_completed_total",
	Help: "Total number of jobs completed.",
})

var JobsFailedTotal = prometheus.NewCounter(prometheus.CounterOpts{
	Name: "jobs_failed_total",
	Help: "Total number of failed job attempts.",
})

var JobsRetriedTotal = prometheus.NewCounter(prometheus.CounterOpts{
	Name: "jobs_retried_total",
	Help: "Total number of jobs moved back to retrying state.",
})

var JobsDeadTotal = prometheus.NewCounter(prometheus.CounterOpts{
	Name: "jobs_dead_total",
	Help: "Total number of jobs marked dead.",
})

var JobsReplayedTotal = prometheus.NewCounter(prometheus.CounterOpts{
	Name: "jobs_replayed_total",
	Help: "Total number of dead-letter jobs replayed.",
})

var DeadLettersPersistedTotal = prometheus.NewCounter(prometheus.CounterOpts{
	Name: "dead_letters_persisted_total",
	Help: "Total number of dead letters persisted.",
})

var WorkerRunningJobs = prometheus.NewGaugeVec(prometheus.GaugeOpts{
	Name: "worker_running_jobs",
	Help: "Current number of running jobs per worker.",
}, []string{"worker_id"})

var WorkerJobDurationSeconds = prometheus.NewHistogramVec(prometheus.HistogramOpts{
	Name:    "worker_job_duration_seconds",
	Help:    "Job execution duration in seconds.",
	Buckets: prometheus.DefBuckets,
}, []string{"job_type", "success"})

var registerOnce sync.Once

func Register() {
	registerOnce.Do(func() {
		prometheus.MustRegister(
			JobsSubmittedTotal,
			JobsCompletedTotal,
			JobsFailedTotal,
			JobsRetriedTotal,
			JobsDeadTotal,
			JobsReplayedTotal,
			DeadLettersPersistedTotal,
			WorkerRunningJobs,
			WorkerJobDurationSeconds,
		)
	})
}

func StartServer(ctx context.Context, port int, log zerolog.Logger) {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())

	srv := &http.Server{
		Addr:    fmt.Sprintf(":%d", port),
		Handler: mux,
	}

	go func() {
		<-ctx.Done()
		if err := srv.Shutdown(context.Background()); err != nil {
			log.Error().Err(err).Msg("metrics server shutdown failed")
		}
	}()

	go func() {
		log.Info().Int("port", port).Msg("metrics server listening")
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Error().Err(err).Msg("metrics server failed")
		}
	}()
}
