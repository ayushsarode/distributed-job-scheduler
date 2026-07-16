package worker

import (
	"context"
	"encoding/json"
	"sync"
	"sync/atomic"

	"github.com/ayushsarode/distributed-job-scheduler/internal/models"
	"github.com/rs/zerolog"
)

const maxAttempts = 3

type JobRunner interface {
	Run(ctx context.Context, payload json.RawMessage) error
}

type ResultReporter interface {
	ReportResult(ctx context.Context, job *models.Job, success bool, errMsg string) (string, error)
}

type Executor struct {
	reporter     ResultReporter
	jobChan      <-chan *models.Job
	runners      map[string]JobRunner
	maxConcurr   int
	log          zerolog.Logger
	runningCount atomic.Int32
}

func NewExecutor(reporter ResultReporter, jobChan <-chan *models.Job, runners map[string]JobRunner, log zerolog.Logger) *Executor {
	return &Executor{
		reporter:   reporter,
		jobChan:    jobChan,
		runners:    runners,
		maxConcurr: 5,
		log:        log,
	}
}

func (e *Executor) RunningJobs() int {
	return int(e.runningCount.Load())
}

func (e *Executor) Run(ctx context.Context) {
	sem := make(chan struct{}, e.maxConcurr)
	var wg sync.WaitGroup

	for {
		select {
		case <-ctx.Done():
			e.log.Info().Msg("execute: waiting for in-flight jobs to finish...")
			wg.Wait()
			e.log.Info().Msg("executor stopped")
			return

		case job, ok := <-e.jobChan:
			if !ok {
				wg.Wait()
				return
			}

			sem <- struct{}{}
			wg.Add(1)
			e.runningCount.Add(1)

			go func(j *models.Job) {
				defer func() {
					<-sem
					wg.Done()
					e.runningCount.Add(-1)
				}()
				e.execute(ctx, j)
			}(job)
		}
	}
}

func (e *Executor) execute(ctx context.Context, job *models.Job) {
	log := e.log.With().Str("job_id", job.ID.String()).Str("type", job.Type).Logger()

	runner, ok := e.runners[job.Type]

	if !ok {
		log.Error().Msg("no runner registered for job type")
		e.fail(ctx, job, "no runner registered for job type", log)
		return
	}

	log.Info().Msg("executing job")

	if err := runner.Run(ctx, job.Payload); err != nil {
		log.Error().Err(err).Msg("job execution failed")
		e.fail(ctx, job, err.Error(), log)
		return
	}

	if _, err := e.reporter.ReportResult(ctx, job, true, ""); err != nil {
		log.Error().Err(err).Msg("mark completed failed")
		return
	}
	log.Info().Msg("job completed")
}

func (e *Executor) fail(ctx context.Context, job *models.Job, errMsg string, log zerolog.Logger) {
	status, err := e.reporter.ReportResult(ctx, job, false, errMsg)
	if err != nil {
		log.Error().Err(err).Msg("mark failed failed")
		return
	}
	log.Warn().Str("new_status", string(status)).Msg("job marked failed")
}
