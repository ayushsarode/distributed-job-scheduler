package worker

import (
	"context"
	"time"

	"github.com/ayushsarode/distributed-job-scheduler/internal/models"
	"github.com/ayushsarode/distributed-job-scheduler/internal/repository"
	"github.com/google/uuid"
	"github.com/rs/zerolog"
)

type Consumer struct {
	jobs     repository.JobsRepository
	workerID uuid.UUID
	jobChan  chan<- *models.Job
	interval time.Duration
	log      zerolog.Logger
}

func NewConsumer(jobs repository.JobsRepository, workerID uuid.UUID, jobChan chan<- *models.Job, log zerolog.Logger) *Consumer {
	return &Consumer{
		jobs:     jobs,
		workerID: workerID,
		jobChan:  jobChan,
		interval: 2 * time.Second,
		log:      log,
	}
}

func (c *Consumer) Run(ctx context.Context) {
	ticker := time.NewTicker(c.interval)
	defer ticker.Stop()

	seen := make(map[uuid.UUID]struct{})

	for {
		select {
		case <-ctx.Done():
			c.log.Info().Msg("consumer stopped")
			return
		case <-ticker.C:
			jobs, err := c.jobs.FetchAssigned(ctx, c.workerID)
			if err != nil {
				c.log.Error().Err(err).Msg("fetch assigned failed")
				continue
			}
			for _, j := range jobs{
				if _, ok := seen[j.ID]; ok {
					continue 
				}
				seen[j.ID] = struct{}{}

				select {
				case c.jobChan <- j:
					c.log.Debug().Str("job_id", j.ID.String()).Msg("job sent to executor")
				case <- ctx.Done():
					return
				}
			}
		}
	}
}
