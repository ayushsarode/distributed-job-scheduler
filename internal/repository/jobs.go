package repository

import (
	"context"
	"errors"
	"fmt"

	"github.com/ayushsarode/distributed-job-scheduler/internal/db"
	"github.com/ayushsarode/distributed-job-scheduler/internal/models"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

type JobsRepository interface {
	Create(ctx context.Context, j *models.Job) (*models.Job, error)
	Cancel(ctx context.Context, id uuid.UUID) error
	Get(ctx context.Context, id uuid.UUID) (*models.Job, error)
	List(ctx context.Context, p ListParams) ([]*models.Job, error)
	FetchPending(ctx context.Context, workerID uuid.UUID, limit int) ([]*models.Job, error)
	RequeueStale(ctx context.Context, staleSeconds int) (int64, error)
	MarkCompleted(ctx context.Context, id uuid.UUID) error
	MarkFailed(ctx context.Context, id uuid.UUID, maxAttempts int) (models.JobStatus, error)
	FetchAssigned(ctx context.Context, workerID uuid.UUID) ([]*models.Job, error)
}

// ListParams supports filtering/pagination

type ListParams struct {
	Status *models.JobStatus
	Type   *string
	Limit  int
	Offset int
}

type pgJobsRepo struct {
	db *db.DB
}

func NewJobsRepo(d *db.DB) JobsRepository {
	return &pgJobsRepo{db: d}
}

// Create job
func (r *pgJobsRepo) Create(ctx context.Context, j *models.Job) (*models.Job, error) {
	const q = `
	INSERT INTO jobs (status, type, payload, priority)
	VALUES ($1,$2,$3,$4)
	RETURNING id, status, type, payload, priority, attempts, worker_id, created_at, updated_at`

	row := r.db.Pool.QueryRow(ctx, q, models.JobStatusQueued, j.Type, j.Payload, j.Priority)
	return scanJob(row)
}

// Cancel job
func (r *pgJobsRepo) Cancel(ctx context.Context, id uuid.UUID) error {
	const q = `
			UPDATE jobs
		SET status = 'DEAD'
		WHERE id = $1 AND status IN ('QUEUED', 'RUNNING', 'RETRYING')`

	tag, err := r.db.Pool.Exec(ctx, q, id)

	if err != nil {
		return fmt.Errorf("cancel job %w", err)
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}

// Get job
func (r *pgJobsRepo) Get(ctx context.Context, id uuid.UUID) (*models.Job, error) {
	const q = `
			SELECT id, status, type, payload, priority, attempts, worker_id, created_at, updated_at
		FROM jobs WHERE id = $1`

	row := r.db.Pool.QueryRow(ctx, q, id)

	job, err := scanJob(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	return job, err
}

// List jobs
func (r *pgJobsRepo) List(ctx context.Context, p ListParams) ([]*models.Job, error) {
	if p.Limit <= 0 {
		p.Limit = 50
	}
 
	q := `
		SELECT id, status, type, payload, priority, attempts, worker_id, created_at, updated_at
		FROM jobs WHERE 1=1`
	args := []any{}
	argN := 0
 
	addArg := func(v any) string {
		argN++
		args = append(args, v)
		return fmt.Sprintf("$%d", argN)
	}
 
	if p.Status != nil {
		q += " AND status = " + addArg(*p.Status)
	}
	if p.Type != nil {
		q += " AND type = " + addArg(*p.Type)
	}
	q += " ORDER BY created_at DESC LIMIT " + addArg(p.Limit) + " OFFSET " + addArg(p.Offset)
 
	rows, err := r.db.Pool.Query(ctx, q, args...)
	if err != nil {
		return nil, fmt.Errorf("list jobs: %w", err)
	}
	defer rows.Close()
 
	var jobs []*models.Job
	for rows.Next() {
		j, err := scanJob(rows)
		if err != nil {
			return nil, err
		}
		jobs = append(jobs, j)
	}
	return jobs, rows.Err()
}

// fetch pending jobs 
// FOR UPDATE SKIP LOCKED: 
// FOR UPDATE basically this means, "I am about to change these rows, no one else is allowed to touch them until I am done."
// SKIP LOCKED: If Worker A is already locking row #1, and Worker B comes along looking for jobs, Worker B will see that row #1 is locked. Instead of waiting (which causes massive slow downs and deadlocks), Worker B will simply skip row #1 and grab row #2 instead.
func (r *pgJobsRepo) FetchPending(ctx context.Context, workerID uuid.UUID, limit int) ([]*models.Job, error) {
	const q = `
		WITH picked AS (
			SELECT id FROM jobs
			WHERE status IN ('QUEUED', 'RETRYING')
			ORDER BY priority DESC, created_at ASC
			LIMIT $1
			FOR UPDATE SKIP LOCKED 
		)
		UPDATE jobs
		SET status = 'RUNNING', worker_id = $2, attempts = jobs.attempts + 1
		FROM picked
		WHERE jobs.id = picked.id
		RETURNING jobs.id, jobs.status, jobs.type, jobs.payload, jobs.priority,
		          jobs.attempts, jobs.worker_id, jobs.created_at, jobs.updated_at`
 
	rows, err := r.db.Pool.Query(ctx, q, limit, workerID)
	if err != nil {
		return nil, fmt.Errorf("fetch pending jobs: %w", err)
	}
	defer rows.Close()
 
	var jobs []*models.Job
	for rows.Next() {
		j, err := scanJob(rows)
		if err != nil {
			return nil, err
		}
		jobs = append(jobs, j)
	}
	return jobs, rows.Err()
}

// this func is basically safety net. without this func job stuck on a dead worker would remain in  the running state forever and no other worker would pick up.
// We only want to rescue jobs from workers that are actually broken. We don't want to steal jobs from healthy workers that are just taking a long time.
// A worker is considered "dead" (and its jobs are stolen) if:
// Condition A: Its status is explicitly marked as UNHEALTHY.
// Condition B: It hasn't sent a heartbeat recently. The database takes the current time (now()), subtracts the allowed staleSeconds ($1), and checks if the worker's last_heartbeat happened before that cutoff. If a worker hasn't "pinged" the database in, say, 60 seconds, it's assumed to be dead.
func (r *pgJobsRepo) RequeueStale(ctx context.Context, staleSeconds int) (int64, error) {
	q := `
		UPDATE jobs
		SET status = 'QUEUED', worker_id = NULL
		WHERE status = 'RUNNING'
		  AND worker_id IN (
		      SELECT id FROM workers
		      WHERE last_heartbeat < now() - ($1 || ' seconds')::interval
		         OR status = 'UNHEALTHY'
		  )`
 
	tag, err := r.db.Pool.Exec(ctx, q, staleSeconds)
	if err != nil {
		return 0, fmt.Errorf("requeue stale jobs: %w", err)
	}
	return tag.RowsAffected(), nil
}

func (r *pgJobsRepo) MarkCompleted(ctx context.Context, id uuid.UUID) error {
	return r.setStatus(ctx, id, models.JobStatusCompleted)
}
 
func (r *pgJobsRepo) MarkFailed(ctx context.Context, id uuid.UUID, maxAttempts int) (models.JobStatus, error) {
	const q = `
		UPDATE jobs
		SET status = CASE WHEN attempts >= $2 THEN 'DEAD' ELSE 'RETRYING' END
		WHERE id = $1
		RETURNING status`
 
	var status models.JobStatus
	err := r.db.Pool.QueryRow(ctx, q, id, maxAttempts).Scan(&status)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", ErrNotFound
	}
	return status, err
}
 
func (r *pgJobsRepo) setStatus(ctx context.Context, id uuid.UUID, status models.JobStatus) error {
	tag, err := r.db.Pool.Exec(ctx, `UPDATE jobs SET status = $2 WHERE id = $1`, id, status)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}

func (r *pgJobsRepo) FetchAssigned(ctx context.Context, workerID uuid.UUID) ([]*models.Job, error) {
		const q = `
		SELECT id, status, type, payload, priority, attempts, worker_id, created_at, updated_at
		FROM jobs
		WHERE worker_id = $1 AND status = 'RUNNING'`

		rows, err := r.db.Pool.Query(ctx, q, workerID)

		if err != nil {
			return nil, fmt.Errorf("fetch assigned jobs: %w", err)
		}
		defer rows.Close()

		var jobs []*models.Job
		for rows.Next() {
			j, err := scanJob(rows)
			if err != nil {
				return nil, err
			}

			jobs = append(jobs, j)
		}
		return jobs, rows.Err()
}