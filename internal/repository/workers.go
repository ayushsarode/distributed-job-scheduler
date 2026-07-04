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

type WorkersRepository interface {
	Register(ctx context.Context, host string)(*models.Worker, error)
	Heartbeat(ctx context.Context, id uuid.UUID, cpu, memory float64, runningJobs int) error
	FetchHealthy(ctx context.Context, staleSecond int) ([]*models.Worker, error)
	MarkUnhealthy(ctx context.Context, staleSecond int)(int64, error)
	Get(ctx context.Context, id uuid.UUID) (*models.Worker, error)
}

type pgWorkersRepo struct {
	db *db.DB
}

func NewWorkerRepo(d *db.DB) WorkersRepository {
	return &pgWorkersRepo{db: d}
}

func (r *pgWorkersRepo) Register(ctx context.Context, host string) (*models.Worker, error) {
	const q = `
		INSERT INTO workers (host, status, running_jobs)
		VALUES ($1, 'IDLE', 0)
		RETURNING id, host, status, cpu, memory, running_jobs, last_heartbeat, created_at`
	
		row := r.db.Pool.QueryRow(ctx, q, host)

		return scanWorker(row)
}

// Heartbeat (worker pool -> scheduler service, every 5s)
// updates liveness + resource stats
func (r *pgWorkersRepo) Heartbeat(ctx context.Context, id uuid.UUID, cpu, memory float64, runningJobs int) error {
	const q = `
		UPDATE workers
		SET last_heartbeat = now(),
		    cpu = $2,
		    memory = $3,
		    running_jobs = $4,
		    status = CASE WHEN status = 'OFFLINE' OR status = 'UNHEALTHY' THEN 'IDLE' ELSE status END
		WHERE id = $1`
 
	tag, err := r.db.Pool.Exec(ctx, q, id, cpu, memory, runningJobs)
	if err != nil {
		return fmt.Errorf("heartbeat: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}

// fetchhealthy returns workers whose heartbeat is recent, ordered so the least-loaded worker comes first
func (r pgWorkersRepo) FetchHealthy(ctx context.Context, staleSecond int) ([]*models.Worker, error) {
	q := `
		SELECT id, host, status, cpu, memory, running_jobs, last_heartbeat, created_at
		FROM workers
		WHERE status NOT IN ('OFFLINE', 'UNHEALTHY')
		  AND last_heartbeat > now() - ($1 || ' seconds')::interval
		ORDER BY running_jobs ASC, cpu ASC NULLS LAST`

	rows, err := r.db.Pool.Query(ctx, q, staleSecond)
	if err != nil {
		return nil, fmt.Errorf("fetch healthy workers: %w", err)
	}

	defer rows.Close()

	var workers []*models.Worker
	for rows.Next() {
		w, err := scanWorker(rows)
		if err != nil {
			return nil, err
		}
		workers = append(workers, w)
	}
	return workers, rows.Err()
}

func (r *pgWorkersRepo) MarkUnhealthy(ctx context.Context, staleSeconds int) (int64, error) {
	q := `
		UPDATE workers
		SET status = 'UNHEALTHY'
		WHERE status NOT IN ('UNHEALTHY', 'OFFLINE')
		  AND (last_heartbeat IS NULL OR last_heartbeat < now() - ($1 || ' seconds')::interval)`
 
	tag, err := r.db.Pool.Exec(ctx, q, staleSeconds)
	if err != nil {
		return 0, fmt.Errorf("mark unhealthy: %w", err)
	}
	return tag.RowsAffected(), nil
}
 
func (r *pgWorkersRepo) Get(ctx context.Context, id uuid.UUID) (*models.Worker, error) {
	const q = `
		SELECT id, host, status, cpu, memory, running_jobs, last_heartbeat, created_at
		FROM workers WHERE id = $1`
 
	row := r.db.Pool.QueryRow(ctx, q, id)
	w, err := scanWorker(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	return w, err
}