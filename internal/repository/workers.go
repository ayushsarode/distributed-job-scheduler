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

// WorkerListParams supports filtering/pagination for worker listing.
type WorkerListParams struct {
	Status *models.WorkerStatus
	Limit  int
	Offset int
}

type WorkersRepository interface {
	Register(ctx context.Context, host string) (*models.Worker, error)
	UpsertHeartbeat(ctx context.Context, id uuid.UUID, host string, cpu, memory float64, runningJobs int) error
	FetchHealthy(ctx context.Context, staleSecond int) ([]*models.Worker, error)
	MarkUnhealthy(ctx context.Context, staleSecond int) (int64, error)
	Get(ctx context.Context, id uuid.UUID) (*models.Worker, error)
	List(ctx context.Context, p WorkerListParams) ([]*models.Worker, error)
}

type pgWorkersRepo struct {
	db *db.DB
}

func NewWorkerRepo(d *db.DB) WorkersRepository {
	return &pgWorkersRepo{db: d}
}

func (r *pgWorkersRepo) Register(ctx context.Context, host string) (*models.Worker, error) {
	const q = `
		INSERT INTO workers (host, status, running_jobs, last_heartbeat)
		VALUES ($1, 'IDLE', 0, now())
		RETURNING id, host, status, cpu, memory, running_jobs, last_heartbeat, created_at`

	row := r.db.Pool.QueryRow(ctx, q, host)

	return scanWorker(row)
}

func (r *pgWorkersRepo) UpsertHeartbeat(ctx context.Context, id uuid.UUID, host string, cpu, memory float64, runningJobs int) error {
	const q = `
		INSERT INTO workers (id, host, status, cpu, memory, running_jobs, last_heartbeat)
		VALUES ($1, $2, 'IDLE', $3, $4, $5, now())
		ON CONFLICT (id) DO UPDATE SET
			last_heartbeat = now(),
			host = EXCLUDED.host,
			cpu = EXCLUDED.cpu,
			memory = EXCLUDED.memory,
			running_jobs = EXCLUDED.running_jobs,
			status = CASE WHEN workers.status = 'OFFLINE' OR workers.status = 'UNHEALTHY' THEN 'IDLE' ELSE workers.status END`

	_, err := r.db.Pool.Exec(ctx, q, id, host, cpu, memory, runningJobs)
	if err != nil {
		return fmt.Errorf("upsert heartbeat: %w", err)
	}
	return nil
}

// fetchhealthy returns workers whose heartbeat is recent, ordered so the least-loaded worker comes first
func (r pgWorkersRepo) FetchHealthy(ctx context.Context, staleSecond int) ([]*models.Worker, error) {
	q := `
		SELECT id, host, status, cpu, memory, running_jobs, last_heartbeat, created_at
		FROM workers
		WHERE status NOT IN ('OFFLINE', 'UNHEALTHY')
		  AND last_heartbeat > now() - make_interval(secs => $1)
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
		  AND (last_heartbeat IS NULL OR last_heartbeat < now() - make_interval(secs => $1))`

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

func (r *pgWorkersRepo) List(ctx context.Context, p WorkerListParams) ([]*models.Worker, error) {
	if p.Limit <= 0 {
		p.Limit = 50
	}

	q := `
		SELECT id, host, status, cpu, memory, running_jobs, last_heartbeat, created_at
		FROM workers WHERE 1=1`
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
	q += " ORDER BY last_heartbeat DESC NULLS LAST LIMIT " + addArg(p.Limit) + " OFFSET " + addArg(p.Offset)

	rows, err := r.db.Pool.Query(ctx, q, args...)
	if err != nil {
		return nil, fmt.Errorf("list workers: %w", err)
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
