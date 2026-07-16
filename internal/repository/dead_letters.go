package repository

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/ayushsarode/distributed-job-scheduler/internal/db"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// DeadLetter represents a single record in the dead_letters table.
type DeadLetter struct {
	ID        uuid.UUID       `db:"id"         json:"id"`
	JobID     uuid.UUID       `db:"job_id"     json:"job_id"`
	WorkerID  *uuid.UUID      `db:"worker_id"  json:"worker_id,omitempty"`
	JobType   string          `db:"job_type"   json:"job_type"`
	Payload   json.RawMessage `db:"payload"    json:"payload"`
	Attempts  int             `db:"attempts"   json:"attempts"`
	Error     string          `db:"error"      json:"error"`
	CreatedAt time.Time       `db:"created_at" json:"created_at"`
}

// DeadLettersRepository is the interface for dead_letters persistence.
type DeadLettersRepository interface {
	Insert(ctx context.Context, dl *DeadLetter) error
	List(ctx context.Context, limit, offset int) ([]*DeadLetter, error)
	Get(ctx context.Context, id uuid.UUID) (*DeadLetter, error)
	Delete(ctx context.Context, id uuid.UUID) error
}

type pgDeadLettersRepo struct {
	db *db.DB
}

func NewDeadLettersRepo(d *db.DB) DeadLettersRepository {
	return &pgDeadLettersRepo{db: d}
}

// Insert persists a new dead letter record.
func (r *pgDeadLettersRepo) Insert(ctx context.Context, dl *DeadLetter) error {
	const q = `
		INSERT INTO dead_letters (job_id, worker_id, job_type, payload, attempts, error)
		VALUES ($1, $2, $3, $4, $5, $6)`

	_, err := r.db.Pool.Exec(ctx, q,
		dl.JobID,
		dl.WorkerID,
		dl.JobType,
		dl.Payload,
		dl.Attempts,
		dl.Error,
	)
	if err != nil {
		return fmt.Errorf("insert dead letter: %w", err)
	}
	return nil
}

// List returns dead letters ordered by most recently created, with pagination.
func (r *pgDeadLettersRepo) List(ctx context.Context, limit, offset int) ([]*DeadLetter, error) {
	if limit <= 0 {
		limit = 50
	}
	const q = `
		SELECT id, job_id, worker_id, job_type, payload, attempts, error, created_at
		FROM dead_letters
		ORDER BY created_at DESC
		LIMIT $1 OFFSET $2`

	rows, err := r.db.Pool.Query(ctx, q, limit, offset)
	if err != nil {
		return nil, fmt.Errorf("list dead letters: %w", err)
	}
	defer rows.Close()

	var results []*DeadLetter
	for rows.Next() {
		dl, err := scanDeadLetter(rows)
		if err != nil {
			return nil, err
		}
		results = append(results, dl)
	}
	return results, rows.Err()
}

// Get fetches a single dead letter by its own ID.
func (r *pgDeadLettersRepo) Get(ctx context.Context, id uuid.UUID) (*DeadLetter, error) {
	const q = `
		SELECT id, job_id, worker_id, job_type, payload, attempts, error, created_at
		FROM dead_letters WHERE id = $1`

	row := r.db.Pool.QueryRow(ctx, q, id)
	dl, err := scanDeadLetter(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	return dl, err
}

// Delete removes a dead letter record (purge after manual inspection/retry).
func (r *pgDeadLettersRepo) Delete(ctx context.Context, id uuid.UUID) error {
	tag, err := r.db.Pool.Exec(ctx, `DELETE FROM dead_letters WHERE id = $1`, id)
	if err != nil {
		return fmt.Errorf("delete dead letter: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}

// scanDeadLetter scans a pgx Row/Rows into a DeadLetter.
type dlScanner interface {
	Scan(dest ...any) error
}

func scanDeadLetter(row dlScanner) (*DeadLetter, error) {
	dl := &DeadLetter{}
	err := row.Scan(
		&dl.ID,
		&dl.JobID,
		&dl.WorkerID,
		&dl.JobType,
		&dl.Payload,
		&dl.Attempts,
		&dl.Error,
		&dl.CreatedAt,
	)
	if err != nil {
		return nil, err
	}
	return dl, nil
}
