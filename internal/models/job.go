package models

import (
	"encoding/json"
	"time"

	"github.com/google/uuid"
)

type JobStatus string

const (
	JobStatusQueued    JobStatus = "QUEUED"
	JobStatusRunning   JobStatus = "RUNNING"
	JobStatusCompleted JobStatus = "COMPLETED"
	JobStatusFailed    JobStatus = "FAILED"
	JobStatusRetrying  JobStatus = "RETRYING"
	JobStatusDead      JobStatus = "DEAD"
)

type Job struct {
	ID        uuid.UUID       `db:"id" json:"id"`
	Status    JobStatus       `db:"status" json:"status"`
	Type      string          `db:"type" json:"type"`
	Payload   json.RawMessage `db:"payload" json:"payload"`
	Priority  int16           `db:"priority" json:"priority"`
	Attempts  int             `db:"attempts" json:"attempts"`
	WorkerID  *uuid.UUID      `db:"worker_id" json:"worker_id,omitempty"`
	CreatedAt time.Time       `db:"created_at" json:"created_at"`
	UpdatedAt time.Time       `db:"updated_at" json:"updated_at"`
}

// NewJob provides a single, consistent way to create a valid Job with the correct default values (such as Status = QUEUED).

func NewJob(jobType string, payload json.RawMessage, priority int16) *Job {
	return &Job{
		Status:   JobStatusQueued,
		Type:     jobType,
		Payload:  payload,
		Priority: priority,
	}
}
