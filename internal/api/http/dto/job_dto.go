package dto

import (
	"encoding/json"
	"time"
)

// body for POST /jobs
type SubmitJobRequest struct {
	Type           string          `json:"type"`
	Payload        json.RawMessage `json:"payload"`
	Priority       int16           `json:"priority"`
	IdempotencyKey string          `json:"idempotency_key"`
}

type JobResponse struct {
	ID        string          `json:"id"`
	Status    string          `json:"status"`
	Type      string          `json:"type"`
	Payload   json.RawMessage `json:"payload"`
	Priority  int16           `json:"priority"`
	Attempts  int             `json:"attempts"`
	WorkerID  *string         `json:"worker_id,omitempty"`
	CreatedAt time.Time       `json:"created_at"`
	UpdatedAt time.Time       `json:"updated_at"`
}

// body for GET /jobs.
type ListJobsResponse struct {
	Jobs   []JobResponse `json:"jobs"`
	Limit  int           `json:"limit"`
	Offset int           `json:"offset"`
}

// this is the standard error body every handler returns on failure.
type ErrorResponse struct {
	Error string `json:"error"`
}
