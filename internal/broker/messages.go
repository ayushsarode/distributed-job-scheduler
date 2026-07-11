package broker

import (
	"encoding/json"
	"time"

	"github.com/google/uuid"
)

type JobMessage struct {
	JobID    uuid.UUID       `json:"job_id"`
	WorkerID uuid.UUID       `json:"worker_id"`
	Type     string          `json:"type"`
	Payload  json.RawMessage `json:"payload"`
	Priority int16           `json:"priority"`
	Attempts int             `json:"attempts"`
}

type ResultMessage struct {
	JobID    uuid.UUID `json:"job_id"`
	WorkerID uuid.UUID `json:"worker_id"`
	Success  bool      `json:"success"`
	Error    string    `json:"error,omitempty"`
}

type HeartbeatMessage struct {
	WorkerID    uuid.UUID `json:"worker_id"`
	CPU         float64   `json:"cpu"`
	Memory      float64   `json:"memory"`
	RunningJobs int       `json:"running_jobs"`
	Timestamp   time.Time `json:"timestamp"`
}
