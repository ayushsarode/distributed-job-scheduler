package dto

import "time"

// WorkerResponse is the API response for a single worker.
type WorkerResponse struct {
	ID            string    `json:"id"`
	Host          string    `json:"host"`
	Status        string    `json:"status"`
	CPU           *float64  `json:"cpu"`
	Memory        *float64  `json:"memory"`
	RunningJobs   int       `json:"running_jobs"`
	LastHeartbeat *string   `json:"last_heartbeat,omitempty"`
	CreatedAt     time.Time `json:"created_at"`
}

// ListWorkersResponse is the body for GET /workers.
type ListWorkersResponse struct {
	Workers []WorkerResponse `json:"workers"`
	Limit   int              `json:"limit"`
	Offset  int              `json:"offset"`
}
