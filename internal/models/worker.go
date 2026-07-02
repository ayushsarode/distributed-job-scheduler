package models

import (
	"time"

	"github.com/google/uuid"
)

type WorkerStatus string

const (
	WorkerStatusActive   WorkerStatus = "ACTIVE"
	WorkerStatusIdle     WorkerStatus = "IDLE"
	WorkerStatusUnhealty WorkerStatus = "UNHEALTHY"
	WorkerStatusOffline  WorkerStatus = "OFFLINE"
)

type Worker struct {
	ID            uuid.UUID    `db:"id" json:"id"`
	Host          string       `db:"host" json:"host"`
	Status        WorkerStatus `db:"status" json:"status"`
	CPU           *float64     `db:"cpu" json:"cpu"`
	Memory        *float64     `db:"memory" json:"memory"`
	RunningJobs   int          `db:"running_jobs" json:"running_jobs"`
	LastHeartBeat *time.Time   `db:"last_heartbeat" json:"last_heartbeat,omitempty"`
	CreatedAt     time.Time    `db:"created_at" json:"created_at"`
}
