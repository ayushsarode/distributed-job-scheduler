package repository

import (
	"encoding/json"
	"errors"

	"github.com/ayushsarode/distributed-job-scheduler/internal/models"
)

var ErrNotFound = errors.New("not found")

type rowScanner interface {
	Scan(dest ...any) error
}

func scanJob(row rowScanner) (*models.Job, error) {
	var j models.Job
	var payload []byte

	err := row.Scan(&j.ID, &j.Status, &j.Type, &payload, &j.Priority, &j.Attempts, &j.WorkerID, &j.CreatedAt, &j.UpdatedAt)

	if err != nil {
		return nil, err
	}

	j.Payload = json.RawMessage(payload)
	return &j, nil
}

func scanWorker(row rowScanner) (*models.Worker, error) {
	var w models.Worker
	err := row.Scan(&w.ID, &w.Host, &w.Status, &w.CPU, &w.Memory, &w.RunningJobs, &w.LastHeartBeat, &w.CreatedAt)

	if err != nil {
		return nil, err
	}

	return &w, nil
}
