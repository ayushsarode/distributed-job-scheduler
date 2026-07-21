package handler

import (
	"errors"
	"net/http"
	"strconv"
	"time"

	"github.com/ayushsarode/distributed-job-scheduler/internal/api/http/dto"
	"github.com/ayushsarode/distributed-job-scheduler/internal/models"
	"github.com/ayushsarode/distributed-job-scheduler/internal/repository"
	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
)

type WorkerHandler struct {
	Workers repository.WorkersRepository
}

func NewWorkerHandler(workers repository.WorkersRepository) *WorkerHandler {
	return &WorkerHandler{Workers: workers}
}

// ListWorkers handles GET /workers?status=ACTIVE&limit=50&offset=0
func (h *WorkerHandler) ListWorkers(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()

	params := repository.WorkerListParams{
		Limit:  50,
		Offset: 0,
	}

	if v := q.Get("limit"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			params.Limit = n
		}
	}
	if v := q.Get("offset"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n >= 0 {
			params.Offset = n
		}
	}
	if v := q.Get("status"); v != "" {
		s := models.WorkerStatus(v)
		params.Status = &s
	}

	workers, err := h.Workers.List(r.Context(), params)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to list workers")
		return
	}

	items := make([]dto.WorkerResponse, 0, len(workers))
	for _, wr := range workers {
		items = append(items, toWorkerResponse(wr))
	}

	writeJSON(w, http.StatusOK, dto.ListWorkersResponse{
		Workers: items,
		Limit:   params.Limit,
		Offset:  params.Offset,
	})
}

// GetWorker handles GET /workers/{id}
func (h *WorkerHandler) GetWorker(w http.ResponseWriter, r *http.Request) {
	id, err := uuid.Parse(chi.URLParam(r, "id"))
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid worker id")
		return
	}

	worker, err := h.Workers.Get(r.Context(), id)
	if errors.Is(err, repository.ErrNotFound) {
		writeError(w, http.StatusNotFound, "worker not found")
		return
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to get worker")
		return
	}

	writeJSON(w, http.StatusOK, toWorkerResponse(worker))
}

// toWorkerResponse maps a models.Worker to the API response DTO.
func toWorkerResponse(wr *models.Worker) dto.WorkerResponse {
	resp := dto.WorkerResponse{
		ID:          wr.ID.String(),
		Host:        wr.Host,
		Status:      string(wr.Status),
		CPU:         wr.CPU,
		Memory:      wr.Memory,
		RunningJobs: wr.RunningJobs,
		CreatedAt:   wr.CreatedAt,
	}
	if wr.LastHeartBeat != nil {
		ts := wr.LastHeartBeat.Format(time.RFC3339)
		resp.LastHeartbeat = &ts
	}
	return resp
}