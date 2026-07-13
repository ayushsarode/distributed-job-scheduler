package handler

import (
	"encoding/json"
	"errors"
	"net/http"
	"strconv"

	"github.com/ayushsarode/distributed-job-scheduler/internal/api/http/dto"
	"github.com/ayushsarode/distributed-job-scheduler/internal/cache"
	"github.com/ayushsarode/distributed-job-scheduler/internal/models"
	"github.com/ayushsarode/distributed-job-scheduler/internal/repository"
	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
)

type JobHandler struct {
	Jobs        repository.JobsRepository
	Idempotency *cache.IdempotencyStore
}

func NewJobHandler(jobs repository.JobsRepository, idem *cache.IdempotencyStore) *JobHandler {
	return &JobHandler{Jobs: jobs, Idempotency: idem}
}

func (h *JobHandler) SubmitJob(w http.ResponseWriter, r *http.Request) {
	var req dto.SubmitJobRequest

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON body")
		return
	}

	if req.Type == "" {
		writeError(w, http.StatusBadRequest, "type is required")
		return
	}

	job := models.NewJob(req.Type, req.Payload, req.Priority)

	// --- Idempotency check ---
	if h.Idempotency != nil {
		pHash := cache.PayloadHash(job.Type, job.Payload)
		idemKey := r.Header.Get("Idempotency-Key")
		if idemKey == "" {
			idemKey = cache.ContentKey(job.Type, job.Payload)
		}

		existingID, isDuplicate, err := h.Idempotency.Check(r.Context(), idemKey, job.ID, pHash)
		if errors.Is(err, cache.ErrPayloadMismatch) {
			writeError(w, http.StatusConflict, err.Error())
			return
		}
		if err != nil {
			writeError(w, http.StatusInternalServerError, "idempotency check failed")
			return
		}
		if isDuplicate {
			// return the existing job instead of creating a duplicate
			existing, err := h.Jobs.Get(r.Context(), existingID)
			if err != nil {
				writeError(w, http.StatusInternalServerError, "failed to fetch existing job")
				return
			}
			writeJSON(w, http.StatusOK, toJobResponse(existing))
			return
		}
	}
	// --- End idempotency check ---

	created, err := h.Jobs.Create(r.Context(), job)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to create job")
		return
	}

	writeJSON(w, http.StatusCreated, toJobResponse(created))
}

func (h *JobHandler) GetJobStatus(w http.ResponseWriter, r *http.Request) {
	id, err := uuid.Parse(chi.URLParam(r, "id"))
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid job id")
		return
	}

	job, err := h.Jobs.Get(r.Context(), id)
	if errors.Is(err, repository.ErrNotFound) {
		writeError(w, http.StatusNotFound, "job not found")
		return
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to get job")
		return
	}

	writeJSON(w, http.StatusOK, toJobResponse(job))
}

func (h *JobHandler) CancelJob(w http.ResponseWriter, r *http.Request) {
	id, err := uuid.Parse(chi.URLParam(r, "id"))
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid job id")
		return
	}

	err = h.Jobs.Cancel(r.Context(), id)
	if errors.Is(err, repository.ErrNotFound) {
		writeError(w, http.StatusNotFound, "job not found or not cancellable")
		return
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to cancel job")
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

func (h *JobHandler) ListJobs(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()

	params := repository.ListParams{
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
		s := models.JobStatus(v)
		params.Status = &s
	}
	if v := q.Get("type"); v != "" {
		params.Type = &v
	}

	jobs, err := h.Jobs.List(r.Context(), params)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to list jobs")
		return
	}

	// convert []*models.Job → []dto.JobResponse
	items := make([]dto.JobResponse, 0, len(jobs))
	for _, j := range jobs {
		items = append(items, toJobResponse(j))
	}

	writeJSON(w, http.StatusOK, dto.ListJobsResponse{
		Jobs:   items,
		Limit:  params.Limit,
		Offset: params.Offset,
	})
}

// this func maps a models.Job to the API response DTO(data transfer object).
func toJobResponse(j *models.Job) dto.JobResponse {
	resp := dto.JobResponse{
		ID:        j.ID.String(),
		Status:    string(j.Status),
		Type:      j.Type,
		Payload:   j.Payload,
		Priority:  j.Priority,
		Attempts:  j.Attempts,
		CreatedAt: j.CreatedAt,
		UpdatedAt: j.UpdatedAt,
	}
	if j.WorkerID != nil {
		wid := j.WorkerID.String()
		resp.WorkerID = &wid
	}
	return resp
}

// writeJSON encodes v as JSON and writes it with the given status code.
func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, dto.ErrorResponse{Error: msg})
}
