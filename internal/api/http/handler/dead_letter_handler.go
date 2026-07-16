package handler

import (
	"errors"
	"net/http"
	"strconv"

	"github.com/ayushsarode/distributed-job-scheduler/internal/repository"
	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
)

type DeadLetterHandler struct {
	DeadLetters repository.DeadLettersRepository
}

func NewDeadLetterHandler(deadLetters repository.DeadLettersRepository) *DeadLetterHandler {
	return &DeadLetterHandler{DeadLetters: deadLetters}
}

func (h *DeadLetterHandler) List(w http.ResponseWriter, r *http.Request) {
	limit := 50
	offset := 0
	q := r.URL.Query()

	if v := q.Get("limit"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			limit = n
		}
	}
	if v := q.Get("offset"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n >= 0 {
			offset = n
		}
	}

	items, err := h.DeadLetters.List(r.Context(), limit, offset)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to list dead letters")
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"dead_letters": items,
		"limit":        limit,
		"offset":       offset,
	})
}

func (h *DeadLetterHandler) Get(w http.ResponseWriter, r *http.Request) {
	id, err := uuid.Parse(chi.URLParam(r, "id"))
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid dead letter id")
		return
	}

	deadLetter, err := h.DeadLetters.Get(r.Context(), id)
	if errors.Is(err, repository.ErrNotFound) {
		writeError(w, http.StatusNotFound, "dead letter not found")
		return
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to get dead letter")
		return
	}

	writeJSON(w, http.StatusOK, deadLetter)
}

func (h *DeadLetterHandler) Delete(w http.ResponseWriter, r *http.Request) {
	id, err := uuid.Parse(chi.URLParam(r, "id"))
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid dead letter id")
		return
	}

	err = h.DeadLetters.Delete(r.Context(), id)
	if errors.Is(err, repository.ErrNotFound) {
		writeError(w, http.StatusNotFound, "dead letter not found")
		return
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to delete dead letter")
		return
	}

	w.WriteHeader(http.StatusNoContent)
}
