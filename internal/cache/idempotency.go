package cache

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

const idempotencyTTL = 24 * time.Hour

var ErrPayloadMismatch = errors.New("idempotency key reused with different payload")

type IdempotencyStore struct {
	client *redis.Client
}

func NewIdempotencyStore(addr string) *IdempotencyStore {
	return &IdempotencyStore{
		client: redis.NewClient(&redis.Options{Addr: addr}),
	}
}

func (s *IdempotencyStore) Close() error {
	return s.client.Close()
}

type idemEntry struct {
	JobID       string `json:"job_id"`
	PayloadHash string `json:"payload_hash"`
}

func ContentKey(jobType string, payload json.RawMessage) string {
	h := sha256.New()
	h.Write([]byte(jobType))
	h.Write(payload)
	return "auto:" + hex.EncodeToString(h.Sum(nil))
}

func PayloadHash(jobType string, payload json.RawMessage) string {
	h := sha256.New()
	h.Write([]byte(jobType))
	h.Write(payload)
	return hex.EncodeToString(h.Sum(nil))
}

func (s *IdempotencyStore) Check(ctx context.Context, key string, jobID uuid.UUID, pHash string) (uuid.UUID, bool, error) {
	redisKey := fmt.Sprintf("idem:%s", key)

	entry := idemEntry{
		JobID:       jobID.String(),
		PayloadHash: pHash,
	}
	data, err := json.Marshal(entry)
	if err != nil {
		return uuid.Nil, false, fmt.Errorf("marshal idem entry: %w", err)
	}

	// SETNX — only sets if key doesn't exist
	set, err := s.client.SetNX(ctx, redisKey, string(data), idempotencyTTL).Result()
	if err != nil {
		return uuid.Nil, false, fmt.Errorf("redis setnx: %w", err)
	}

	if set {
		// key was newly set — fresh request
		return uuid.Nil, false, nil
	}

	// key already existed, fetch and validate
	existing, err := s.client.Get(ctx, redisKey).Result()
	if err != nil {
		return uuid.Nil, false, fmt.Errorf("redis get: %w", err)
	}

	var existingEntry idemEntry
	if err := json.Unmarshal([]byte(existing), &existingEntry); err != nil {
		return uuid.Nil, false, fmt.Errorf("unmarshal idem entry: %w", err)
	}

	// check for payload mismatch (same key, different content)
	if existingEntry.PayloadHash != pHash {
		return uuid.Nil, false, ErrPayloadMismatch
	}

	existingID, err := uuid.Parse(existingEntry.JobID)
	if err != nil {
		return uuid.Nil, false, fmt.Errorf("parse existing job id: %w", err)
	}

	return existingID, true, nil
}