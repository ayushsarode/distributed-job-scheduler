package cache

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)



const statusCacheTTL = 5 *time.Minute

type StatusCache struct {
	client *redis.Client
}

func NewStatusCache(addr string) *StatusCache {
	return &StatusCache{
		client: redis.NewClient(&redis.Options{Addr: addr}),
	}
}

func (c *StatusCache) Close() error {
	return c.client.Close()
}

func jobKey(id uuid.UUID) string {
	return fmt.Sprintf("job: %s", id.String())
}

func (c *StatusCache) Get(ctx context.Context, id uuid.UUID) ([]byte, error) {
	data, err := c.client.Get(ctx, jobKey(id)).Bytes()
	if err == redis.Nil {
		return nil, nil // cache miss
	}
	return data, err
}

func (c *StatusCache) Set(ctx context.Context, id uuid.UUID, jobJSON []byte) error {
	return c.client.Set(ctx, jobKey(id), jobJSON, statusCacheTTL).Err()
}


func (c *StatusCache) Invalidate(ctx context.Context, id uuid.UUID) error {
	return c.client.Del(ctx, jobKey(id)).Err()
}

