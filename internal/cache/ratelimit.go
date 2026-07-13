package cache

import (
	"context"
	"fmt"
	"time"
	"github.com/redis/go-redis/v9"
)
type RateLimiter struct {
	client   *redis.Client
	limit    int
	window   time.Duration
}

func NewRateLimiter(addr string, limit int, window time.Duration) *RateLimiter {
	return &RateLimiter{
		client: redis.NewClient(&redis.Options{Addr: addr}),
		limit:  limit,
		window: window,
	}
}

func (rl *RateLimiter) Close() error {
	return rl.client.Close()
}

func (rl *RateLimiter) Allow(ctx context.Context, key string) (bool, int, error) {
	redisKey := fmt.Sprintf("rate:%s:%d", key, time.Now().Unix()/int64(rl.window.Seconds()))
	count, err := rl.client.Incr(ctx, redisKey).Result()
	if err != nil {
		return false, 0, fmt.Errorf("redis incr: %w", err)
	}
	// set TTL on first request in this window
	if count == 1 {
		rl.client.Expire(ctx, redisKey, rl.window)
	}
	remaining := rl.limit - int(count)
	if remaining < 0 {
		remaining = 0
	}
	return int(count) <= rl.limit, remaining, nil
}