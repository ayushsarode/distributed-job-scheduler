package collector

import (
	"context"

	"github.com/ayushsarode/distributed-job-scheduler/internal/broker"
	"github.com/ayushsarode/distributed-job-scheduler/internal/cache"
	"github.com/ayushsarode/distributed-job-scheduler/internal/repository"
	"github.com/rs/zerolog"
)

const maxAttempts = 3

type ResultCollector struct {
    Jobs     repository.JobsRepository
    Producer *broker.Producer
    Log      zerolog.Logger
	StatusCache   *cache.StatusCache
}

func (rc *ResultCollector) HandleResult(ctx context.Context, key string, value []byte) error {
    var msg broker.ResultMessage
    if err := broker.Decode(value, &msg); err != nil {
        return err
    }

    // invalidate cache on any status change
    defer func() {
        if rc.StatusCache != nil {
            rc.StatusCache.Invalidate(ctx, msg.JobID)
        }
    }()

    if msg.Success {
        return rc.Jobs.MarkCompleted(ctx, msg.JobID)
    }

    status, err := rc.Jobs.MarkFailed(ctx, msg.JobID, maxAttempts)
    if err != nil {
        return err
    }

    if status == "DEAD" {
        return rc.Producer.Publish(ctx, broker.TopicDeadLetter, msg.JobID.String(), msg)
    }

    return nil
}