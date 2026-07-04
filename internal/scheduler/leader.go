package scheduler

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rs/zerolog"
)

const schedulerLockID = 851_001

type LeaderElector struct {
	pool *pgxpool.Pool
	conn *pgxpool.Conn
	log zerolog.Logger
}

func NewLeaderElector(pool *pgxpool.Pool, log zerolog.Logger) *LeaderElector {
	return &LeaderElector{pool: pool, log: log}
}

func (l *LeaderElector) Campaign(ctx context.Context) (bool, error) {
	if l.conn != nil {
		return true, nil
	}

	conn, err := l.pool.Acquire(ctx)
	if err != nil {
		return false, fmt.Errorf("acquire conn: %w", err)
	}

	var acquired bool
	err = conn.QueryRow(ctx, `SELECT pg_try_advisory_lock($1)`, schedulerLockID).Scan(&acquired)

	if err != nil {
		conn.Release()
		return false, fmt.Errorf("try advisory lock: %w", err)
	}

	if !acquired {
		conn.Release()
		return false, nil
	}

	l.conn = conn
	l.log.Info().Msg("became scheduler leader")
	return true, nil
}

func (l *LeaderElector) Isleader() bool {
	return l.conn != nil
}

func (l *LeaderElector) Resign(ctx context.Context) error {
	if l.conn == nil {
		return nil
	}
	_, err := l.conn.Exec(ctx, `SELECT pg_advisory_unlock($1)`, schedulerLockID)
	l.conn.Release()
	l.conn = nil
	l.log.Info().Msg("resigned scheduler leadership")
	return err
}