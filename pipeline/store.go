package main

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const schemaSQL = `
CREATE TABLE IF NOT EXISTS agv_telemetry (
    id SERIAL PRIMARY KEY,
    device_id VARCHAR(32) NOT NULL,
    battery_level INT NOT NULL,
    x_coord DOUBLE PRECISION NOT NULL,
    y_coord DOUBLE PRECISION NOT NULL,
    speed DOUBLE PRECISION NOT NULL,
    status VARCHAR(32) NOT NULL,
    timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_agv_device ON agv_telemetry(device_id);
CREATE INDEX IF NOT EXISTS idx_agv_time ON agv_telemetry(timestamp DESC);
CREATE UNIQUE INDEX IF NOT EXISTS idx_agv_device_time_uniq ON agv_telemetry(device_id, timestamp);
`

const insertSQL = `
INSERT INTO agv_telemetry (device_id, battery_level, x_coord, y_coord, speed, status, timestamp)
VALUES ($1, $2, $3, $4, $5, $6, $7)
ON CONFLICT (device_id, timestamp) DO NOTHING
`

// Store manages the PostgreSQL connection pool, schema verification, and idempotent batch writes.
type Store struct {
	pool *pgxpool.Pool
}

// newStore opens a pgxpool connection, verifies connectivity, and ensures the schema exists.
func newStore(ctx context.Context, dsn string) (*Store, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("create postgres pool: %w", err)
	}

	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping postgres: %w", err)
	}

	s := &Store{pool: pool}
	if err := s.ensureSchema(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ensure schema: %w", err)
	}
	return s, nil
}

// Close releases all pooled database connections.
func (s *Store) Close() {
	if s != nil && s.pool != nil {
		s.pool.Close()
	}
}

func (s *Store) ensureSchema(ctx context.Context) error {
	_, err := s.pool.Exec(ctx, schemaSQL)
	return err
}

// WriteBatch executes an idempotent batch insert for all buffered telemetry records.
func (s *Store) WriteBatch(ctx context.Context, records []bufferedRecord) error {
	if len(records) == 0 {
		return nil
	}

	batch := &pgx.Batch{}
	for _, rec := range records {
		batch.Queue(
			insertSQL,
			rec.event.DeviceID,
			rec.event.RoundedBattery(),
			rec.event.XCoord,
			rec.event.YCoord,
			rec.event.Speed,
			rec.event.Status,
			rec.event.Timestamp,
		)
	}

	br := s.pool.SendBatch(ctx, batch)
	var execErr error
	for range records {
		if _, err := br.Exec(); err != nil && execErr == nil {
			execErr = err
		}
	}
	if closeErr := br.Close(); closeErr != nil && execErr == nil {
		execErr = closeErr
	}
	return execErr
}
