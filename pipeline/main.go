package main

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"math"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/segmentio/kafka-go"
)

const (
	defaultBrokerAddr  = "localhost:19092"
	defaultTopic       = "agv-telemetry"
	defaultGroupID     = "agv-indexer"
	defaultPostgresDSN = "postgres://agv_admin:agv_password@127.0.0.1:5433/warehouse_telemetry?sslmode=disable"
	batchSize          = 50
	batchInterval      = 1 * time.Second
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
`

const insertSQL = `
INSERT INTO agv_telemetry (device_id, battery_level, x_coord, y_coord, speed, status, timestamp)
VALUES ($1, $2, $3, $4, $5, $6, $7)
`

// TelemetryEvent matches the JSON payload published by the AGV simulator.
type TelemetryEvent struct {
	DeviceID     string    `json:"device_id"`
	BatteryLevel float64   `json:"battery_level"`
	XCoord       float64   `json:"x_coord"`
	YCoord       float64   `json:"y_coord"`
	Speed        float64   `json:"speed"`
	Status       string    `json:"status"`
	Timestamp    time.Time `json:"timestamp"`
}

type bufferedRecord struct {
	event TelemetryEvent
	msg   kafka.Message
}

func envOrDefault(key, fallback string) string {
	if val := strings.TrimSpace(os.Getenv(key)); val != "" {
		return val
	}
	return fallback
}

func ensureSchema(ctx context.Context, pool *pgxpool.Pool) error {
	_, err := pool.Exec(ctx, schemaSQL)
	return err
}

func flushBatch(
	ctx context.Context,
	pool *pgxpool.Pool,
	reader *kafka.Reader,
	buffer []bufferedRecord,
) error {
	if len(buffer) == 0 {
		return nil
	}

	batch := &pgx.Batch{}
	msgs := make([]kafka.Message, 0, len(buffer))
	for _, rec := range buffer {
		batch.Queue(
			insertSQL,
			rec.event.DeviceID,
			int(math.Round(rec.event.BatteryLevel)),
			rec.event.XCoord,
			rec.event.YCoord,
			rec.event.Speed,
			rec.event.Status,
			rec.event.Timestamp,
		)
		msgs = append(msgs, rec.msg)
	}

	br := pool.SendBatch(ctx, batch)
	var execErr error
	for range buffer {
		if _, err := br.Exec(); err != nil && execErr == nil {
			execErr = err
		}
	}
	if closeErr := br.Close(); closeErr != nil && execErr == nil {
		execErr = closeErr
	}
	if execErr != nil {
		return execErr
	}

	if err := reader.CommitMessages(ctx, msgs...); err != nil {
		return err
	}

	log.Printf("[INDEXED] %d events batch-inserted into Postgres", len(buffer))
	return nil
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	dsn := envOrDefault("POSTGRES_DSN", defaultPostgresDSN)
	brokers := strings.Split(envOrDefault("KAFKA_BROKERS", defaultBrokerAddr), ",")
	topic := envOrDefault("KAFKA_TOPIC", defaultTopic)
	groupID := envOrDefault("KAFKA_GROUP_ID", defaultGroupID)

	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		log.Fatalf("failed to create postgres pool: %v", err)
	}
	defer pool.Close()

	if err := pool.Ping(ctx); err != nil {
		log.Fatalf("failed to connect to postgres: %v", err)
	}
	if err := ensureSchema(ctx, pool); err != nil {
		log.Fatalf("failed to ensure agv_telemetry schema: %v", err)
	}
	log.Println("Connected to PostgreSQL and verified agv_telemetry schema.")

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  brokers,
		Topic:    topic,
		GroupID:  groupID,
		MinBytes: 1,
		MaxBytes: 10 * 1024 * 1024,
	})
	defer func() {
		if err := reader.Close(); err != nil {
			log.Printf("error closing kafka reader: %v", err)
		}
	}()
	log.Printf("Consuming topic %q from %v under group %q", topic, brokers, groupID)

	msgCh := make(chan kafka.Message, batchSize)
	go func() {
		defer close(msgCh)
		for {
			msg, err := reader.FetchMessage(ctx)
			if err != nil {
				if errors.Is(err, context.Canceled) || ctx.Err() != nil {
					return
				}
				log.Printf("error fetching message: %v", err)
				select {
				case <-time.After(500 * time.Millisecond):
					continue
				case <-ctx.Done():
					return
				}
			}
			select {
			case msgCh <- msg:
			case <-ctx.Done():
				return
			}
		}
	}()

	buffer := make([]bufferedRecord, 0, batchSize)
	ticker := time.NewTicker(batchInterval)
	defer ticker.Stop()

	flush := func(flushCtx context.Context) {
		if len(buffer) == 0 {
			return
		}
		if err := flushBatch(flushCtx, pool, reader, buffer); err != nil {
			log.Printf("error flushing batch of %d events: %v", len(buffer), err)
			return
		}
		buffer = buffer[:0]
	}

	for {
		select {
		case <-ctx.Done():
			log.Println("Shutdown signal received, flushing remaining events...")
			shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			flush(shutdownCtx)
			cancel()
			log.Println("Pipeline stopped cleanly.")
			return

		case msg, ok := <-msgCh:
			if !ok {
				shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				flush(shutdownCtx)
				cancel()
				return
			}

			var ev TelemetryEvent
			if err := json.Unmarshal(msg.Value, &ev); err != nil {
				log.Printf("skipping invalid JSON at offset %d: %v", msg.Offset, err)
				if commitErr := reader.CommitMessages(ctx, msg); commitErr != nil && ctx.Err() == nil {
					log.Printf("error committing skipped message: %v", commitErr)
				}
				continue
			}

			buffer = append(buffer, bufferedRecord{event: ev, msg: msg})
			if len(buffer) >= batchSize {
				flush(ctx)
				ticker.Reset(batchInterval)
			}

		case <-ticker.C:
			flush(ctx)
		}
	}
}
