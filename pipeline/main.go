package main

import (
	"context"
	"errors"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/segmentio/kafka-go"
)

const (
	defaultBrokerAddr  = "localhost:19092"
	defaultTopic       = "agv-telemetry"
	defaultGroupID     = "agv-indexer"
	defaultPostgresDSN = "postgres://agv_admin:agv_password@127.0.0.1:5433/warehouse_telemetry?sslmode=disable"
	defaultHTTPAddr    = ":8081"

	batchSize      = 50
	maxBufferSize  = 500
	batchInterval  = 1 * time.Second
	baseRetryDelay = 1 * time.Second
	maxRetryDelay  = 30 * time.Second
)

// batchBuffer buffers telemetry records up to maxSize and applies exponential backoff on flush failures.
type batchBuffer struct {
	records             []bufferedRecord
	batchSize           int
	maxSize             int
	baseBackoff         time.Duration
	maxBackoff          time.Duration
	consecutiveFailures int
	nextRetryAt         time.Time
	droppedRecords      int64
}

func newBatchBuffer(batchSz, maxSz int, baseBackoff, maxBackoff time.Duration) *batchBuffer {
	return &batchBuffer{
		records:     make([]bufferedRecord, 0, batchSz),
		batchSize:   batchSz,
		maxSize:     maxSz,
		baseBackoff: baseBackoff,
		maxBackoff:  maxBackoff,
	}
}

// append adds a record to the buffer, evicting the oldest batch if the buffer cap is reached.
func (b *batchBuffer) append(rec bufferedRecord) int {
	dropped := 0
	if len(b.records) >= b.maxSize {
		dropCount := b.batchSize
		if dropCount <= 0 || dropCount > len(b.records) {
			dropCount = len(b.records)
		}
		if len(b.records)-dropCount >= b.maxSize {
			dropCount = len(b.records) - b.maxSize + 1
		}

		remaining := len(b.records) - dropCount
		copy(b.records, b.records[dropCount:])
		clear(b.records[remaining:])
		b.records = b.records[:remaining]
		b.droppedRecords += int64(dropCount)
		dropped = dropCount

		log.Printf(
			"[WARN] batch buffer reached cap (%d); dropped oldest %d buffered records (total dropped=%d)",
			b.maxSize, dropCount, b.droppedRecords,
		)
	}
	b.records = append(b.records, rec)
	return dropped
}

func (b *batchBuffer) len() int {
	return len(b.records)
}

func (b *batchBuffer) shouldFlush() bool {
	return len(b.records) >= b.batchSize
}

func (b *batchBuffer) nextBackoff() time.Duration {
	shift := b.consecutiveFailures - 1
	if shift < 0 {
		shift = 0
	}
	if shift > 10 {
		shift = 10
	}
	delay := b.baseBackoff << shift
	if delay <= 0 || delay > b.maxBackoff {
		return b.maxBackoff
	}
	return delay
}

// flush attempts to persist buffered records via fn, enforcing exponential backoff after failures.
func (b *batchBuffer) flush(
	ctx context.Context,
	now time.Time,
	force bool,
	fn func(context.Context, []bufferedRecord) error,
) (int, error) {
	if len(b.records) == 0 {
		return 0, nil
	}
	if !force && !b.nextRetryAt.IsZero() && now.Before(b.nextRetryAt) {
		return 0, nil
	}

	if err := fn(ctx, b.records); err != nil {
		b.consecutiveFailures++
		backoff := b.nextBackoff()
		b.nextRetryAt = now.Add(backoff)
		return 0, err
	}

	flushed := len(b.records)
	clear(b.records)
	b.records = b.records[:0]
	b.consecutiveFailures = 0
	b.nextRetryAt = time.Time{}
	return flushed, nil
}

func resolveWebDir() string {
	if dir := strings.TrimSpace(os.Getenv("WEB_DIR")); dir != "" {
		return dir
	}
	if info, err := os.Stat("../web"); err == nil && info.IsDir() {
		return "../web"
	}
	if info, err := os.Stat("web"); err == nil && info.IsDir() {
		return "web"
	}
	return "../web"
}

func envOrDefault(key, fallback string) string {
	if val := strings.TrimSpace(os.Getenv(key)); val != "" {
		return val
	}
	return fallback
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	dsn := envOrDefault("POSTGRES_DSN", defaultPostgresDSN)
	brokers := strings.Split(envOrDefault("KAFKA_BROKERS", defaultBrokerAddr), ",")
	topic := envOrDefault("KAFKA_TOPIC", defaultTopic)
	groupID := envOrDefault("KAFKA_GROUP_ID", defaultGroupID)
	httpAddr := envOrDefault("HTTP_ADDR", defaultHTTPAddr)
	allowedOrigins := parseAllowedOrigins(os.Getenv("ALLOWED_ORIGINS"))
	webDir := resolveWebDir()

	store, err := newStore(ctx, dsn)
	if err != nil {
		log.Fatalf("failed to initialize postgres store: %v", err)
	}
	defer store.Close()
	log.Println("Connected to PostgreSQL and verified agv_telemetry schema.")

	hub := newHub(allowedOrigins)
	go hub.run(ctx)

	mux := http.NewServeMux()
	mux.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
		hub.serveWS(ctx, w, r)
	})
	mux.Handle("/", http.FileServer(http.Dir(webDir)))

	httpSrv := &http.Server{
		Addr:              httpAddr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}
	go func() {
		log.Printf("Dashboard & WebSocket server listening on %s (serving %s)", httpAddr, webDir)
		if err := httpSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Printf("HTTP server error: %v", err)
		}
	}()

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

	buf := newBatchBuffer(batchSize, maxBufferSize, baseRetryDelay, maxRetryDelay)
	ticker := time.NewTicker(batchInterval)
	defer ticker.Stop()

	persistAndCommit := func(flushCtx context.Context, records []bufferedRecord) error {
		if err := store.WriteBatch(flushCtx, records); err != nil {
			return err
		}
		msgs := make([]kafka.Message, len(records))
		for i, rec := range records {
			msgs[i] = rec.msg
		}
		if err := reader.CommitMessages(flushCtx, msgs...); err != nil {
			return err
		}
		log.Printf("[INDEXED] %d events batch-inserted into Postgres", len(records))
		return nil
	}

	tryFlush := func(flushCtx context.Context, force bool) {
		n, err := buf.flush(flushCtx, time.Now(), force, persistAndCommit)
		if err != nil {
			hub.SetHealthy(false)
			log.Printf("error flushing batch of %d events (next retry in %s): %v", buf.len(), buf.nextBackoff(), err)
			return
		}
		if n > 0 {
			hub.SetHealthy(true)
			hub.AddIndexed(n)
		}
	}

	shutdown := func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := httpSrv.Shutdown(shutdownCtx); err != nil {
			log.Printf("HTTP server shutdown error: %v", err)
		}
		tryFlush(shutdownCtx, true)
	}

	for {
		select {
		case <-ctx.Done():
			log.Println("Shutdown signal received, flushing remaining events...")
			shutdown()
			log.Println("Pipeline stopped cleanly.")
			return

		case msg, ok := <-msgCh:
			if !ok {
				shutdown()
				return
			}

			ev, err := parseTelemetryEvent(msg.Value)
			if err != nil {
				log.Printf("skipping invalid telemetry at offset %d: %v", msg.Offset, err)
				if commitErr := reader.CommitMessages(ctx, msg); commitErr != nil && ctx.Err() == nil {
					log.Printf("error committing skipped message: %v", commitErr)
				}
				continue
			}

			hub.RecordEvent(ev)
			buf.append(bufferedRecord{event: ev, msg: msg})
			if buf.shouldFlush() {
				tryFlush(ctx, false)
				ticker.Reset(batchInterval)
			}

		case <-ticker.C:
			tryFlush(ctx, false)
		}
	}
}
