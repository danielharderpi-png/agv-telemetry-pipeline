package main

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"math"
	"net/http"
	"os"
	"os/signal"
	"sort"
	"sync"
	"sync/atomic"
	"strings"
	"syscall"
	"time"

	"github.com/gorilla/websocket"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/segmentio/kafka-go"
)

const (
	defaultBrokerAddr  = "localhost:19092"
	defaultTopic       = "agv-telemetry"
	defaultGroupID     = "agv-indexer"
	defaultPostgresDSN = "postgres://agv_admin:agv_password@127.0.0.1:5433/warehouse_telemetry?sslmode=disable"
	defaultHTTPAddr    = ":8081"
	batchSize          = 50
	batchInterval      = 1 * time.Second

	wsWriteWait  = 5 * time.Second
	wsPongWait   = 60 * time.Second
	wsPingPeriod = 30 * time.Second
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

// PipelineMetrics reports real-time throughput and fleet health over WebSocket.
type PipelineMetrics struct {
	Status       string    `json:"status"`
	EventsPerSec float64   `json:"events_per_sec"`
	ActiveAGVs   int       `json:"active_agvs"`
	TotalEvents  int64     `json:"total_events"`
	TotalIndexed int64     `json:"total_indexed"`
	Timestamp    time.Time `json:"timestamp"`
}

type wsEnvelope struct {
	Type    string           `json:"type"`
	Event   *TelemetryEvent  `json:"event,omitempty"`
	AGVs    []TelemetryEvent `json:"agvs,omitempty"`
	Metrics *PipelineMetrics `json:"metrics,omitempty"`
}

var wsUpgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 4096,
	CheckOrigin: func(r *http.Request) bool {
		return true
	},
}

type wsClient struct {
	hub  *Hub
	conn *websocket.Conn
	send chan []byte
}

func (c *wsClient) readPump() {
	defer func() {
		select {
		case c.hub.unregister <- c:
		case <-c.hub.done:
		}
		_ = c.conn.Close()
	}()
	c.conn.SetReadLimit(4096)
	_ = c.conn.SetReadDeadline(time.Now().Add(wsPongWait))
	c.conn.SetPongHandler(func(string) error {
		return c.conn.SetReadDeadline(time.Now().Add(wsPongWait))
	})
	for {
		if _, _, err := c.conn.ReadMessage(); err != nil {
			break
		}
	}
}

func (c *wsClient) writePump() {
	ticker := time.NewTicker(wsPingPeriod)
	defer func() {
		ticker.Stop()
		_ = c.conn.Close()
	}()
	for {
		select {
		case msg, ok := <-c.send:
			_ = c.conn.SetWriteDeadline(time.Now().Add(wsWriteWait))
			if !ok {
				_ = c.conn.WriteMessage(websocket.CloseMessage, []byte{})
				return
			}
			if err := c.conn.WriteMessage(websocket.TextMessage, msg); err != nil {
				return
			}
		case <-ticker.C:
			_ = c.conn.SetWriteDeadline(time.Now().Add(wsWriteWait))
			if err := c.conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				return
			}
		}
	}
}

// Hub maintains the latest AGV state map, throughput counters, and WebSocket subscribers.
type Hub struct {
	mu           sync.RWMutex
	latest       map[string]TelemetryEvent
	lastRate     float64
	windowEvents atomic.Int64
	totalEvents  atomic.Int64
	totalIndexed atomic.Int64

	clients    map[*wsClient]struct{}
	register   chan *wsClient
	unregister chan *wsClient
	broadcast  chan []byte
	done       chan struct{}
}

func newHub() *Hub {
	return &Hub{
		latest:     make(map[string]TelemetryEvent),
		clients:    make(map[*wsClient]struct{}),
		register:   make(chan *wsClient, 32),
		unregister: make(chan *wsClient, 32),
		broadcast:  make(chan []byte, 512),
		done:       make(chan struct{}),
	}
}

// RecordEvent updates the in-memory AGV state map and broadcasts without blocking the caller.
func (h *Hub) RecordEvent(ev TelemetryEvent) {
	h.windowEvents.Add(1)
	h.totalEvents.Add(1)

	h.mu.Lock()
	h.latest[ev.DeviceID] = ev
	h.mu.Unlock()

	payload, err := json.Marshal(wsEnvelope{
		Type:  "telemetry",
		Event: &ev,
	})
	if err != nil {
		return
	}

	select {
	case h.broadcast <- payload:
	default:
		// Drop broadcast frame if hub queue is saturated so Kafka/Postgres never blocks.
	}
}

// AddIndexed records the number of events persisted to Postgres.
func (h *Hub) AddIndexed(n int) {
	h.totalIndexed.Add(int64(n))
}

func (h *Hub) currentMetrics(rate float64) PipelineMetrics {
	h.mu.RLock()
	active := len(h.latest)
	h.mu.RUnlock()

	return PipelineMetrics{
		Status:       "HEALTHY",
		EventsPerSec: rate,
		ActiveAGVs:   active,
		TotalEvents:  h.totalEvents.Load(),
		TotalIndexed: h.totalIndexed.Load(),
		Timestamp:    time.Now().UTC(),
	}
}

func (h *Hub) buildSnapshot() []byte {
	h.mu.RLock()
	agvs := make([]TelemetryEvent, 0, len(h.latest))
	for _, ev := range h.latest {
		agvs = append(agvs, ev)
	}
	rate := h.lastRate
	h.mu.RUnlock()

	sort.Slice(agvs, func(i, j int) bool {
		return agvs[i].DeviceID < agvs[j].DeviceID
	})

	metrics := PipelineMetrics{
		Status:       "HEALTHY",
		EventsPerSec: rate,
		ActiveAGVs:   len(agvs),
		TotalEvents:  h.totalEvents.Load(),
		TotalIndexed: h.totalIndexed.Load(),
		Timestamp:    time.Now().UTC(),
	}

	data, err := json.Marshal(wsEnvelope{
		Type:    "snapshot",
		AGVs:    agvs,
		Metrics: &metrics,
	})
	if err != nil {
		return nil
	}
	return data
}

func (h *Hub) fanOut(msg []byte) {
	for client := range h.clients {
		select {
		case client.send <- msg:
		default:
			close(client.send)
			delete(h.clients, client)
		}
	}
}

func (h *Hub) run(ctx context.Context) {
	defer close(h.done)
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()
	lastTick := time.Now()

	for {
		select {
		case <-ctx.Done():
			for client := range h.clients {
				close(client.send)
				delete(h.clients, client)
			}
			return

		case client := <-h.register:
			h.clients[client] = struct{}{}
			if snap := h.buildSnapshot(); snap != nil {
				select {
				case client.send <- snap:
				default:
					close(client.send)
					delete(h.clients, client)
				}
			}

		case client := <-h.unregister:
			if _, ok := h.clients[client]; ok {
				delete(h.clients, client)
				close(client.send)
			}

		case msg := <-h.broadcast:
			h.fanOut(msg)

		case now := <-ticker.C:
			elapsed := now.Sub(lastTick).Seconds()
			lastTick = now
			count := h.windowEvents.Swap(0)
			rate := 0.0
			if elapsed > 0 {
				rate = math.Round((float64(count)/elapsed)*10) / 10
			}
			h.mu.Lock()
			h.lastRate = rate
			h.mu.Unlock()

			metrics := h.currentMetrics(rate)
			if payload, err := json.Marshal(wsEnvelope{
				Type:    "metrics",
				Metrics: &metrics,
			}); err == nil {
				h.fanOut(payload)
			}
		}
	}
}

func (h *Hub) serveWS(ctx context.Context, w http.ResponseWriter, r *http.Request) {
	conn, err := wsUpgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("websocket upgrade failed: %v", err)
		return
	}

	client := &wsClient{
		hub:  h,
		conn: conn,
		send: make(chan []byte, 256),
	}

	select {
	case h.register <- client:
	case <-h.done:
		_ = conn.Close()
		return
	case <-ctx.Done():
		_ = conn.Close()
		return
	}

	go client.writePump()
	go client.readPump()
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
	httpAddr := envOrDefault("HTTP_ADDR", defaultHTTPAddr)
	webDir := resolveWebDir()

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

	hub := newHub()
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
		hub.AddIndexed(len(buffer))
		buffer = buffer[:0]
	}

	shutdown := func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := httpSrv.Shutdown(shutdownCtx); err != nil {
			log.Printf("HTTP server shutdown error: %v", err)
		}
		flush(shutdownCtx)
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

			var ev TelemetryEvent
			if err := json.Unmarshal(msg.Value, &ev); err != nil {
				log.Printf("skipping invalid JSON at offset %d: %v", msg.Offset, err)
				if commitErr := reader.CommitMessages(ctx, msg); commitErr != nil && ctx.Err() == nil {
					log.Printf("error committing skipped message: %v", commitErr)
				}
				continue
			}

			hub.RecordEvent(ev)
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
