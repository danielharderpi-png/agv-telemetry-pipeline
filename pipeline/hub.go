package main

import (
	"context"
	"encoding/json"
	"log"
	"math"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
)

const (
	wsWriteWait  = 5 * time.Second
	wsPongWait   = 60 * time.Second
	wsPingPeriod = 30 * time.Second
)

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

// parseAllowedOrigins splits a comma-separated ALLOWED_ORIGINS string into trimmed non-empty entries.
func parseAllowedOrigins(raw string) []string {
	if strings.TrimSpace(raw) == "" {
		return nil
	}
	parts := strings.Split(raw, ",")
	origins := make([]string, 0, len(parts))
	for _, p := range parts {
		if trimmed := strings.TrimSpace(p); trimmed != "" {
			origins = append(origins, trimmed)
		}
	}
	return origins
}

// isAllowedOrigin checks whether a WebSocket Origin header satisfies ALLOWED_ORIGINS or local loopback defaults.
func isAllowedOrigin(origin string, allowed []string) bool {
	if origin == "" {
		return true
	}
	u, err := url.Parse(origin)
	if err != nil || u.Host == "" {
		return false
	}
	host := strings.ToLower(u.Hostname())

	if len(allowed) == 0 {
		return host == "localhost" || host == "127.0.0.1" || host == "::1"
	}

	for _, entry := range allowed {
		if entry == "*" || strings.EqualFold(origin, entry) || strings.EqualFold(u.Host, entry) || strings.EqualFold(host, entry) {
			return true
		}
	}
	return false
}

// Hub maintains the latest AGV state map, throughput/health counters, and WebSocket subscribers.
type Hub struct {
	mu                sync.RWMutex
	latest            map[string]TelemetryEvent
	lastRate          float64
	degraded          atomic.Bool
	windowEvents      atomic.Int64
	totalEvents       atomic.Int64
	totalIndexed      atomic.Int64
	droppedBroadcasts atomic.Int64
	evictedClients    atomic.Int64

	clients    map[*wsClient]struct{}
	register   chan *wsClient
	unregister chan *wsClient
	broadcast  chan []byte
	done       chan struct{}
	upgrader   websocket.Upgrader
}

func newHub(allowedOrigins []string) *Hub {
	return &Hub{
		latest:     make(map[string]TelemetryEvent),
		clients:    make(map[*wsClient]struct{}),
		register:   make(chan *wsClient, 32),
		unregister: make(chan *wsClient, 32),
		broadcast:  make(chan []byte, 512),
		done:       make(chan struct{}),
		upgrader: websocket.Upgrader{
			ReadBufferSize:  1024,
			WriteBufferSize: 4096,
			CheckOrigin: func(r *http.Request) bool {
				return isAllowedOrigin(r.Header.Get("Origin"), allowedOrigins)
			},
		},
	}
}

// SetHealthy updates the reported pipeline health status ("HEALTHY" vs "DEGRADED").
func (h *Hub) SetHealthy(healthy bool) {
	h.degraded.Store(!healthy)
}

// HealthStatus returns the current pipeline health state string.
func (h *Hub) HealthStatus() string {
	if h.degraded.Load() {
		return healthStatusDegraded
	}
	return healthStatusHealthy
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
		h.droppedBroadcasts.Add(1)
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
		Status:       h.HealthStatus(),
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
		Status:       h.HealthStatus(),
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
			h.evictedClients.Add(1)
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
					h.evictedClients.Add(1)
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
	conn, err := h.upgrader.Upgrade(w, r, nil)
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
