package main

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"
)

func TestTelemetryParsingRoundingAndValidation(t *testing.T) {
	validJSON := []byte(`{
		"device_id": "AGV-004",
		"battery_level": 78.62,
		"x_coord": 44.812,
		"y_coord": 23.195,
		"speed": 1.68,
		"status": "MOVING",
		"timestamp": "2026-11-11T20:15:02.418291Z"
	}`)

	ev, err := parseTelemetryEvent(validJSON)
	if err != nil {
		t.Fatalf("expected valid payload to parse, got error: %v", err)
	}
	if ev.DeviceID != "AGV-004" {
		t.Fatalf("expected DeviceID AGV-004, got %q", ev.DeviceID)
	}
	if got := ev.RoundedBattery(); got != 79 {
		t.Fatalf("expected RoundedBattery 79 for 78.62, got %d", got)
	}

	evDown, err := parseTelemetryEvent([]byte(`{
		"device_id": "AGV-005",
		"battery_level": 78.49,
		"x_coord": 10,
		"y_coord": 20,
		"speed": 0,
		"status": "IDLE",
		"timestamp": "2026-11-11T20:15:02Z"
	}`))
	if err != nil {
		t.Fatalf("expected valid payload to parse, got error: %v", err)
	}
	if got := evDown.RoundedBattery(); got != 78 {
		t.Fatalf("expected RoundedBattery 78 for 78.49, got %d", got)
	}

	invalidCases := []struct {
		name string
		raw  string
	}{
		{
			name: "malformed json",
			raw:  `{"device_id": "AGV-001"`,
		},
		{
			name: "missing device_id",
			raw:  `{"battery_level": 50, "x_coord": 1, "y_coord": 2, "speed": 1, "status": "MOVING", "timestamp": "2026-11-11T20:15:02Z"}`,
		},
		{
			name: "missing status",
			raw:  `{"device_id": "AGV-001", "battery_level": 50, "x_coord": 1, "y_coord": 2, "speed": 1, "timestamp": "2026-11-11T20:15:02Z"}`,
		},
		{
			name: "missing timestamp",
			raw:  `{"device_id": "AGV-001", "battery_level": 50, "x_coord": 1, "y_coord": 2, "speed": 1, "status": "MOVING"}`,
		},
		{
			name: "battery out of range",
			raw:  `{"device_id": "AGV-001", "battery_level": 105, "x_coord": 1, "y_coord": 2, "speed": 1, "status": "MOVING", "timestamp": "2026-11-11T20:15:02Z"}`,
		},
		{
			name: "negative speed",
			raw:  `{"device_id": "AGV-001", "battery_level": 50, "x_coord": 1, "y_coord": 2, "speed": -0.5, "status": "MOVING", "timestamp": "2026-11-11T20:15:02Z"}`,
		},
	}

	for _, tc := range invalidCases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := parseTelemetryEvent([]byte(tc.raw)); err == nil {
				t.Fatalf("expected validation error for %s, got nil", tc.name)
			}
		})
	}
}

func TestHubNonBlockingBroadcastAndBackpressureDrop(t *testing.T) {
	hub := newHub(nil)

	// 1. Fill the broadcast queue without a consumer running hub.run(),
	// then ensure RecordEvent never blocks and records dropped frames.
	totalEvents := cap(hub.broadcast) + 25
	done := make(chan struct{})
	go func() {
		defer close(done)
		ts := time.Date(2026, 11, 11, 20, 15, 0, 0, time.UTC)
		for i := 0; i < totalEvents; i++ {
			hub.RecordEvent(TelemetryEvent{
				DeviceID:     fmt.Sprintf("AGV-%03d", (i%10)+1),
				BatteryLevel: 80.0,
				XCoord:       12.5,
				YCoord:       24.0,
				Speed:        1.5,
				Status:       "MOVING",
				Timestamp:    ts.Add(time.Duration(i) * time.Millisecond),
			})
		}
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("RecordEvent blocked when hub broadcast channel was saturated")
	}

	if got := hub.droppedBroadcasts.Load(); got != 25 {
		t.Fatalf("expected 25 dropped broadcasts, got %d", got)
	}
	if got := hub.totalEvents.Load(); got != int64(totalEvents) {
		t.Fatalf("expected totalEvents=%d, got %d", totalEvents, got)
	}

	// 2. Verify slow client eviction during fanOut while healthy clients still receive frames.
	slowClient := &wsClient{hub: hub, send: make(chan []byte, 1)}
	slowClient.send <- []byte(`{"type":"stale"}`)
	fastClient := &wsClient{hub: hub, send: make(chan []byte, 4)}

	hub.clients[slowClient] = struct{}{}
	hub.clients[fastClient] = struct{}{}

	hub.fanOut([]byte(`{"type":"telemetry"}`))

	if _, exists := hub.clients[slowClient]; exists {
		t.Fatal("expected slow client to be evicted from hub.clients")
	}
	if got := hub.evictedClients.Load(); got != 1 {
		t.Fatalf("expected evictedClients=1, got %d", got)
	}
	if _, exists := hub.clients[fastClient]; !exists {
		t.Fatal("expected fast client to remain registered")
	}
	select {
	case msg := <-fastClient.send:
		if string(msg) != `{"type":"telemetry"}` {
			t.Fatalf("unexpected message delivered to fast client: %s", msg)
		}
	default:
		t.Fatal("expected fast client to receive broadcast frame")
	}
}

func TestBoundedBatchBufferCapAndBackoff(t *testing.T) {
	const (
		testBatchSize = 50
		testMaxCap    = 500
	)
	buf := newBatchBuffer(testBatchSize, testMaxCap, 1*time.Second, 30*time.Second)

	baseTime := time.Date(2026, 11, 11, 20, 0, 0, 0, time.UTC)
	makeRecord := func(seq int) bufferedRecord {
		return bufferedRecord{
			event: TelemetryEvent{
				DeviceID:     fmt.Sprintf("SEQ-%04d", seq),
				BatteryLevel: 75.0,
				XCoord:       10.0,
				YCoord:       20.0,
				Speed:        1.2,
				Status:       "MOVING",
				Timestamp:    baseTime.Add(time.Duration(seq) * time.Second),
			},
		}
	}

	// Append 50 records and fail flush; verify exponential backoff is scheduled.
	for i := 0; i < testBatchSize; i++ {
		buf.append(makeRecord(i))
	}
	errDBDown := errors.New("postgres unreachable")
	if _, err := buf.flush(context.Background(), baseTime, false, func(context.Context, []bufferedRecord) error {
		return errDBDown
	}); !errors.Is(err, errDBDown) {
		t.Fatalf("expected flush error %v, got %v", errDBDown, err)
	}
	if buf.consecutiveFailures != 1 {
		t.Fatalf("expected consecutiveFailures=1, got %d", buf.consecutiveFailures)
	}

	// Retry before backoff elapses (baseTime + 500ms < baseTime + 1s) should be skipped.
	flushCalled := false
	if _, err := buf.flush(context.Background(), baseTime.Add(500*time.Millisecond), false, func(context.Context, []bufferedRecord) error {
		flushCalled = true
		return nil
	}); err != nil {
		t.Fatalf("expected skipped flush during backoff to return nil error, got %v", err)
	}
	if flushCalled {
		t.Fatal("expected flush to be skipped before exponential backoff elapsed")
	}

	// Fill past maxBufferSize (550 total records) while DB remains unreachable.
	for i := testBatchSize; i < 550; i++ {
		buf.append(makeRecord(i))
	}
	if buf.len() > testMaxCap {
		t.Fatalf("buffer exceeded cap %d: got %d", testMaxCap, buf.len())
	}
	if buf.len() != 500 {
		t.Fatalf("expected buffer len 500 after dropping one oldest batch of 50, got %d", buf.len())
	}
	if buf.droppedRecords != 50 {
		t.Fatalf("expected 50 dropped records, got %d", buf.droppedRecords)
	}
	// Oldest 50 (SEQ-0000..SEQ-0049) should have been evicted; first retained is SEQ-0050.
	if buf.records[0].event.DeviceID != "SEQ-0050" {
		t.Fatalf("expected oldest retained record to be SEQ-0050, got %q", buf.records[0].event.DeviceID)
	}
	if buf.records[buf.len()-1].event.DeviceID != "SEQ-0549" {
		t.Fatalf("expected newest retained record to be SEQ-0549, got %q", buf.records[buf.len()-1].event.DeviceID)
	}

	// Once backoff expires and DB recovers, flush clears the buffer and resets failure counters.
	flushed, err := buf.flush(context.Background(), baseTime.Add(2*time.Second), false, func(context.Context, []bufferedRecord) error {
		return nil
	})
	if err != nil {
		t.Fatalf("expected recovery flush to succeed, got %v", err)
	}
	if flushed != 500 {
		t.Fatalf("expected 500 flushed records, got %d", flushed)
	}
	if buf.len() != 0 {
		t.Fatalf("expected empty buffer after flush, got %d", buf.len())
	}
	if buf.consecutiveFailures != 0 {
		t.Fatalf("expected consecutiveFailures reset to 0, got %d", buf.consecutiveFailures)
	}
}
