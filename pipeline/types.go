package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"
)

const (
	healthStatusHealthy  = "HEALTHY"
	healthStatusDegraded = "DEGRADED"
)

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

// Validate checks that required fields are present and numeric telemetry values are in valid bounds.
func (e TelemetryEvent) Validate() error {
	if strings.TrimSpace(e.DeviceID) == "" {
		return errors.New("device_id is required")
	}
	if strings.TrimSpace(e.Status) == "" {
		return errors.New("status is required")
	}
	if e.Timestamp.IsZero() {
		return errors.New("timestamp is required")
	}
	if math.IsNaN(e.BatteryLevel) || math.IsInf(e.BatteryLevel, 0) || e.BatteryLevel < 0 || e.BatteryLevel > 100 {
		return fmt.Errorf("battery_level out of range [0, 100]: %v", e.BatteryLevel)
	}
	if math.IsNaN(e.XCoord) || math.IsInf(e.XCoord, 0) {
		return errors.New("x_coord must be finite")
	}
	if math.IsNaN(e.YCoord) || math.IsInf(e.YCoord, 0) {
		return errors.New("y_coord must be finite")
	}
	if math.IsNaN(e.Speed) || math.IsInf(e.Speed, 0) || e.Speed < 0 {
		return fmt.Errorf("speed must be finite and non-negative: %v", e.Speed)
	}
	return nil
}

// RoundedBattery returns the battery percentage rounded to the nearest integer for DB storage.
func (e TelemetryEvent) RoundedBattery() int {
	return int(math.Round(e.BatteryLevel))
}

// parseTelemetryEvent unmarshals and validates a raw JSON telemetry payload.
func parseTelemetryEvent(data []byte) (TelemetryEvent, error) {
	var ev TelemetryEvent
	if err := json.Unmarshal(data, &ev); err != nil {
		return TelemetryEvent{}, err
	}
	if err := ev.Validate(); err != nil {
		return TelemetryEvent{}, err
	}
	return ev, nil
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
