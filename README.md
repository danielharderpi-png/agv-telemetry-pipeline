# AGV Telemetry Pipeline

Event-driven warehouse AGV (Automated Guided Vehicle) telemetry ingestion pipeline and real-time dispatch control board. Designed around a simulated 10-AGV fleet workload at 20 events/sec and architected for buffered batch ingestion with backpressure-safe WebSocket fan-out.

[![CI](https://github.com/danielharderpi-png/agv-telemetry-pipeline/actions/workflows/ci.yml/badge.svg)](https://github.com/danielharderpi-png/agv-telemetry-pipeline/actions/workflows/ci.yml)
![Go](https://img.shields.io/badge/Go-1.22+-00ADD8?style=flat-square&logo=go&logoColor=white)
![Python](https://img.shields.io/badge/Python-3.10+-3776AB?style=flat-square&logo=python&logoColor=white)
![Redpanda](https://img.shields.io/badge/Redpanda-Kafka%20API-E94E32?style=flat-square)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-16-4169E1?style=flat-square&logo=postgresql&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?style=flat-square&logo=docker&logoColor=white)
![WebSockets](https://img.shields.io/badge/WebSockets-Gorilla-0F172A?style=flat-square)
![License: MIT](https://img.shields.io/badge/License-MIT-green?style=flat-square)

---

## Live Demo

https://github.com/user-attachments/assets/b9192ccc-ff49-4957-9cf3-ea9ddd83e811

---

## System Architecture

Telemetry flows from simulated warehouse vehicles through Redpanda into a Go ingestion service that splits each event across a non-blocking real-time WebSocket hub and a bounded, idempotent PostgreSQL batch writer.

### Flow Diagram

```mermaid
flowchart LR
    SIM["Python Fleet Simulator\n10 AGVs | 20 events/sec\nsimulator/producer.py"]
    RP["Redpanda Broker\nlocalhost:19092\ntopic: agv-telemetry"]
    GO["Go Consumer & Hub\npipeline/main.go\ngroup: agv-indexer"]
    BATCH["Bounded Batch Persister\npgx/v5 SendBatch\n50 records or 1s flush | cap 500"]
    PG[("PostgreSQL 16\n127.0.0.1:5433\nwarehouse_telemetry")]
    WS["WebSocket Broadcaster\n:8081/ws\nNon-blocking fan-out"]
    UI["2D Dispatch Control UI\nweb/index.html\n100m x 80m Canvas"]

    SIM -->|"JSON events (key: device_id)"| RP
    RP -->|"FetchMessage"| GO
    GO -->|"Append to bounded buffer"| BATCH
    BATCH -->|"INSERT ... ON CONFLICT DO NOTHING"| PG
    BATCH -.->|"CommitMessages on DB success"| RP
    GO -->|"RecordEvent (drop-on-full)"| WS
    WS -->|"snapshot / telemetry / metrics"| UI
```

### Component Topology

```text
+----------------------------------+
|  Python Fleet Simulator          |
|  simulator/producer.py           |
|  - 10 AGVs (AGV-001 .. AGV-010)  |
|  - 20 events/sec                 |
|  - State machine & waypoints     |
+----------------+-----------------+
                 |  Kafka protocol (topic: agv-telemetry)
                 v
+----------------------------------+
|  Redpanda Broker (:19092)        |
|  Redpanda Console (:8080)        |
+----------------+-----------------+
                 |  Consumer group: agv-indexer
                 v
+-------------------------------------------------------------------+
|  Go Ingestion & Dispatch Server (pipeline/)                       |
|                                                                   |
|  +-----------------------------+   +---------------------------+  |
|  | Buffered Batch Persister    |   | WebSocket Hub (:8081/ws)  |  |
|  | - store.go (pgx/v5 batch)   |   | - hub.go (origin-checked) |  |
|  | - Flush: 50 ev or 1s        |   | - In-memory state map     |  |
|  | - Cap: 500 + exp backoff    |   | - HEALTHY/DEGRADED state  |  |
|  | - ON CONFLICT DO NOTHING    |   | - Drop-on-full fan-out    |  |
|  +--------------+--------------+   +-------------+-------------+  |
+-----------------|--------------------------------|----------------+
                  |                                |
                  v                                v
+----------------------------------+  +-----------------------------+
|  PostgreSQL 16 (host :5433)      |  | 2D Dispatch UI (:8081/)     |
|  db: warehouse_telemetry         |  | web/index.html              |
|  table: agv_telemetry            |  | 100m x 80m live floor map   |
+----------------------------------+  +-----------------------------+
```

---

## Key Engineering Highlights

### 1. Backpressure-Safe Decoupling (Real-Time vs. Persistence)
- `Hub.RecordEvent` updates the in-memory `latest` vehicle map under a short-lived `sync.RWMutex`, increments atomic throughput counters, and sends serialized JSON to a buffered `broadcast` channel (capacity `512`) using a non-blocking `select` with a `default` branch.
- Each WebSocket subscriber gets its own buffered `send` channel (capacity `256`) serviced by a dedicated `writePump` goroutine. If a browser tab stalls or disconnects, `Hub.fanOut` closes and evicts the slow client immediately so neither the hub nor the Kafka-to-Postgres loop ever blocks.
- WebSocket upgrades enforce a local loopback origin check (`localhost`, `127.0.0.1`, `::1`) by default, configurable via `ALLOWED_ORIGINS`.

### 2. Idempotent Delivery & Bounded Retry Buffer
- Kafka offsets are never auto-committed before persistence. `batchBuffer` in `pipeline/main.go` buffers validated `bufferedRecord` items (`event` + raw `kafka.Message`) and flushes at `batchSize = 50` or `batchInterval = 1s`.
- `Store.WriteBatch` executes all inserts via `pgx.Batch` (`pool.SendBatch`) using `INSERT ... ON CONFLICT (device_id, timestamp) DO NOTHING` backed by a unique index on `(device_id, timestamp)`, ensuring at-least-once Kafka redeliveries are strictly idempotent and never create duplicate rows.
- `reader.CommitMessages` runs only after PostgreSQL confirms the batch. If PostgreSQL becomes unreachable, `batchBuffer` switches pipeline health metrics from `HEALTHY` to `DEGRADED`, applies exponential backoff (`1s` up to `30s`), and caps memory at `maxBufferSize = 500` records by dropping the oldest buffered batch with a warning rather than growing unboundedly. On `SIGINT`/`SIGTERM`, pending buffered records are flushed before exit.

### 3. Zero-Build Industrial Dispatch Control Board
- Served directly by the Go binary from `web/index.html` using HTML5, Tailwind CDN, and Vanilla JS (no bundler or Node build step).
- **100m × 80m Spatial Canvas**: HiDPI-scaled floor grid rendering storage aisles (`AISLE A`–`D`), charging pads (`CHG-1`–`CHG-3`), and loading docks (`DOCK-W/N/S/E`) matching the simulator's coordinate space, with 60fps position interpolation, heading vectors, and motion trails.
- **Health Color-Coding**:
  - **Green**: Normal operation (`MOVING`, `IDLE`, `LOADING`, `UNLOADING`, battery ≥ 25%)
  - **Amber**: Battery `< 25%` or `CHARGING`
  - **Red**: Battery `< 15%` or `ERROR` / `STALLED`
- **Vehicle Inspector & Alert Feed**: Clicking any AGV on the canvas or in the 5×2 fleet roster displays live battery percentage, speed (`m/s`), `(x, y)` coordinates, and live ping age. Faults (`ERROR`/`STALLED`) and low-battery events (`< 20%`) populate a throttled Recent Alerts feed.

---

## Telemetry Data Contract

### Simulator JSON Event (`agv-telemetry`)

Emitted by `simulator/producer.py` at 20 events/sec across `AGV-001` through `AGV-010` (`device_id` used as the Kafka message key) and validated by `parseTelemetryEvent` in `pipeline/types.go`:

```json
{
  "device_id": "AGV-004",
  "battery_level": 78.42,
  "x_coord": 44.812,
  "y_coord": 23.195,
  "speed": 1.68,
  "status": "MOVING",
  "timestamp": "2026-11-11T20:15:02.418291+00:00"
}
```

Valid `status` values generated by the simulator: `MOVING`, `CHARGING`, `IDLE`, `LOADING`, `UNLOADING`, `ERROR`.

### PostgreSQL Schema (`agv_telemetry`)

Automatically verified on startup by `ensureSchema` in `pipeline/store.go`, with idempotent writes via `ON CONFLICT (device_id, timestamp) DO NOTHING`:

```sql
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
```

---

## Quick Start & Verification

### 1. Start Infrastructure (Redpanda, Redpanda Console, PostgreSQL 16)

```powershell
docker compose up -d
docker compose ps
```

### 2. Start the Go Consumer & WebSocket Server

```powershell
cd pipeline
go run .
```

Default configuration (overridable via `POSTGRES_DSN`, `KAFKA_BROKERS`, `KAFKA_TOPIC`, `KAFKA_GROUP_ID`, `HTTP_ADDR`, `ALLOWED_ORIGINS`, `WEB_DIR`):
- Kafka broker: `localhost:19092` (`agv-telemetry`, group `agv-indexer`)
- PostgreSQL DSN: `postgres://agv_admin:agv_password@127.0.0.1:5433/warehouse_telemetry?sslmode=disable`
- HTTP / WebSocket server: `:8081` (serving `../web`, WebSocket origin restricted to `localhost`/`127.0.0.1` unless `ALLOWED_ORIGINS` is set)

### 3. Run Unit Tests & Static Checks

```powershell
cd pipeline
go vet ./...
go test -v -race ./...
```

### 4. Start the Python Fleet Simulator

In a separate terminal from the project root:

```powershell
pip install -r simulator/requirements.txt
python simulator/producer.py
```

### 5. Open Dashboards

- **Live AGV Dispatch UI**: http://localhost:8081
- **Redpanda Console**: http://localhost:8080

### 6. Verify Indexed Telemetry in PostgreSQL

```powershell
docker exec -it agv-postgres psql -U agv_admin -d warehouse_telemetry -c "SELECT COUNT(*) AS total_rows, COUNT(DISTINCT device_id) AS active_agvs FROM agv_telemetry;"
```

Inspect the latest telemetry per vehicle:

```powershell
docker exec -it agv-postgres psql -U agv_admin -d warehouse_telemetry -c "SELECT DISTINCT ON (device_id) device_id, battery_level, x_coord, y_coord, speed, status, timestamp FROM agv_telemetry ORDER BY device_id, timestamp DESC;"
```

---

## Architecture Tradeoffs & Production Next Steps

- **Idempotency (`(device_id, timestamp)` vs. synthetic UUIDs)**: Deduplicating on `(device_id, timestamp)` with `ON CONFLICT DO NOTHING` avoids schema bloat and stops duplicate rows on Kafka redelivery, assuming a vehicle emits at most one event per microsecond timestamp. In production, adding an explicit producer-generated `event_id` UUID (or TimescaleDB hypertable partitioning by time) handles sub-microsecond bursts and clock skew cleanly.
- **Failure Handling (bounded memory buffer vs. DLQ)**: When PostgreSQL is unreachable, `batchBuffer` caps memory at `500` events with exponential backoff and evicts the oldest batch rather than crashing from OOM. A production deployment would route unrecoverable payloads and prolonged outage overflow to a dedicated Kafka dead-letter queue (`agv-telemetry-dlq`) and expose Prometheus counters for dropped batches.
- **Demo Security Defaults**: `docker-compose.yml` and default DSN constants use fixed local credentials (`agv_admin` / `agv_password`) for zero-config local startup, and `Hub` defaults to allowing loopback origins (`localhost`/`127.0.0.1`). Production environments should inject secrets via a secret manager, enable TLS/SASL on Kafka and Postgres, and set `ALLOWED_ORIGINS` explicitly.
- **Horizontal Scaling**: `simulator/producer.py` keys every message by `device_id`, preserving per-AGV ordering within a partition. Scaling `agv-indexer` horizontally only requires increasing partitions on `agv-telemetry` and running multiple Go consumer instances under the same consumer group—with a shared pub/sub layer (e.g., Redis) if multiple WebSocket pods need a unified fleet view.

---

## Repository Structure

```text
agv-telemetry-pipeline/
├── .github/
│   └── workflows/
│       └── ci.yml            # GitHub Actions CI (Go vet/build/test -race + Python compile check)
├── docs/
│   └── demo-small.mp4        # Compressed dashboard walkthrough clip
├── pipeline/
│   ├── go.mod
│   ├── go.sum
│   ├── hub.go                # Origin-checked WebSocket hub and non-blocking fan-out
│   ├── main.go               # Kafka consumer loop, bounded retry buffer, graceful shutdown
│   ├── pipeline_test.go      # Unit tests for parsing, hub backpressure, and bounded buffer
│   ├── store.go              # PostgreSQL pool, schema migration, idempotent pgx batch writes
│   └── types.go              # TelemetryEvent validation, rounding, metrics, WS envelopes
├── simulator/
│   ├── producer.py           # 10-AGV kinematic & battery simulator (20 events/sec)
│   └── requirements.txt      # kafka-python
├── web/
│   └── index.html            # Zero-build 2D warehouse dispatch dashboard
├── docker-compose.yml        # Redpanda, Redpanda Console, and PostgreSQL 16
├── LICENSE                   # MIT License
└── README.md
```
