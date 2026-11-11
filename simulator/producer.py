"""AGV Telemetry Simulator producing realistic warehouse AGV events to Kafka/Redpanda."""

import json
import math
import os
import random
import signal
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone

from kafka import KafkaProducer
from kafka.errors import KafkaError

BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:19092")
TOPIC = os.getenv("KAFKA_TOPIC", "agv-telemetry")
EVENTS_PER_SEC = float(os.getenv("EVENTS_PER_SEC", "20"))
NUM_AGVS = int(os.getenv("NUM_AGVS", "10"))

WAREHOUSE_WIDTH = 100.0
WAREHOUSE_HEIGHT = 80.0
CHARGING_STATIONS = [(5.0, 5.0), (5.0, 75.0), (95.0, 5.0)]
DOCK_STATIONS = [(20.0, 40.0), (50.0, 20.0), (50.0, 60.0), (80.0, 40.0)]


@dataclass
class AGVState:
    """Maintains kinematic, battery, and operational state for a single AGV."""

    device_id: str
    battery_level: float
    x_coord: float
    y_coord: float
    heading_rad: float
    speed: float
    status: str
    target_x: float
    target_y: float
    state_ticks_remaining: int

    def step(self, dt: float) -> dict:
        """Advance simulation by dt seconds and return a telemetry payload."""
        self.state_ticks_remaining -= 1

        if self.status == "CHARGING":
            self.speed = 0.0
            self.battery_level = min(100.0, self.battery_level + 1.5 * dt)
            if self.battery_level >= 95.0 or self.state_ticks_remaining <= 0:
                self._assign_new_task()

        elif self.status in ("IDLE", "LOADING", "UNLOADING"):
            self.speed = 0.0
            self.battery_level = max(0.0, self.battery_level - 0.02 * dt)
            if self.state_ticks_remaining <= 0:
                self._assign_new_task()

        elif self.status == "ERROR":
            self.speed = 0.0
            if self.state_ticks_remaining <= 0:
                self._assign_new_task()

        else:  # MOVING
            dx = self.target_x - self.x_coord
            dy = self.target_y - self.y_coord
            dist = math.hypot(dx, dy)

            if dist < 1.0:
                self._on_arrival()
            else:
                self.heading_rad = math.atan2(dy, dx)
                target_speed = random.uniform(1.2, 2.2)
                self.speed = round(0.7 * self.speed + 0.3 * target_speed, 2)
                step_dist = min(dist, self.speed * dt)
                self.x_coord += math.cos(self.heading_rad) * step_dist
                self.y_coord += math.sin(self.heading_rad) * step_dist
                self.x_coord = max(0.0, min(WAREHOUSE_WIDTH, self.x_coord))
                self.y_coord = max(0.0, min(WAREHOUSE_HEIGHT, self.y_coord))
                self.battery_level = max(0.0, self.battery_level - (0.08 + 0.04 * self.speed) * dt)

                if self.battery_level < 15.0:
                    self._head_to_charger()
                elif random.random() < 0.002:
                    self.status = "ERROR"
                    self.speed = 0.0
                    self.state_ticks_remaining = random.randint(10, 30)

        return {
            "device_id": self.device_id,
            "battery_level": round(self.battery_level, 2),
            "x_coord": round(self.x_coord, 3),
            "y_coord": round(self.y_coord, 3),
            "speed": round(self.speed, 2),
            "status": self.status,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }

    def _head_to_charger(self) -> None:
        nearest = min(
            CHARGING_STATIONS,
            key=lambda c: math.hypot(c[0] - self.x_coord, c[1] - self.y_coord),
        )
        self.target_x, self.target_y = nearest
        self.status = "MOVING"
        self.state_ticks_remaining = 200

    def _on_arrival(self) -> None:
        self.speed = 0.0
        at_charger = any(
            math.hypot(cx - self.x_coord, cy - self.y_coord) < 1.5
            for cx, cy in CHARGING_STATIONS
        )
        if at_charger and self.battery_level < 80.0:
            self.status = "CHARGING"
            self.state_ticks_remaining = random.randint(30, 80)
        else:
            self.status = random.choice(["LOADING", "UNLOADING", "IDLE"])
            self.state_ticks_remaining = random.randint(5, 20)

    def _assign_new_task(self) -> None:
        if self.battery_level < 20.0:
            self._head_to_charger()
            return

        if random.random() < 0.6:
            tx, ty = random.choice(DOCK_STATIONS)
            # Small jitter around the dock bay
            self.target_x = max(0.0, min(WAREHOUSE_WIDTH, tx + random.uniform(-3.0, 3.0)))
            self.target_y = max(0.0, min(WAREHOUSE_HEIGHT, ty + random.uniform(-3.0, 3.0)))
        else:
            self.target_x = random.uniform(5.0, WAREHOUSE_WIDTH - 5.0)
            self.target_y = random.uniform(5.0, WAREHOUSE_HEIGHT - 5.0)

        self.status = "MOVING"
        self.speed = random.uniform(0.8, 1.5)
        self.state_ticks_remaining = random.randint(40, 150)


def create_fleet(num_agvs: int) -> list[AGVState]:
    """Initialize a fleet of AGVs across the warehouse."""
    fleet = []
    for i in range(1, num_agvs + 1):
        x = random.uniform(10.0, WAREHOUSE_WIDTH - 10.0)
        y = random.uniform(10.0, WAREHOUSE_HEIGHT - 10.0)
        tx, ty = random.choice(DOCK_STATIONS)
        fleet.append(
            AGVState(
                device_id=f"AGV-{i:03d}",
                battery_level=random.uniform(35.0, 100.0),
                x_coord=x,
                y_coord=y,
                heading_rad=random.uniform(0.0, 2 * math.pi),
                speed=random.uniform(0.8, 1.8),
                status="MOVING",
                target_x=tx,
                target_y=ty,
                state_ticks_remaining=random.randint(20, 100),
            )
        )
    return fleet


def build_producer(bootstrap_servers: str) -> KafkaProducer:
    """Create a JSON-serializing KafkaProducer."""
    return KafkaProducer(
        bootstrap_servers=bootstrap_servers.split(","),
        key_serializer=lambda k: k.encode("utf-8"),
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        acks="all",
        linger_ms=10,
    )


def main() -> None:
    running = True

    def handle_signal(signum, _frame):
        nonlocal running
        print(f"\nReceived signal {signum}, shutting down simulator...")
        running = False

    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    print(
        f"Starting AGV simulator -> brokers={BOOTSTRAP_SERVERS}, "
        f"topic={TOPIC}, rate={EVENTS_PER_SEC} events/sec, fleet={NUM_AGVS} AGVs"
    )

    producer = build_producer(BOOTSTRAP_SERVERS)
    fleet = create_fleet(NUM_AGVS)
    interval = 1.0 / EVENTS_PER_SEC
    # Each AGV is sampled roughly once every (NUM_AGVS / EVENTS_PER_SEC) seconds
    agv_dt = NUM_AGVS / EVENTS_PER_SEC
    agv_idx = 0
    sent_count = 0
    last_report = time.monotonic()

    try:
        while running:
            tick_start = time.monotonic()
            agv = fleet[agv_idx]
            agv_idx = (agv_idx + 1) % len(fleet)

            event = agv.step(agv_dt)
            producer.send(TOPIC, key=event["device_id"], value=event)
            sent_count += 1

            now = time.monotonic()
            if now - last_report >= 5.0:
                rate = sent_count / (now - last_report)
                print(
                    f"Sent {sent_count} events in {now - last_report:.1f}s "
                    f"({rate:.1f} events/sec) | sample={event}"
                )
                sent_count = 0
                last_report = now

            elapsed = time.monotonic() - tick_start
            sleep_time = interval - elapsed
            if sleep_time > 0:
                time.sleep(sleep_time)
    except KafkaError as exc:
        print(f"Kafka error occurred: {exc}", file=sys.stderr)
        sys.exit(1)
    finally:
        producer.flush(timeout=5)
        producer.close()
        print("Producer closed cleanly.")


if __name__ == "__main__":
    main()
