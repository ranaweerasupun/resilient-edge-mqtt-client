# Production MQTT Client for Edge Devices

An MQTT client library built for edge devices — Raspberry Pi, industrial gateways, field sensors — running in environments where network connectivity cannot be taken for granted. The design goal is zero message loss: messages are never silently dropped, whether the broker is reachable or not.

---

## Overview

Standard MQTT clients assume the network is up. On an edge device that assumption breaks regularly — cellular links drop, Wi-Fi roams, VPNs time out. This library treats disconnection as a normal operating state rather than an exceptional one.

When the broker is unreachable, outgoing messages are written to a local SQLite-backed queue and held there until connectivity returns. Messages that were already sent but not yet acknowledged are tracked separately so they can be re-sent after reconnection, closing the gap that most client libraries leave open.

---

## Architecture

```
Your Application
      │
      ▼
ProductionMQTTClient  (production_client.py)
      ├── InflightTracker  (inflight_tracker.py)   — sent-but-unacknowledged messages
      ├── OfflineQueue     (offline_queue.py)       — messages queued during disconnection
      └── Config           (config.py)              — configuration from file/env/args

ProductionLogger     (production_logger.py)         — rotating file logs + structured metrics
```

---

## Features

**Reliable delivery** — QoS 1 and QoS 2 messages that haven't been acknowledged are held in the inflight tracker. On reconnect, they are re-sent automatically before the offline queue starts draining, so ordering and delivery guarantees are preserved.

**Offline queuing** — Messages published while disconnected are written to SQLite and replayed in the background once the broker is reachable again. The queue drainer runs as a daemon thread and requires no application-level intervention.

**Priority-based eviction** — Each message carries a priority from 1 to 10. When the queue reaches capacity, lower-priority messages are evicted first. Critical alerts can displace routine telemetry rather than being dropped because the queue was full of older, less important data.

**Exponential backoff** — Reconnection intervals double on each failed attempt, bounded by a configurable minimum and maximum. This prevents large fleets from hammering a broker that's just come back online.

**Thread-safe storage** — All SQLite operations in both the inflight tracker and the offline queue are guarded by threading locks. The MQTT network thread and the application thread can both touch the database without coordination at the call site.

**Structured logging** — `production_logger.py` wraps Python's standard `logging` with rotating file handlers, a structured key-value context format, and a separate `.jsonl` metrics file suitable for ingestion by Prometheus, Loki, or similar tools.

**Flexible configuration** — `config.py` loads from a JSON file, a simple `key=value` file, environment variables, or a plain dictionary. All values are validated at load time with clear error messages.

---

## File Reference

**`production_client.py`** — The main entry point. Instantiate `ProductionMQTTClient`, call `.connect()` and `.start()`, then use `.publish()` for all outgoing messages. The client decides internally whether to publish directly or queue offline based on current connection state.

**`inflight_tracker.py`** — SQLite-backed store for messages that have been handed to the broker but not yet acknowledged. Used internally by `ProductionMQTTClient`; no direct interaction required.

**`offline_queue.py`** — SQLite-backed queue for messages waiting to be sent. Handles priority ordering, batch retrieval, and capacity management. Also used internally by `ProductionMQTTClient`.

**`config.py`** — The `Config` class. Load with `Config.from_file("config.json")`, `Config.from_env()`, or `Config({...})`. All supported fields and their defaults are listed in `Config.DEFAULTS`.

**`production_logger.py`** — The `ProductionLogger` class and `get_logger()` factory. Call `get_logger("my_app")` from any module to get the shared singleton instance.

**`test_13.py`** — End-to-end simulation that publishes temperature readings every 5 seconds and prints queue statistics every 10 readings. Designed specifically to demonstrate offline behaviour: run it, stop the broker, watch messages accumulate, restart the broker, watch the queue drain.

---

## Installation

```bash
pip install -r requirements.txt
```

SQLite3 is part of the Python standard library and needs no separate installation.

---

## Quick Start

```python
from production_client import ProductionMQTTClient
import json

client = ProductionMQTTClient(
    client_id="my_edge_device_001",
    broker_host="localhost",
    broker_port=1883,
    max_queue_size=1000
)

client.connect()
client.start()

# Works whether connected or offline — routing is handled internally
client.publish(
    topic="sensors/temperature",
    payload=json.dumps({"value": 23.5, "unit": "C"}),
    qos=1,
    priority=5
)

stats = client.get_statistics()
print(stats)
```

---

## Configuration

Copy the provided template and fill in your values:

```bash
cp config.template.json config.json
```

The file is gitignored so credentials stay out of version control. All fields have sensible defaults except `client_id`, which must be set explicitly.

| Field | Default | Description |
|---|---|---|
| `client_id` | *(required)* | Unique identifier for this MQTT client |
| `broker_host` | `localhost` | MQTT broker hostname or IP address |
| `broker_port` | `1883` | MQTT broker port |
| `max_queue_size` | `1000` | Maximum offline queue depth |
| `min_backoff` | `1` | Minimum reconnection wait in seconds |
| `max_backoff` | `60` | Maximum reconnection wait in seconds |
| `log_dir` | `./logs` | Directory for log files |
| `log_level` | `INFO` | One of: DEBUG, INFO, WARNING, ERROR, CRITICAL |
| `db_path` | `./mqtt_client.db` | Path for the SQLite database files |

Configuration can also be loaded from environment variables prefixed with `MQTT_`:

```bash
export MQTT_BROKER_HOST=mqtt.example.com
export MQTT_CLIENT_ID=device_001
```

Then call `Config.from_env()`.

---

## Logging

```python
from production_logger import get_logger

logger = get_logger("my_app")

logger.info("Sensor started", device_id="sensor_01", location="warehouse_A")
logger.warning("Queue is nearly full", queue_size=950, max_size=1000)
logger.error("Publish failed", topic="sensors/temp", error=str(e))

# Numeric metric — written as a JSON line for monitoring tools
logger.log_metric("messages_published", 150, topic="sensors/temp", qos=1)

# Structured event
logger.log_event("connection_established", broker="localhost", port=1883)
```

Log files rotate at `log_max_bytes` (default 10 MB) and keep up to `log_backup_count` (default 5) backups. Metrics land in a separate `.jsonl` file alongside the main log.

---

## Testing the Offline Queue

The quickest way to see the full system in action is against a local Mosquitto broker:

```bash
# Terminal 1 — run the simulation
python test_13.py

# Terminal 2 — simulate a network outage
sudo systemctl stop mosquitto

# Watch messages queue up in Terminal 1, then restore connectivity
sudo systemctl start mosquitto

# Watch the offline queue drain automatically
```

---

## Project Structure

```
.
├── production_client.py      # Main MQTT client with offline + inflight support
├── production_logger.py      # Rotating file logger and metrics writer
├── config.py                 # Configuration loader and validator
├── inflight_tracker.py       # SQLite-backed inflight message tracker
├── offline_queue.py          # SQLite-backed offline message queue
├── test_13.py                # End-to-end simulation script
├── config.template.json      # Configuration template (copy to config.json)
├── requirements.txt          # Python dependencies
├── README.md
└── LICENSE
```

---

## Requirements

- Python 3.7 or later
- `paho-mqtt`
- An MQTT broker (e.g., [Eclipse Mosquitto](https://mosquitto.org/))
- SQLite3 (included in Python standard library)

---

## License

MIT License. See `LICENSE` for details.
