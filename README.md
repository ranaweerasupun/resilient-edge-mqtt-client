# Production MQTT Client for Edge Devices

A production-ready MQTT client library designed for edge devices (such as Raspberry Pi) that operate in environments with unreliable network connectivity. The system ensures **zero message loss** by combining offline queuing, inflight message tracking, and automatic reconnection with exponential backoff.

---

## Overview

Traditional MQTT clients assume a stable network connection. On an edge device — a sensor in a warehouse, a field unit, or an IoT gateway — connectivity can drop at any time. This library solves that problem by treating disconnection as a normal operating condition rather than an error.

When the broker is unreachable, messages are persisted locally in an SQLite-backed offline queue. When connectivity returns, the queue drains automatically in the background. Messages that were already sent but not yet acknowledged (inflight messages) are tracked separately and re-sent after reconnection to guarantee delivery.

---

## Architecture

```
Your Application
      │
      ▼
ProductionMQTTClient  (production_client.py)
      ├── InflightTracker  (inflight_tracker.py)   — tracks sent-but-unacknowledged messages
      ├── OfflineQueue     (offline_queue.py)       — persists messages during disconnection
      └── Config           (config.py)              — manages configuration from file/env/args

ProductionLogger     (production_logger.py)         — rotating file logs + structured metrics
```

---

## Features

**Reliable message delivery** — QoS 1 and QoS 2 messages that are sent but not yet acknowledged are stored in the inflight tracker. After a reconnection, they are automatically re-sent so no message is silently lost.

**Offline queuing** — When the device has no broker connection, published messages are stored in a local SQLite database. When connectivity is restored, a background thread drains the queue automatically in configurable batch sizes.

**Priority-based queue management** — Each message can be assigned a priority (1–10). When the offline queue reaches capacity, lower-priority messages are evicted first to make room for more important ones (such as critical sensor alerts).

**Exponential backoff reconnection** — After a disconnect, the client waits before reconnecting and doubles the wait time on each failed attempt (between a configurable minimum and maximum), preventing thundering-herd problems when many devices reconnect at once.

**Thread-safe design** — All SQLite operations in both the inflight tracker and offline queue are protected by threading locks, allowing the MQTT background thread and the main application thread to operate safely on the same database connections.

**Production logging** — `production_logger.py` provides a `ProductionLogger` with rotating file handlers (configurable max size and backup count), multiple log levels, structured JSON metrics output for monitoring tools, and a global singleton so all modules share one logger instance.

**Flexible configuration** — `config.py` supports loading configuration from a JSON file, a simple `key=value` file, environment variables (with a configurable prefix), or programmatic dictionaries. All values are validated on load.

---

## File Reference

**`production_client.py`** is the main entry point. Instantiate `ProductionMQTTClient` with a client ID and broker details, then call `.connect()` and `.start()`. Use `.publish()` to send messages — the client automatically routes to direct publish or offline queue based on connection state.

**`inflight_tracker.py`** manages an SQLite table of messages that have been sent to the broker but not yet acknowledged. It is used internally by `ProductionMQTTClient` and does not need to be used directly.

**`offline_queue.py`** manages an SQLite table of messages waiting to be sent. It supports priority-based insertion, batch retrieval, and smart eviction when the queue is full. Also used internally by `ProductionMQTTClient`.

**`config.py`** provides the `Config` class. Load configuration with `Config.from_file("config.json")`, `Config.from_env()`, or `Config({"client_id": "...", ...})`. A full list of supported fields and their defaults is defined in `Config.DEFAULTS`.

**`production_logger.py`** provides the `ProductionLogger` class and a `get_logger()` factory function. Import and call `get_logger("my_app")` to get a shared logger instance across all modules.

**`test_13.py`** is a simulation script that creates a `ProductionMQTTClient`, publishes simulated temperature sensor readings every 5 seconds, and prints statistics every 10 readings. It is intended to demonstrate offline queueing — run it, stop the MQTT broker, observe messages queuing up, then restart the broker and watch the queue drain.

---

## Installation

Install the required dependencies:

```bash
pip install -r requirements.txt
```

SQLite3 is included in the Python standard library and requires no additional installation.

---

## Quick Start

```python
from production_client import ProductionMQTTClient
import json

# Create the client
client = ProductionMQTTClient(
    client_id="my_edge_device_001",
    broker_host="localhost",
    broker_port=1883,
    max_queue_size=1000
)

# Connect and start the network loop
client.connect()
client.start()

# Publish a message — works whether connected or offline
client.publish(
    topic="sensors/temperature",
    payload=json.dumps({"value": 23.5, "unit": "C"}),
    qos=1,
    priority=5
)

# Check health at any time
stats = client.get_statistics()
print(stats)
```

---

## Configuration

Copy the provided template and fill in your values:

```bash
cp config.template.json config.json
```

Then edit `config.json` with your broker details. The file is gitignored so your credentials stay private. Key fields are described below — all have sensible defaults except `client_id`, which must be provided.

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

You can also load configuration from environment variables. Set variables prefixed with `MQTT_` and call `Config.from_env()`:

```bash
export MQTT_BROKER_HOST=mqtt.example.com
export MQTT_CLIENT_ID=device_001
```

---

## Logging

Use the production logger in your own modules:

```python
from production_logger import get_logger

logger = get_logger("my_app")

logger.info("Sensor started", device_id="sensor_01", location="warehouse_A")
logger.warning("Queue is nearly full", queue_size=950, max_size=1000)
logger.error("Publish failed", topic="sensors/temp", error=str(e))

# Log a numeric metric — written as JSON lines for monitoring tools
logger.log_metric("messages_published", 150, topic="sensors/temp", qos=1)

# Log a structured event
logger.log_event("connection_established", broker="localhost", port=1883)
```

Log files are written to the configured `log_dir` and rotate automatically when they reach `log_max_bytes` (default 10 MB), keeping up to `log_backup_count` (default 5) backup files. Metrics are written separately to a `.jsonl` file for easy ingestion by monitoring tools.

---

## Testing the Offline Queue

The best way to see the system working end-to-end is to run `test_13.py` against a local Mosquitto broker:

```bash
# Terminal 1 — run the simulation
python test_13.py

# Terminal 2 — stop the broker to simulate a network outage
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
- `paho-mqtt` library
- An MQTT broker (e.g., [Eclipse Mosquitto](https://mosquitto.org/))
- SQLite3 (included in Python standard library)

---

## License

MIT License. See `LICENSE` for details.
