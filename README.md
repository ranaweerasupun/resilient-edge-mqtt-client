# Production MQTT Client for Edge Devices

[![Version](https://img.shields.io/badge/version-1.0.0-blue.svg)](CHANGELOG.md)
[![Python](https://img.shields.io/badge/python-3.7%2B-blue.svg)](pyproject.toml)
[![License: MIT](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)

An MQTT client built for devices that live in the real world — Raspberry Pis, industrial gateways, field sensors — where the network is unreliable and dropping a message silently is not acceptable.

---

## The Reason Behind for Designing This ...

Standard MQTT clients assume the network is up. On an edge device, that assumption breaks regularly. Cellular links drop, Wi-Fi roams, VPNs time out, brokers restart. Most client libraries handle this by... not handling it. They hand the problem back to your application code and wish you luck.

This library treats disconnection as a normal operating condition rather than an edge case. When the broker is unreachable, outgoing messages are written to a local SQLite queue and held there until the connection returns. Messages that were already sent but not yet acknowledged are tracked separately so they can be re-sent after reconnection. And as of v0.6.0, the client is fully bidirectional — you can subscribe to topics and receive messages just as reliably as you can publish them.As of v0.7.0 it exposes an HTTP health check endpoint so external systems can observe its state without having to inspect log files or SSH into the device.

---

## Code Structure

```
Your Application
      │
      ▼
ProductionMQTTClient  (production_client.py)
      ├── InflightTracker  (inflight_tracker.py)   — sent-but-unacknowledged messages
      ├── OfflineQueue     (offline_queue.py)       — messages queued during disconnection
      └── Config           (config.py)              — configuration from file/env/args

ProductionLogger     (production_logger.py)         — rotating file logs + structured metrics
HTTP Health Check    (built into production_client)  — GET /health over a background thread
```

The client is the only thing your application talks to directly. `InflightTracker` and `OfflineQueue` are internal — the client decides which one to use based on the current connection state. You don't have to think about either of them.

---

## Working Mechanism

**Zero message loss** — Messages published while offline are written to SQLite and replayed automatically once the broker is reachable again. QoS 1/2 messages that were acknowledged by your app but not yet confirmed by the broker are tracked and re-sent after reconnection.

**TLS encryption** — Set `use_tls=True` and provide a CA certificate to encrypt all traffic between the device and the broker. Mutual TLS (where the broker also verifies the device's identity using a client certificate) is supported for brokers that require it, such as AWS IoT Core.

**Authentication** — Username and password credentials are passed in the MQTT CONNECT packet. Works alongside TLS so credentials are always encrypted in transit.

**Priority-based eviction** — Each message carries a priority from 1 to 10. When the queue fills up, lower-priority messages are dropped first. A critical alert can displace routine telemetry rather than being blocked behind a full queue of sensor readings.

**Exponential backoff** — Reconnection intervals double on each failed attempt, up to a configurable ceiling. This matters in fleet deployments — you don't want hundreds of devices hammering a broker the moment it comes back online.

**Thread-safe storage** — All SQLite operations are guarded by locks. The MQTT network thread and the application thread can both interact with the database without you having to coordinate anything at the call site.

**Structured logging** — `production_logger.py` wraps Python's standard logging with rotating file handlers, a structured key-value format, and a separate `.jsonl` metrics file that works well with Prometheus, Loki, or similar tools.

**Flexible configuration** — `config.py` can load from a JSON file, a simple `key=value` file, environment variables, or a plain dictionary. Everything is validated at load time with clear error messages.

**Subscribe support** — Register callbacks for incoming messages using full MQTT wildcard syntax. Subscriptions survive reconnections automatically.

**Health check endpoint** — When `enable_health_check=True`, a minimal HTTP server runs on a background thread and exposes `GET /health`. The response body is a JSON snapshot of the client's current state. The HTTP status code is 200 when the client is healthy or degraded, and 503 when it is not connected to the broker.

---

## Installation and Usage

```bash
pip install -r requirements.txt

# With development/testing dependencies
pip install ".[dev]"

# Traditional approach (still works)
pip install -r requirements.txt
```


SQLite3 is part of the Python standard library, so no separate install needed.

---

### Basic usage with health check enabled

```python
from config import Config
from production_client import ProductionMQTTClient

config = Config({
    "client_id":            "my_device_001",
    "broker_host":          "localhost",
    "broker_port":          1883,
    "enable_health_check":  True,
    "health_check_port":    8080,
})

client = ProductionMQTTClient.from_config(config)
client.connect()
client.start()

# Health check is now available at http://localhost:8080/health
```

### Querying the health check

```bash
curl http://localhost:8080/health
```

A healthy response looks like this:

```json
{
  "status": "healthy",
  "client_id": "my_device_001",
  "statistics": {
    "connected": true,
    "current_backoff_seconds": 1,
    "offline_queue": {
      "total_messages": 0,
      "by_priority": {},
      "oldest_message_age_seconds": null,
      "capacity_used_percent": 0.0
    },
    "inflight_messages": 0,
    "tls_enabled": false,
    "active_subscriptions": 2
  }
}
```

A degraded response (connected but queue under pressure) returns HTTP 200 with `"status": "degraded"`. An unhealthy response (not connected) returns HTTP 503 with `"status": "unhealthy"`.

### Docker HEALTHCHECK

```dockerfile
HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
  CMD curl -f http://localhost:8080/health || exit 1
```

### Kubernetes liveness probe

```yaml
livenessProbe:
  httpGet:
    path: /health
    port: 8080
  initialDelaySeconds: 10
  periodSeconds: 30
  failureThreshold: 3
```


### Publishing only (no subscriptions)

```python
from config import Config
from production_client import ProductionMQTTClient

config = Config.from_file("config.json")
client = ProductionMQTTClient.from_config(config)
client.connect()
client.start()

client.publish(
    topic="sensors/temperature",
    payload='{"value": 23.5, "unit": "C"}',
    qos=1,
    priority=5,
)
```

### Publishing and subscribing

```python
from config import Config
from production_client import ProductionMQTTClient

config = Config.from_file("config.json")
client = ProductionMQTTClient.from_config(config)

# Register callbacks before connecting.
# Subscriptions registered offline are stored and sent to the broker
# automatically on the first successful connection.

def on_command(topic, payload, qos, retain):
    print(f"Command received: {payload.decode()}")

def on_config_update(topic, payload, qos, retain):
    print(f"Config update on {topic}: {payload.decode()}")

client.subscribe("devices/my_device/commands", on_command, qos=1)
client.subscribe("devices/my_device/config/#", on_config_update, qos=1)

client.connect()
client.start()
```

### Wildcard subscriptions

```python
# '+' matches exactly one level
client.subscribe("sensors/+/temperature", on_temperature)
# Receives: sensors/room1/temperature, sensors/room2/temperature
# Does NOT receive: sensors/room1/floor2/temperature

# '#' matches zero or more levels — must be the last character
client.subscribe("devices/#", on_any_device_message)
# Receives: devices/001, devices/001/status, devices/001/sensors/temp
```

### Without TLS (development / local broker)

```python
from config import Config
from production_client import ProductionMQTTClient

config = Config({
    "client_id":   "my_device_001",
    "broker_host": "localhost",
    "broker_port": 1883,
})

client = ProductionMQTTClient.from_config(config)
client.connect()
client.start()

client.publish(
    topic="sensors/temperature",
    payload='{"value": 23.5, "unit": "C"}',
    qos=1,
    priority=5,
)
```

### With TLS (production broker)

```python
config = Config({
    "client_id":   "my_device_001",
    "broker_host": "mqtt.example.com",
    "broker_port": 8883,         # Standard TLS port
    "use_tls":     True,
    "ca_certs":    "/etc/ssl/certs/broker-ca.pem",
    "username":    "device_001",
    "password":    "secret",
})

client = ProductionMQTTClient.from_config(config)
client.connect()
client.start()
```

### With mutual TLS (AWS IoT Core, mTLS brokers)

```python
config = Config({
    "client_id":   "my_device_001",
    "broker_host": "xxxxxxxxxxxx.iot.us-east-1.amazonaws.com",
    "broker_port": 8883,
    "use_tls":     True,
    "ca_certs":    "/certs/AmazonRootCA1.pem",
    "certfile":    "/certs/device-certificate.pem.crt",
    "keyfile":     "/certs/device-private.pem.key",
})

client = ProductionMQTTClient.from_config(config)
client.connect()
client.start()
```


---

## Configuration

Copy the template and fill in your values:

```bash
cp config.template.json config.json
```

It is better to gitignore the file so credentials stay out of version control. All fields have sensible defaults except `client_id`, which must be set explicitly because there's no safe default for a value that has to be unique across your entire fleet.

| Field | Default | Description |
|---|---|---|
| `client_id` | *(required)* | Unique identifier for this device |
| `broker_host` | `localhost` | MQTT broker hostname or IP |
| `broker_port` | `1883` | MQTT broker port (use `8883` for TLS) |
| `use_tls` | `false` | Enable TLS encryption |
| `ca_certs` | `null` | Path to CA certificate file |
| `certfile` | `null` | Path to client certificate (mutual TLS only) |
| `keyfile` | `null` | Path to client private key (mutual TLS only) |
| `username` | `null` | MQTT username |
| `password` | `null` | MQTT password |
| `max_queue_size` | `1000` | Maximum offline queue depth |
| `min_backoff` | `1` | Minimum reconnection wait in seconds |
| `max_backoff` | `60` | Maximum reconnection wait in seconds |
| `log_dir` | `./logs` | Directory for log files |
| `log_level` | `INFO` | One of: DEBUG, INFO, WARNING, ERROR, CRITICAL |
| `db_path` | `./mqtt_client.db` | Path for the shared SQLite database |
| `enable_health_check` | `false` | Enable the HTTP health check endpoint |
| `health_check_port` | `8080` | Port for the health check endpoint |

Configuration can also be loaded from environment variables prefixed with `MQTT_`:

```bash
export MQTT_BROKER_HOST=mqtt.example.com
export MQTT_CLIENT_ID=device_001
export MQTT_USE_TLS=true
export MQTT_CA_CERTS=/etc/ssl/certs/broker-ca.pem
```

Then call `Config.from_env()` instead of `Config.from_file()`.

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

## Testing the offline queue

The quickest way to see the system in action is against a local Mosquitto broker:

```bash
# Terminal 1 — run the simulation
python test_13.py

# Terminal 2 — simulate an outage
sudo systemctl stop mosquitto

# Watch messages queue up in Terminal 1, then bring it back
sudo systemctl start mosquitto

# Watch the offline queue drain automatically
```

---

## Running the tests

```bash
# Install test dependencies (separate from production deps)
pip install -r dev-requirements.txt

# Run all unit and component tests (no broker needed, ~4 seconds)
pytest

# Run with verbose output to see each test name
pytest -v

# Run integration tests (requires Mosquitto on localhost:1883)
mosquitto -p 1883 &
pytest -m integration -v
```

The test suite has 73 tests across 5 files. Integration tests are skipped automatically when no broker is detected. Running `pytest` without any flags runs everything except integration tests.

---

## File reference

`production_client.py` — The main entry point. Instantiate `ProductionMQTTClient`, call `.connect()` and `.start()`, then use `.publish()` for all outgoing messages. The client decides internally whether to send directly or queue offline.

`inflight_tracker.py` — SQLite-backed store for messages that have been handed to the broker but not yet acknowledged. Used internally by `ProductionMQTTClient`; your code doesn't need to touch this directly.

`offline_queue.py` — SQLite-backed queue for messages waiting to be sent. Handles priority ordering, batch retrieval, and capacity management. Also internal.

`config.py` — The `Config` class. Load with `Config.from_file("config.json")`, `Config.from_env()`, or `Config({...})`. All supported fields and their defaults are listed in `Config.DEFAULTS`. Supports JSON files, key=value files, environment variables, and plain dicts. All fields validated at load time.

`production_logger.py` — The `ProductionLogger` class and `get_logger()` factory. Rotating file logs plus a structured `.jsonl` metrics file.

`test_13.py` — End-to-end simulation that publishes temperature readings every 5 seconds and prints queue statistics every 10 readings. Specifically designed to demonstrate offline behaviour.


---

## Requirements

- Python 3.7 or later
- `paho-mqtt`
- An MQTT broker (Mosquitto for local development; any TLS-capable broker for production)
- SQLite3 (included in the standard library)

---


## License

MIT License. See `LICENSE` for details.
