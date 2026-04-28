# robmqtt — A Resilient MQTT Client for Edge Devices

[![PyPI](https://img.shields.io/pypi/v/robmqtt.svg)](https://pypi.org/project/robmqtt/)
[![Python](https://img.shields.io/badge/python-3.9%2B-blue.svg)](pyproject.toml)
[![License: MIT](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)

An MQTT client built for devices that live in the real world — Raspberry Pis, industrial gateways, field sensors — where the network is unreliable and dropping a message silently is not acceptable.

---

## Why I built this library

Standard MQTT clients assume the network is up. On an edge device, that assumption breaks regularly. Cellular links drop, Wi-Fi roams, VPNs time out, brokers restart. Most client libraries handle this by... not handling it. They hand the problem back to your application code and wish you luck.

This library treats disconnection as a normal operating condition rather than an edge case. When the broker is unreachable, outgoing messages are written to a local SQLite queue and held there until the connection returns. Messages that were already sent but not yet acknowledged are tracked separately so they can be re-sent after reconnection. The client is fully bidirectional — you can subscribe to topics and receive messages just as reliably as you can publish them. It also exposes an HTTP health check endpoint so external systems can observe its state without having to inspect log files or SSH into the device.

---

## Quick start

```bash
pip install robmqtt
```

```python
from robmqtt import ProductionMQTTClient

client = ProductionMQTTClient(
    client_id="my-device-001",
    broker_host="localhost",
    broker_port=1883,
)
client.connect()
client.start()

client.publish("sensors/temperature", '{"value": 23.5}', qos=1)
```

That's the whole minimum example. The client now keeps a SQLite-backed offline queue, tracks inflight messages, and reconnects on its own if the broker disappears.

---

## Architecture

```
Your Application
      │
      ▼
robmqtt.ProductionMQTTClient
      ├── InflightTracker      — sent-but-unacknowledged messages
      ├── OfflineQueue         — messages queued during disconnection
      └── Config               — configuration from file/env/args

robmqtt.ProductionLogger       — rotating file logs + structured metrics
HTTP Health Check              — built into ProductionMQTTClient
```

The client is the only thing your application talks to directly. `InflightTracker` and `OfflineQueue` are internal — the client decides which one to use based on the current connection state. You don't have to think about either of them.

---

## Features

**Zero message loss** — Messages published while offline are written to SQLite and replayed automatically once the broker is reachable again. QoS 1/2 messages that were acknowledged by your app but not yet confirmed by the broker are tracked and re-sent after reconnection.

**TLS encryption** — Set `use_tls=True` and provide a CA certificate to encrypt all traffic between the device and the broker. Mutual TLS (where the broker also verifies the device's identity using a client certificate) is supported for brokers that require it, such as AWS IoT Core.

**Authentication** — Username and password credentials are passed in the MQTT CONNECT packet. Works alongside TLS so credentials are always encrypted in transit.

**Priority-based eviction** — Each message carries a priority from 1 to 10. When the queue fills up, lower-priority messages are dropped first. A critical alert can displace routine telemetry rather than being blocked behind a full queue of sensor readings.

**Exponential backoff** — Reconnection intervals double on each failed attempt, up to a configurable ceiling. This matters in fleet deployments — you don't want hundreds of devices hammering a broker the moment it comes back online.

**Thread-safe storage** — All SQLite operations are guarded by locks. The MQTT network thread and the application thread can both interact with the database without you having to coordinate anything at the call site.

**Structured logging** — The bundled logger wraps Python's standard logging with rotating file handlers, a structured key-value format, and a separate `.jsonl` metrics file that works well with Prometheus, Loki, or similar tools.

**Flexible configuration** — `Config` can load from a JSON file, a simple `key=value` file, environment variables, or a plain dictionary. Everything is validated at load time with clear error messages.

**Subscribe support** — Register callbacks for incoming messages using full MQTT wildcard syntax. Subscriptions survive reconnections automatically.

**Health check endpoint** — When `enable_health_check=True`, a minimal HTTP server runs on a background thread and exposes `GET /health`. The response body is a JSON snapshot of the client's current state. The HTTP status code is 200 when the client is healthy or degraded, and 503 when it is not connected to the broker.

---

## Installation

```bash
pip install robmqtt
```

That's it. `paho-mqtt` is pulled in as a dependency. SQLite3 is part of the Python standard library, so nothing else to install.

If you want the test dependencies for running the project's own test suite (you usually don't, unless you're contributing):

```bash
pip install "robmqtt[dev]"
```

---

## Usage

All code samples below assume you've run `pip install robmqtt`.

### Basic publishing

```python
from robmqtt import ProductionMQTTClient, Config

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

### Publishing and subscribing

```python
from robmqtt import ProductionMQTTClient, Config

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

### With TLS (production broker)

```python
from robmqtt import ProductionMQTTClient, Config

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

### With health check enabled

```python
from robmqtt import ProductionMQTTClient, Config

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

Querying it:

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

#### Docker HEALTHCHECK

```dockerfile
HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
  CMD curl -f http://localhost:8080/health || exit 1
```

#### Kubernetes liveness probe

```yaml
livenessProbe:
  httpGet:
    path: /health
    port: 8080
  initialDelaySeconds: 10
  periodSeconds: 30
  failureThreshold: 3
```

---

## Configuration

All fields have sensible defaults except `client_id`, which must be set explicitly because there's no safe default for a value that has to be unique across your entire fleet.

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
from robmqtt import ProductionLogger

logger = ProductionLogger("my_app")

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

## Reconnection model

The client distinguishes two kinds of disconnect:

- **Network failure** — the TCP connection drops unexpectedly (broker restart, Wi-Fi outage, cable unplugged). The client treats this as recoverable and automatically reconnects with exponential backoff. While disconnected, calls to `publish()` route to the offline queue.

- **Clean disconnect** — your code, or the broker, explicitly closed the connection. The client treats this as deliberate and does **not** attempt to reconnect on its own. To bring the connection back, call `client.connect()` and `client.start()` again.

The reason for the distinction: silently auto-reconnecting after the user explicitly closed the connection is a common source of confusing behaviour in MQTT libraries. Manual disconnect should mean what it says.

---

## Testing the offline queue

The quickest way to see the offline queue in action is against a local Mosquitto broker. Save the following as `demo_offline.py`:

```python
import time
from robmqtt import ProductionMQTTClient

client = ProductionMQTTClient(
    client_id="offline-demo",
    broker_host="localhost",
    broker_port=1883,
)
client.connect()
client.start()

print("Publishing every 5 seconds.")
print("Stop your broker (e.g. `sudo systemctl stop mosquitto`)")
print("and watch the queue grow. Restart it and watch the queue drain.")

i = 0
try:
    while True:
        client.publish("demo/temperature", f'{{"reading": {i}}}'.encode(), qos=1)
        i += 1
        if i % 5 == 0:
            stats = client.get_statistics()
            print(
                f"  connected={stats['connected']}  "
                f"queued={stats['offline_queue']['total_messages']}"
            )
        time.sleep(5)
except KeyboardInterrupt:
    client.stop()
```

Run it in one terminal:

```bash
python demo_offline.py
```

In a second terminal, simulate an outage and recovery:

```bash
sudo systemctl stop mosquitto
# Watch the queue grow in the first terminal.
sudo systemctl start mosquitto
# Watch the queue drain.
```

---

## API reference

The package exports five classes at the top level:

| Class | Purpose |
|---|---|
| `ProductionMQTTClient` | The client. Most applications only ever instantiate this one. |
| `Config` | Configuration loader. Supports JSON files, key=value files, environment variables, and dicts. |
| `OfflineQueue` | SQLite-backed queue for messages that arrived while disconnected. Internal — exposed for advanced use only. |
| `InflightTracker` | SQLite-backed store for sent-but-unacknowledged messages. Internal — exposed for advanced use only. |
| `ProductionLogger` | Rotating file logger plus structured metrics. Used by `ProductionMQTTClient` internally and available standalone. |

```python
from robmqtt import (
    ProductionMQTTClient,
    Config,
    OfflineQueue,
    InflightTracker,
    ProductionLogger,
)
```

`ProductionMQTTClient` public methods:

- `connect()` — establish the initial connection to the broker
- `start()` — start the network loop thread and (if configured) the health check server
- `publish(topic, payload, qos=0, retain=False, priority=1)` — publish a message; routes to broker if connected, to offline queue otherwise
- `subscribe(topic, callback, qos=1)` — register a callback for incoming messages; callback signature is `(topic, payload, qos, retain) -> None`
- `unsubscribe(topic)` — remove a subscription
- `get_statistics()` — return a dict snapshot of current client state
- `stop()` — graceful shutdown; closes the database. Terminal — do not call other methods after this.

---

## Running the project's own tests

Only relevant if you're modifying the library itself.

```bash
git clone https://github.com/ranaweerasupun/resilient-edge-mqtt-client.git
cd resilient-edge-mqtt-client
pip install -e ".[dev]"

# Run all unit and component tests (no broker needed)
pytest

# Run integration tests (requires Mosquitto on localhost:1883)
pytest -m integration -v
```

The test suite has 73 tests across 5 files. Integration tests are skipped automatically when no broker is detected.

---

## Requirements

- Python 3.9 or later
- `paho-mqtt` 1.6.x (installed automatically)
- An MQTT broker (Mosquitto for local development; any TLS-capable broker for production)

---

## License

MIT License. See `LICENSE` for details.