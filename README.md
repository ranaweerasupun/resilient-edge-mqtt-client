# Production MQTT Client for Edge Devices

An MQTT client built for devices that live in the real world — Raspberry Pis, industrial gateways, field sensors — where the network is unreliable and dropping a message silently is not acceptable.

---

## The Reason for Designing This ...

Standard MQTT clients assume the network is up. On an edge device, that assumption breaks regularly. Cellular links drop, Wi-Fi roams, VPNs time out, brokers restart. Most client libraries handle this by... not handling it. They hand the problem back to your application code and wish you luck.

This library treats disconnection as a normal operating condition rather than an edge case. When the broker is unreachable, outgoing messages are written to a local SQLite queue and held there until the connection returns. Messages that were already sent but not yet acknowledged are tracked separately so they can be re-sent after reconnection — closing the gap that most libraries leave open.

The goal is simple: if you call `publish()`, the message eventually arrives at the broker. Full stop.

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

---

## Installation

```bash
pip install -r requirements.txt
```

SQLite3 is part of the Python standard library, so no separate install needed.

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

## Changelog

### v0.5.0 — Production Connectivity

This release makes the library actually connectable to real production brokers and fixes a threading bug that could cause paho's network loop to stall on disconnect.

**TLS support** — `connect()` now calls `client.tls_set()` before opening the connection when `use_tls=True`. Providing `ca_certs` enables one-way TLS, where the client verifies the broker's certificate against a known CA. Providing `certfile` and `keyfile` additionally enables mutual TLS (mTLS), where the broker also verifies the client's identity. mTLS is required by AWS IoT Core and some other enterprise brokers. Without this release, the `use_tls`, `ca_certs`, `certfile`, and `keyfile` config fields existed but were silently ignored.

**Authentication** — `connect()` now calls `client.username_pw_set()` before opening the connection when a `username` is configured. Previously the `username` and `password` config fields were also silently ignored.

**Queue drainer threading fix** — The `_stop_queue_drainer()` method previously called `thread.join(timeout=2)`, which blocks the calling thread for up to 2 seconds. This method is called from `_on_disconnect()`, which runs on paho's internal network thread. Blocking that thread means paho cannot process any network events — incoming packets, further disconnections, reconnection acknowledgments — for the duration of the join. The fix replaces the boolean flag and `join()` pattern with a `threading.Event`. The drainer uses `event.wait(timeout=N)` instead of `time.sleep(N)`, which means it can be interrupted immediately when the event is set. `_stop_queue_drainer()` now simply sets the event and returns — no blocking at all. The `stop()` method, which is called by the user rather than paho's thread, still joins the drainer thread for a clean shutdown.

### v0.4.0 — Internal Consistency

This release is about making the components of the library actually work as a unified system rather than as a collection of loosely connected modules.

**Single database file** — `InflightTracker` and `OfflineQueue` previously maintained separate SQLite files (`inflight_messages.db` and `mqtt_client.db`). They now share a single connection to a single file, coordinated by a shared lock. Both tables live side by side in `mqtt_client.db`. This simplifies the lifecycle (one connection to open, one to close), eliminates the extra file handle, and removes the possibility of the two databases ever getting out of sync. Backward compatibility is preserved: both classes still accept a standalone `db_path` argument if used outside of `ProductionMQTTClient`.

**Config integration** — `ProductionMQTTClient` now has a `from_config()` class method that reads all relevant settings — broker address, queue size, database path, log directory, backoff limits — from a `Config` object. Previously, the client was hardcoding values like `min_backoff=1` and `max_backoff=60` internally and ignoring the `Config` class entirely despite it being part of the project. `from_config()` is now the recommended instantiation path. The direct constructor still works.

**Logger integration** — All `print()` calls across `production_client.py`, `inflight_tracker.py`, and `offline_queue.py` have been replaced with structured log calls via `ProductionLogger`. Connection events log at `INFO`, queue operations at `DEBUG`, warnings at `WARNING`, errors at `ERROR`. This means operators can now control verbosity via `log_level` in config, route output to rotating log files, and feed structured events to monitoring tools — none of which was possible when everything went to stdout.

### v0.3.0 — Data Integrity

Two silent data-loss bugs were fixed in this release, plus a race condition that could cause a message to vanish without a trace.

**Payload encoding in `InflightTracker`** — The old schema stored message payloads as `TEXT` using Python's `str()`. This looks harmless when the payload is a plain string, but if the payload is `bytes` — which is common for binary sensor data or anything serialised with protobuf — `str(b'\x00\x01')` produces the literal string `"b'\\x00\\x01'"` rather than the actual bytes. The broker receives garbage. The fix is to store payloads as `BLOB` and encode strings to UTF-8 explicitly before storing. On first startup after upgrading, `InflightTracker` detects the old schema and migrates automatically. The old corrupted rows are dropped — they weren't recoverable anyway.

**Resent messages disappearing after a second disconnect** — When the client reconnects, it resends any messages that were inflight during the previous session. The problem was that paho assigns a brand-new packet ID (`mid`) each time a message is published, including resends. The old code called `client.publish()` but never updated the tracker with the new `mid`, so the tracker still held the stale ID from the previous connection. If the broker disconnected again before sending its acknowledgment, the tracker would have no entry for the new `mid` and the message would be quietly abandoned. The fix is to remove the old tracker entry immediately after republishing and insert a new one with the fresh `mid`.

**Race condition in `publish()`** — Between checking `is_connected` and calling `client.publish()`, the connection can drop. In that narrow window, the message was neither sent nor queued — it simply disappeared. The fix wraps `client.publish()` in a try/except: if the send raises because the socket is already dead, the exception is caught, `is_connected` is set to `False`, and the message is routed to the offline queue as a fallback.

### v0.2.0

Initial release with offline queuing, inflight tracking, exponential backoff, priority-based eviction, structured logging, and flexible configuration.

---

## License

MIT License. See `LICENSE` for details.
