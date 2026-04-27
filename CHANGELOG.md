# Changelog

All notable changes to this project are documented here.
The format follows [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).
Versioning follows [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

---

## [1.0.0] — Stable & Packaged

The first stable release. The public API is now considered stable — breaking changes will not be made without a major version bump.

### Added
- `pyproject.toml` for `pip install .` support using setuptools. The library can now be installed into any Python environment as a proper package. Development dependencies are also available via `pip install ".[dev]"`.
- `CHANGELOG.md` (this file) to track changes between versions in a format that is easy for users to read when upgrading.
- Type hints throughout all five source modules (`production_client.py`, `offline_queue.py`, `inflight_tracker.py`, `config.py`, `production_logger.py`). All public method signatures now declare their parameter and return types using `typing` module primitives compatible with Python 3.7+.

### Changed
- `pytest.ini` configuration merged into `pyproject.toml` under `[tool.pytest.ini_options]`. Running `pytest` from the project root works exactly as before.

### Removed
- `pytest.ini` — superseded by the `[tool.pytest.ini_options]` section in `pyproject.toml`.

---

## [0.8.0] — Test Coverage

### Added
- `tests/` directory with 73 tests across five files: `test_offline_queue.py`, `test_inflight_tracker.py`, `test_topic_matching.py` (33 parametrized wildcard cases), `test_production_client.py` (with paho-mqtt mocked), and `test_integration.py` (skipped when no broker is available).
- `tests/conftest.py` with shared fixtures: `configure_logger` (session-scoped, writes to a temp directory) and `db_connection` (per-test in-memory SQLite).
- `pytest.ini` for test configuration.
- `dev-requirements.txt` separating test dependencies from production dependencies.

### Fixed
- Test development revealed that the queue eviction policy always succeeds via the oldest-message fallback. The mental model in documentation was incorrect and has been corrected.

---

## [0.7.0] — Observability

### Added
- HTTP health check server started in `start()` when `enable_health_check=True`. `GET /health` returns a JSON body with three statuses: `healthy` (HTTP 200), `degraded` (HTTP 200, queue ≥ 80% full), or `unhealthy` (HTTP 503, not connected). `_DEGRADED_THRESHOLD = 80.0` controls the boundary.
- `active_subscriptions` field added to `get_statistics()` output.
- Docker `HEALTHCHECK` and Kubernetes liveness probe examples in the README.

### Changed
- `start()` now conditionally starts the health check server after `loop_start()`.
- `stop()` calls `_stop_health_check_server()` before `loop_stop()` to ensure the port is released cleanly.

---

## [0.6.0] — Bidirectional Communication

### Added
- `subscribe(topic, callback, qos)` — registers a callback for an MQTT topic pattern. Safe to call before `connect()`; subscriptions registered offline are sent to the broker automatically on the next connection.
- `unsubscribe(topic)` — removes the callback and sends an UNSUBSCRIBE packet when connected.
- `_restore_subscriptions()` — called from `_on_connect()` on every connection. Replays the full subscription registry to the broker, covering both pre-existing and newly registered subscriptions.
- `_on_message()` — routes incoming messages to matching callbacks using a snapshot pattern to avoid holding the lock during user callback execution.
- `_topic_matches(pattern, topic)` — static method implementing the full MQTT wildcard specification for `+` (single level) and `#` (multi-level, must be last).

### Changed
- `_on_connect()` now calls `_restore_subscriptions()` between `_resend_inflight_messages()` and `_start_queue_drainer()`. Order is intentional: subscriptions must be active before queued messages are replayed.
- `get_statistics()` now includes `active_subscriptions`.

---

## [0.5.0] — Production Connectivity

### Added
- TLS support: `connect()` calls `client.tls_set()` before opening the socket when `use_tls=True`. Supports one-way TLS (`ca_certs` only) and mutual TLS (`ca_certs` + `certfile` + `keyfile`).
- Authentication: `connect()` calls `client.username_pw_set()` before sending the CONNECT packet when `username` is set.
- New constructor parameters: `use_tls`, `ca_certs`, `certfile`, `keyfile`, `username`, `password`.
- `certfile` and `keyfile` added to `Config.DEFAULTS`.
- TLS and authentication fields now flow through `from_config()`.

### Fixed
- Queue drainer threading: the `join(timeout=2)` call in `_stop_queue_drainer()` has been replaced with `threading.Event`. The drainer uses `event.wait(timeout=N)` instead of `time.sleep(N)`, making all sleeps interruptible. `_stop_queue_drainer()` now sets the event and returns immediately — safe to call from paho's callback thread without stalling the network loop. `stop()` still joins the drainer thread, which is safe because `stop()` is not called from a paho callback.

---

## [0.4.0] — Internal Consistency

### Added
- `ProductionMQTTClient.from_config(config)` factory method. The recommended way to create a client instance. All configuration fields map directly from a `Config` object.
- New constructor parameters: `db_path`, `min_backoff`, `max_backoff`, `log_dir`, `log_level` (previously hardcoded).

### Changed
- `InflightTracker` and `OfflineQueue` now share a single SQLite connection and lock created in `ProductionMQTTClient.__init__()`. Both classes accept optional `conn` and `lock` parameters; if omitted they create their own connection (backward compatible). `_owns_connection` flag controls whether `close()` closes the connection or defers to the owner.
- All `print()` calls in `production_client.py`, `inflight_tracker.py`, and `offline_queue.py` replaced with structured `ProductionLogger` calls at appropriate log levels (DEBUG for individual message operations, INFO for lifecycle events, WARNING for abnormal-but-handled conditions, ERROR for failures).
- `stop()` now closes the shared SQLite connection after both storage systems have finished.

---

## [0.3.0] — Data Integrity

### Fixed
- **Payload encoding corruption** — `InflightTracker` stored payloads as `str(payload)`, corrupting binary data. The column type is now `BLOB` and payloads are stored as raw bytes via the `_to_bytes()` helper. A schema migration runs on startup: if the old TEXT-based table is detected (via `PRAGMA table_info`), it is dropped and rebuilt. Corrupted rows are not recoverable and are discarded.
- **Resend tracking gap** — When inflight messages were resent after reconnection, paho assigned new packet IDs (`mid`). The old code never updated the tracker with the new IDs, so a second disconnection before acknowledgment silently lost those messages. The fix removes the stale tracker entry and inserts a fresh one with the new `mid` immediately after each resend.
- **Race condition in `publish()`** — Between checking `is_connected` and calling `client.publish()`, the connection could drop, leaving the message neither published nor queued. The fix wraps `client.publish()` in a `try/except`; on failure, `is_connected` is set to `False` and the message is routed to the offline queue.

---

## [0.2.0] — Initial Release

First working version. Included offline queuing with SQLite persistence, inflight tracking for QoS 1/2 acknowledgment, priority-based eviction, exponential backoff reconnection, rotating file logging with structured metrics output, and multi-source configuration loading (JSON file, key=value file, environment variables, dictionary).
