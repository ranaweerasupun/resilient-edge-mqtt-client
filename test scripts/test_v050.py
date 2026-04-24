# -*- coding: utf-8 -*-
"""
test_v050.py — Simulation script for v0.5.0 (Production Connectivity)

v0.5.0 made two changes: it wired TLS and authentication into connect(), and
it replaced the blocking join() in _stop_queue_drainer() with a threading.Event
so that paho's network callback thread is never blocked during disconnection.

v0.5.0 features demonstrated:
  - TLS configuration patterns (one-way and mutual TLS shown as code)
  - Authentication configuration
  - Non-blocking disconnect: measure how quickly the client recovers
  - Reconnection backoff visible in real time

Run against a local Mosquitto broker (no TLS needed for the timing tests):
    mosquitto -p 1883
    python test_v050.py
"""

import json
import time
from production_client import ProductionMQTTClient
from config import Config


SEPARATOR = "─" * 60


def section(title):
    print(f"\n{SEPARATOR}")
    print(f"  {title}")
    print(SEPARATOR)


# ── Section 1: TLS Configuration Reference ───────────────────────────────────

section("1 — TLS Configuration Patterns (Reference)")

print("""
Before v0.5.0, these Config fields existed but were silently ignored:
  use_tls, ca_certs, certfile, keyfile, username, password

After v0.5.0, connect() calls tls_set() and username_pw_set() before
opening the socket. Here is how each deployment pattern looks:

One-way TLS (verify the broker, common for HiveMQ, Azure IoT Hub):

    config = Config({
        "client_id":   "my_device",
        "broker_host": "mqtt.example.com",
        "broker_port": 8883,
        "use_tls":     True,
        "ca_certs":    "/etc/ssl/certs/broker-ca.pem",
        "username":    "device_001",
        "password":    "secret",
    })

Mutual TLS (broker also verifies the device, required by AWS IoT Core):

    config = Config({
        "client_id":   "my_device",
        "broker_host": "xxxx.iot.us-east-1.amazonaws.com",
        "broker_port": 8883,
        "use_tls":     True,
        "ca_certs":    "/certs/AmazonRootCA1.pem",
        "certfile":    "/certs/device-certificate.pem.crt",
        "keyfile":     "/certs/device-private.pem.key",
    })

The order inside connect() is critical:
  1. tls_set()          — wraps the socket in SSL before it opens
  2. username_pw_set()  — credentials go in the CONNECT packet
  3. client.connect()   — opens the socket, sends CONNECT
""")

print("(Connecting to localhost without TLS for the timing tests below.)")


# ── Section 2: Non-blocking disconnect timing ─────────────────────────────────

section("2 — Non-blocking Disconnect (threading.Event Fix)")

print("""
In v0.4.0, _stop_queue_drainer() called thread.join(timeout=2). Because it
was called from paho's _on_disconnect callback, this stalled paho's network
loop for up to 2 seconds on every disconnection.

In v0.5.0, _stop_queue_drainer() just sets an Event and returns immediately.
The drainer wakes up and exits on its own. We can measure the difference by
timing how long a full disconnect-reconnect cycle takes.
""")

config = Config({
    "client_id":     "v050_timing_demo",
    "broker_host":   "localhost",
    "broker_port":   1883,
    "log_dir":       "/tmp/logs_v050",
    "db_path":       "/tmp/v050_demo.db",
    "min_backoff":   1,
    "max_backoff":   4,
})

client = ProductionMQTTClient.from_config(config)
client.connect()
client.start()

print("Waiting for connection...")
if not any(client.is_connected for _ in range(30) if not time.sleep(0.1)):
    pass
time.sleep(1)

# Publish some messages to get the queue drainer running
for i in range(5):
    client.publish(f"test/v050/{i}", f"msg_{i}".encode(), qos=1)

time.sleep(0.5)
print(f"Connected: {client.is_connected}")

# Measure how quickly a disconnect is processed
print("\nTriggering a clean disconnect...")
t_start = time.time()
client.client.disconnect()

# Poll until disconnected
while client.is_connected and (time.time() - t_start) < 5:
    time.sleep(0.01)

t_disconnect = time.time() - t_start
print(f"Disconnect processed in {t_disconnect*1000:.0f}ms")

if t_disconnect < 0.5:
    print("✓ Disconnect is near-instant — threading.Event is working correctly.")
    print("  (v0.4.0 would have blocked for up to 2000ms here.)")
else:
    print(f"  Took {t_disconnect:.2f}s — broker may be slow to respond.")


# ── Section 3: Reconnection backoff visible ───────────────────────────────────

section("3 — Reconnection Backoff")

print("""
When a connection drops unexpectedly, the client waits before retrying —
and the wait doubles on each failure up to max_backoff. This prevents a
fleet of devices from hammering a broker that just came back online.

Watch the backoff sequence: 1s → 2s → 4s (capped at max_backoff=4s).
""")

print(f"Current backoff state: {client.current_backoff}s")
print(f"min_backoff={config.get('min_backoff')}s, max_backoff={config.get('max_backoff')}s")
print("\nWaiting for reconnection to localhost:1883...")
print("(If Mosquitto is running, the client will reconnect within a few seconds.)\n")

reconnect_deadline = time.time() + 15
while not client.is_connected and time.time() < reconnect_deadline:
    print(f"  Backoff state: {client.current_backoff}s, connected={client.is_connected}")
    time.sleep(1.5)

if client.is_connected:
    print(f"\n✓ Reconnected. Backoff reset to {client.current_backoff}s (minimum).")
else:
    print("\n(Broker not available — that is fine for this demo.)")


# ── Final statistics ──────────────────────────────────────────────────────────

section("Statistics")
print(json.dumps(client.get_statistics(), indent=2, default=str))

client.stop()
print(f"\n{SEPARATOR}")
print("  v0.5.0 demo complete.")
print(SEPARATOR)
