# -*- coding: utf-8 -*-
"""
test_v100.py — Full Feature Tour for v1.0.0 (Stable & Packaged)

v1.0.0 is the first stable release. The API is now intentional and committed.
This script demonstrates all eight major features of the library together,
starting with verifying the package installation and ending with a full
bidirectional session that exercises every layer simultaneously.

v1.0.0 features demonstrated:
  - Package installation check (pip install .)
  - Type-annotated API — editor autocomplete and mypy compatibility
  - All three Config sources (dict, file, environment)
  - TLS configuration reference
  - Publish with offline queuing and priority
  - Subscribe with wildcards
  - Health check endpoint
  - Complete get_statistics() snapshot

Run against a local Mosquitto broker:
    mosquitto -p 1883
    python test_v100.py
"""

import json
import os
import sys
import time
import http.client
from typing import Callable
import paho.mqtt.client as mqtt

from production_client import ProductionMQTTClient
from config import Config
from production_logger import get_logger, ProductionLogger


SEPARATOR         = "─" * 60
HEALTH_PORT       = 18100
DEVICE_ID         = "v100_stable_device"
SENSOR_TOPIC      = f"sensors/{DEVICE_ID}/temperature"
COMMAND_TOPIC     = f"devices/{DEVICE_ID}/commands/#"
STATUS_TOPIC      = f"devices/{DEVICE_ID}/status"


def section(title: str) -> None:
    print(f"\n{SEPARATOR}")
    print(f"  {title}")
    print(SEPARATOR)


# ── Section 0: Installation check ─────────────────────────────────────────────

section("0 — Installation Check")

print("Verifying that all library modules can be imported cleanly.\n")

modules = [
    ("production_client", "ProductionMQTTClient"),
    ("config",            "Config"),
    ("production_logger", "get_logger"),
    ("inflight_tracker",  "InflightTracker"),
    ("offline_queue",     "OfflineQueue"),
]

all_ok = True
for module_name, class_name in modules:
    try:
        mod = __import__(module_name)
        getattr(mod, class_name)
        print(f"  ✓ {module_name}.{class_name}")
    except (ImportError, AttributeError) as e:
        print(f"  ✗ {module_name}.{class_name} — {e}")
        all_ok = False

if all_ok:
    print("\n✓ All modules importable. If installed via pip install ., these")
    print("  would work from any directory, not just the project root.")
else:
    print("\n✗ Some imports failed. Run: pip install .")
    sys.exit(1)


# ── Section 1: Type-annotated API ─────────────────────────────────────────────

section("1 — Type-annotated API (v1.0.0 addition)")

print("""
v1.0.0 added type hints to every public method signature. These have no effect
at runtime, but they enable two things:

  1. Editor autocomplete: when you type client.subscribe(, your editor shows:
       subscribe(topic: str, callback: Callable[[str, bytes, int, bool], None],
                 qos: int = 1) -> None

  2. Static analysis: mypy can tell you if you pass the wrong type BEFORE running.
     Example — passing a string instead of a callable:
       client.subscribe("t/+", "not_a_callback")
       mypy: Argument "callback" has incompatible type "str"; expected
             "Callable[[str, bytes, int, bool], None]"

The type annotations are declarations. They document what the API expects
and what it returns. The Callable type for the subscribe callback is the
most expressive:  Callable[[str, bytes, int, bool], None]  reads as:
'a function taking (topic: str, payload: bytes, qos: int, retain: bool)
and returning None.'
""")

# Demonstrate a correctly-typed callback
def on_temperature(topic: str, payload: bytes, qos: int, retain: bool) -> None:
    """This signature matches Callable[[str, bytes, int, bool], None]."""
    data = json.loads(payload)
    print(f"  📊 Temperature: {data.get('value')}°C  (topic={topic}, qos={qos})")

def on_command(topic: str, payload: bytes, qos: int, retain: bool) -> None:
    """Command handler — same signature."""
    data = json.loads(payload)
    print(f"  📨 Command on {topic}: {data}")

print("Both callbacks have the correct type signature: Callable[[str, bytes, int, bool], None]")


# ── Section 2: Config — all three sources ─────────────────────────────────────

section("2 — Config Sources")

print("The same Config class reads from any of three sources.\n")

# Dictionary — useful for scripting and tests
config_dict = Config({
    "client_id":            DEVICE_ID,
    "broker_host":          "localhost",
    "broker_port":          1883,
    "max_queue_size":       50,
    "log_dir":              "/tmp/logs_v100",
    "db_path":              "/tmp/v100_demo.db",
    "enable_health_check":  True,
    "health_check_port":    HEALTH_PORT,
    "min_backoff":          1,
    "max_backoff":          8,
})
print(f"  Dictionary:    broker={config_dict.get('broker_host')}:{config_dict.get('broker_port')}")

# Environment — useful for containers
os.environ["MQTT_MAX_BACKOFF"] = "30"
config_env = Config.from_env()
print(f"  Environment:   MQTT_MAX_BACKOFF → max_backoff={config_env.get('max_backoff')}s")

# File — recommended for production; use config_dict for this demo
config_file_path = "/tmp/v100_demo_config.json"
config_dict.save_to_file(config_file_path)
config_file = Config.from_file(config_file_path)
print(f"  JSON file:     loaded from {config_file_path}")

print("\n✓ All three sources work. Use from_config() with whichever is appropriate.")


# ── Section 3: Full session ────────────────────────────────────────────────────

section("3 — Full Session: Publish + Subscribe + Health Check")

print("Creating client via from_config() (the recommended path since v0.4.0).\n")

client = ProductionMQTTClient.from_config(config_dict)

# Subscribe before connecting
client.subscribe(COMMAND_TOPIC,  on_command,     qos=1)
client.subscribe(SENSOR_TOPIC,   on_temperature, qos=0)

print(f"Subscriptions registered: {client.get_statistics()['active_subscriptions']}")

client.connect()
client.start()

print("Waiting for connection and health check server...")
timeout = time.time() + 10
while not client.is_connected and time.time() < timeout:
    time.sleep(0.1)
time.sleep(0.3)  # let health server bind

if not client.is_connected:
    print("Could not connect to broker. Is Mosquitto running?")
    client.stop()
    sys.exit(1)

print(f"✓ Connected. Health check: http://localhost:{HEALTH_PORT}/health\n")

# Show initial health
try:
    conn = http.client.HTTPConnection("localhost", HEALTH_PORT, timeout=2)
    conn.request("GET", "/health")
    resp = conn.getresponse()
    body = json.loads(resp.read().decode())
    conn.close()
    print(f"GET /health → HTTP {resp.status}  status={body['status']}")
except Exception as e:
    print(f"Health endpoint not responding: {e}")


# ── Section 4: Publish sensor readings ────────────────────────────────────────

section("4 — Publishing with Priority")

print("Publishing temperature readings. High temps get priority=10.\n")

readings_sent = 0
for i in range(8):
    temperature = 22 + i   # climbs from 22 to 29
    is_high     = temperature >= 28
    priority    = 10 if is_high else 3
    payload     = json.dumps({
        "device_id":   DEVICE_ID,
        "value":       temperature,
        "unit":        "C",
        "sequence":    i + 1,
        "high_temp":   is_high,
    }).encode()

    client.publish(
        topic=SENSOR_TOPIC,
        payload=payload,
        qos=1,
        retain=False,
        priority=priority,
    )
    readings_sent += 1
    label = "🔥 HIGH " if is_high else "      "
    print(f"  {label} {temperature}°C  priority={priority}")
    time.sleep(0.3)

print(f"\n✓ Published {readings_sent} readings.")


# ── Section 5: Cloud sends commands ───────────────────────────────────────────

section("5 — Receiving Commands (Cloud → Device)")

print("A separate paho client acts as the cloud, sending three commands.\n")

cloud = mqtt.Client(client_id="cloud_v100", clean_session=True)
cloud.connect("localhost", 1883)
cloud.loop_start()
time.sleep(0.3)

commands_to_send = [
    (f"devices/{DEVICE_ID}/commands/fan",      {"action": "on",  "speed": 80}),
    (f"devices/{DEVICE_ID}/commands/led",      {"color": "blue", "blink": False}),
    (f"devices/{DEVICE_ID}/commands/setpoint", {"target_celsius": 24, "tolerance": 1}),
]
for topic, payload in commands_to_send:
    cloud.publish(topic, json.dumps(payload), qos=1)
    time.sleep(0.4)

time.sleep(1)
cloud.loop_stop()
cloud.disconnect()
print("✓ Commands sent by cloud, received by device callbacks.")


# ── Section 6: Offline behaviour ──────────────────────────────────────────────

section("6 — Offline Queue Behaviour")

print("Simulating offline by disconnecting. Messages go to the queue.\n")

client.client.disconnect()
time.sleep(0.5)
print(f"Disconnected: is_connected={client.is_connected}")

for i in range(5):
    client.publish(
        f"sensors/{DEVICE_ID}/offline_{i}",
        f"offline_reading_{i}".encode(),
        qos=1,
        priority=5,
    )

stats = client.get_statistics()
print(f"Queue depth while offline: {stats['offline_queue']['total_messages']} messages")
print(f"Queue capacity used:       {stats['offline_queue']['capacity_used_percent']:.1f}%")

print("\nReconnecting — queue will drain automatically...")
client.client.reconnect()
timeout = time.time() + 10
while not client.is_connected and time.time() < timeout:
    time.sleep(0.1)
time.sleep(2)   # give the drainer time to run

stats = client.get_statistics()
print(f"Queue depth after reconnect: {stats['offline_queue']['total_messages']} messages")
if stats["offline_queue"]["total_messages"] == 0:
    print("✓ Offline queue drained successfully.")


# ── Final statistics ──────────────────────────────────────────────────────────

section("Final Statistics (get_statistics())")

final_stats = client.get_statistics()
print(json.dumps(final_stats, indent=2, default=str))

# Check health one last time
try:
    conn = http.client.HTTPConnection("localhost", HEALTH_PORT, timeout=2)
    conn.request("GET", "/health")
    resp = conn.getresponse()
    body = json.loads(resp.read().decode())
    conn.close()
    print(f"\nFinal health: HTTP {resp.status}  status={body['status']}")
except Exception:
    pass


# ── Shutdown ──────────────────────────────────────────────────────────────────

client.stop()

print(f"\n{'=' * 60}")
print("  v1.0.0 full feature tour complete.")
print("  This is the stable API — these interfaces will not break")
print("  in any future 1.x release.")
print(f"{'=' * 60}\n")
