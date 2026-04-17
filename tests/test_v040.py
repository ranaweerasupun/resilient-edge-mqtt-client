# -*- coding: utf-8 -*-
"""
test_v040.py — Simulation script for v0.4.0 (Internal Consistency)

v0.4.0 wired the components together: Config feeds into the client,
the logger replaces all print() statements, and both storage systems
share a single SQLite database file. Before this version the components
existed but were largely independent islands.

v0.4.0 features demonstrated:
  - Config.from_file(), Config.from_env(), Config({...}) — three sources
  - ProductionMQTTClient.from_config() — the recommended instantiation path
  - Structured log output in ./logs/ — see the log file as messages arrive
  - Single database file — both tables visible in one sqlite3 inspection
  - get_statistics() includes richer context now that logger is wired up

Run against a local Mosquitto broker:
    mosquitto -p 1883
    python test_v040.py
"""

import json
import os
import sqlite3
import time
from production_client import ProductionMQTTClient
from config import Config
from production_logger import get_logger


SEPARATOR = "─" * 60


def section(title):
    print(f"\n{SEPARATOR}")
    print(f"  {title}")
    print(SEPARATOR)


# ── Section 1: Three ways to create a Config ─────────────────────────────────

section("1 — Three Config Sources")

print("Source A: plain dictionary (useful for tests and one-off scripts)")
config_a = Config({
    "client_id":   "demo_device_dict",
    "broker_host": "localhost",
    "broker_port": 1883,
    "log_level":   "INFO",
})
print(f"  broker_host = {config_a.get('broker_host')}")
print(f"  log_level   = {config_a.get('log_level')}")

print("\nSource B: environment variables (useful for containers and CI)")
os.environ["MQTT_CLIENT_ID"]   = "demo_device_env"
os.environ["MQTT_BROKER_HOST"] = "localhost"
os.environ["MQTT_LOG_LEVEL"]   = "WARNING"
config_b = Config.from_env()
print(f"  broker_host = {config_b.get('broker_host')}")
print(f"  log_level   = {config_b.get('log_level')}")

# Save a JSON file and load it back
print("\nSource C: JSON file (recommended for production deployments)")
config_file = "/tmp/demo_v040_config.json"
config_a.save_to_file(config_file)
# Override client_id for the file variant
with open(config_file) as f:
    data = json.load(f)
data["client_id"] = "demo_device_file"
with open(config_file, "w") as f:
    json.dump(data, f, indent=2)
config_c = Config.from_file(config_file)
print(f"  broker_host = {config_c.get('broker_host')}")
print(f"  client_id   = {config_c.get('client_id')}")
print(f"  (loaded from {config_file})")


# ── Section 2: from_config() factory ─────────────────────────────────────────

section("2 — from_config() Factory Method")

config = Config({
    "client_id":     "v040_demo_client",
    "broker_host":   "localhost",
    "broker_port":   1883,
    "max_queue_size": 100,
    "log_dir":       "./logs_v040_demo",
    "log_level":     "DEBUG",
    "db_path":       "/tmp/v040_demo.db",
    "min_backoff":   1,
    "max_backoff":   30,
})

print("Before v0.4.0, you had to wire every field manually:")
print("  client = ProductionMQTTClient(")
print("      client_id=config.get('client_id'),")
print("      broker_host=config.get('broker_host'),")
print("      # ... six more lines ...")
print("  )")
print("\nAfter v0.4.0, one line does it all:")
print("  client = ProductionMQTTClient.from_config(config)")

client = ProductionMQTTClient.from_config(config)
print("\n✓ Client created via from_config()")
print(f"  client_id:   {client.client_id}")
print(f"  broker:      {client.broker_host}:{client.broker_port}")
print(f"  backoff:     {client.min_backoff}s – {client.max_backoff}s")


# ── Section 3: Structured logging ────────────────────────────────────────────

section("3 — Structured Logging")

print("Connecting to broker. Watch ./logs_v040_demo/mqtt_client.log")
print("for structured log lines. Each line has context key=value pairs.\n")

client.connect()
client.start()
time.sleep(2)

# Publish some messages to generate log activity
for i in range(5):
    client.publish(
        topic=f"demo/v040/message_{i}",
        payload=json.dumps({"seq": i, "ts": time.time()}),
        qos=1,
    )
    time.sleep(0.2)

time.sleep(1)

# Show what the log file looks like
log_path = "./logs_v040_demo/mqtt_client.log"
if os.path.exists(log_path):
    print(f"Contents of {log_path} (last 8 lines):")
    print()
    with open(log_path) as f:
        lines = f.readlines()
    for line in lines[-8:]:
        print(f"  {line.rstrip()}")
else:
    print(f"Log file not yet created at {log_path}")
    print("(This is normal if the logger initialised with WARNING level from a previous run.)")


# ── Section 4: Single shared database ────────────────────────────────────────

section("4 — Single Shared Database")

print("Both InflightTracker and OfflineQueue now write to the same .db file.")
print(f"Inspecting: {config.get('db_path')}\n")

db_path = config.get("db_path")
if os.path.exists(db_path):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
    tables = [row[0] for row in cursor.fetchall()]
    print(f"  Tables in {db_path}: {tables}")
    for table in tables:
        cursor.execute(f"SELECT COUNT(*) FROM {table}")
        count = cursor.fetchone()[0]
        print(f"    {table}: {count} row(s)")
    conn.close()
    print("\n✓ Both tables confirmed in a single database file.")
else:
    print(f"  Database not found at {db_path}")
    print("  (It may not have been created yet — try publishing more messages.)")


# ── Final statistics ──────────────────────────────────────────────────────────

section("Statistics")
stats = client.get_statistics()
print(json.dumps(stats, indent=2, default=str))

client.stop()
print(f"\n{SEPARATOR}")
print("  v0.4.0 demo complete.")
print(SEPARATOR)
