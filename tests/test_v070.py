# -*- coding: utf-8 -*-
"""
test_v070.py — Simulation script for v0.7.0 (Observability)

v0.7.0 added a health check HTTP endpoint so external systems — Docker,
Kubernetes, Nagios, a simple cron job — can ask "is this client healthy?"
without having to parse log files or SSH into the device.

The endpoint returns one of three statuses:
  "healthy"   HTTP 200 — connected, queue below 80%
  "degraded"  HTTP 200 — connected, but queue pressure is high
  "unhealthy" HTTP 503 — not connected to the broker

v0.7.0 features demonstrated:
  - Health check server starting with start()
  - Querying /health in all three states
  - How monitoring tools would interpret the responses

Run against a local Mosquitto broker:
    mosquitto -p 1883
    python test_v070.py
"""

import json
import time
import http.client
from production_client import ProductionMQTTClient
from config import Config


SEPARATOR          = "─" * 60
HEALTH_CHECK_PORT  = 18070   # using a non-standard port to avoid conflicts


def section(title):
    print(f"\n{SEPARATOR}")
    print(f"  {title}")
    print(SEPARATOR)


def query_health(port=HEALTH_CHECK_PORT):
    """Make a GET /health request and return (http_status, body_dict)."""
    try:
        conn = http.client.HTTPConnection("localhost", port, timeout=3)
        conn.request("GET", "/health")
        resp = conn.getresponse()
        body = json.loads(resp.read().decode())
        conn.close()
        return resp.status, body
    except Exception as e:
        return None, {"error": str(e)}


def show_health(label: str):
    """Query the health endpoint and print a formatted summary."""
    http_status, body = query_health()
    if http_status is None:
        print(f"  [{label}] ERROR: could not reach health endpoint")
        return

    status   = body.get("status", "unknown")
    q        = body.get("statistics", {}).get("offline_queue", {})
    inflight = body.get("statistics", {}).get("inflight_messages", "?")
    subs     = body.get("statistics", {}).get("active_subscriptions", "?")
    q_pct    = q.get("capacity_used_percent", 0)

    status_icon = {"healthy": "✓", "degraded": "⚠", "unhealthy": "✗"}.get(status, "?")
    print(f"  [{label}]")
    print(f"    HTTP {http_status}  status={status}  {status_icon}")
    print(f"    Queue: {q.get('total_messages', '?')} msgs ({q_pct:.1f}% full)")
    print(f"    Inflight: {inflight}   Subscriptions: {subs}")

# ── Setup ─────────────────────────────────────────────────────────────────────

section("Setup — Client with Health Check Enabled")

config = Config({
    "client_id":            "v070_health_demo",
    "broker_host":          "localhost",
    "broker_port":          1883,
    "max_queue_size":       20,    # small queue so we can fill it quickly
    "log_dir":              "/tmp/logs_v070",
    "db_path":              "/tmp/v070_demo.db",
    "enable_health_check":  True,
    "health_check_port":    HEALTH_CHECK_PORT,
})

client = ProductionMQTTClient.from_config(config)
client.connect()
client.start()   # <-- this is what starts the health check server

# ── State 1: Healthy ──────────────────────────────────────────────────────────

show_health("before publish")

# ── State 2: Degraded ─────────────────────────────────────────────────────────


# Temporarily disconnect so messages go to the offline queue
client.is_connected = False
for i in range(17):  # 17 of 20 = 85% — above the 80% threshold
    client.publish(f"sensors/flood/{i}", f"reading_{i}".encode(), qos=1)
client.is_connected = True

show_health("queue at 85%")

# Drain the queue for the next demonstration
client.offline_queue.clear()
time.sleep(0.5)


# ── State 3: Unhealthy ───────────────────────────────────────────────────────

client.is_connected = False
show_health("simulated disconnect")

# Restore for the polling demo
client.is_connected = True
time.sleep(0.5)


# ── Section 4: Polling loop (what a monitoring script would do) ───────────────

for i in range(5):
    http_status, body = query_health()
    status = body.get("status", "unknown")
    q_pct  = body.get("statistics", {}).get("offline_queue", {}).get("capacity_used_percent", 0)
    connected = body.get("statistics", {}).get("connected", False)
    icon   = {"healthy": "✓", "degraded": "⚠", "unhealthy": "✗"}.get(status, "?")
    print(f"  Poll {i+1}/5  HTTP {http_status}  {icon} {status:<10}  "
          f"connected={connected}  queue={q_pct:.0f}%")
    time.sleep(2)

