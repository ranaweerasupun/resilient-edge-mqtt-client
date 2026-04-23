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

