# -*- coding: utf-8 -*-
"""
test_v020.py — Simulation script for v0.2.0 (Initial Release)

This is the original baseline simulation. It models a warehouse temperature
sensor that publishes a reading every 5 seconds. The key thing to observe
is the offline queue behaviour: stop the broker while it runs, watch messages
pile up, restart the broker, and watch the queue drain automatically.

v0.2.0 features demonstrated:
  - Basic publish with offline queuing
  - Priority-based eviction (critical readings get priority 10)
  - Exponential backoff on reconnection
  - Queue statistics via get_statistics()

Run against a local Mosquitto broker:
    mosquitto -p 1883
    python test_v020.py
"""

import time
import json
from production_client import ProductionMQTTClient


def simulate_sensor(client, duration_seconds=300):
    print(f"\n{'='*60}")
    print("  v0.2.0 — TEMPERATURE SENSOR SIMULATION")
    print(f"{'='*60}\n")
    print("Instructions:")
    print("  1. Let it run for 30 seconds (normal operation)")
    print("  2. Stop the broker:    sudo systemctl stop mosquitto")
    print("  3. Watch messages queue up")
    print("  4. Restart the broker: sudo systemctl start mosquitto")
    print("  5. Watch the offline queue drain\n")
    print(f"{'='*60}\n")

    start      = time.time()
    reading_no = 0

    while (time.time() - start) < duration_seconds:
        reading_no += 1
        temperature = 20 + (reading_no % 10)   # cycles 20–29 °C
        is_critical = temperature > 28
        priority    = 10 if is_critical else 1

        payload = json.dumps({
            "reading": reading_no,
            "temperature": temperature,
            "critical": is_critical,
            "timestamp": time.time(),
        })

        client.publish(
            topic="sensors/warehouse/temperature",
            payload=payload,
            qos=1,
            priority=priority,
        )

        label = "🔥 CRITICAL" if is_critical else f"   Reading {reading_no:03d}"
        print(f"{label}: {temperature}°C  (priority={priority})")

        # Print statistics every 10 readings
        if reading_no % 10 == 0:
            stats = client.get_statistics()
            q     = stats["offline_queue"]
            print(f"\n  ┌─ Statistics ─────────────────────────────┐")
            print(f"  │ Connected:         {str(stats['connected']):5}                  │")
            print(f"  │ Queue depth:       {q['total_messages']:5} messages            │")
            print(f"  │ Queue used:        {q['capacity_used_percent']:5.1f}%                │")
            print(f"  │ Inflight (unsent): {stats['inflight_messages']:5} messages            │")
            if q["oldest_message_age_seconds"]:
                age = q["oldest_message_age_seconds"] / 60
                print(f"  │ Oldest queued:     {age:5.1f} minutes               │")
            print(f"  └───────────────────────────────────────────┘\n")

        time.sleep(5)


# ── Entry point ──────────────────────────────────────────────────────────────

# v0.2.0 uses the direct constructor — Config integration came in v0.4.0
client = ProductionMQTTClient(
    client_id="warehouse_sensor_01",
    broker_host="localhost",
    broker_port=1883,
    max_queue_size=50,   # small queue to make eviction visible
)

client.connect()
client.start()

print("\nWaiting for initial connection...")
time.sleep(3)

try:
    simulate_sensor(client, duration_seconds=300)
except KeyboardInterrupt:
    print("\n\nInterrupted by user.")

print("\nFinal statistics:")
print(json.dumps(client.get_statistics(), indent=2))

print("\nShutting down...")
client.stop()
print("Done.")
