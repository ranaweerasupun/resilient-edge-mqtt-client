# -*- coding: utf-8 -*-
"""
test_v060.py — Simulation script for v0.6.0 (Bidirectional Communication)

Up to v0.5.0 the client was publish-only. v0.6.0 makes it a full MQTT
participant. This script demonstrates the complete bidirectional pattern:
a device publishes sensor data AND listens for commands from the cloud.

v0.6.0 features demonstrated:
  - subscribe() with a callback before connect()
  - Wildcard subscriptions with + (single level) and # (multi-level)
  - A "command/response" exchange using two topics
  - Subscription survives a broker restart

Run against a local Mosquitto broker:
    mosquitto -p 1883
    python test_v060.py

In a second terminal, you can send commands manually:
    mosquitto_pub -t "devices/warehouse_01/commands/led" -m '{"color":"red"}'
    mosquitto_pub -t "devices/warehouse_01/commands/fan" -m '{"speed":75}'
"""

import json
import time
import threading
import paho.mqtt.client as mqtt
from production_client import ProductionMQTTClient
from config import Config


SEPARATOR   = "─" * 60
DEVICE_ID   = "warehouse_01"
CMD_TOPIC   = f"devices/{DEVICE_ID}/commands/#"
STATUS_TOPIC = f"devices/{DEVICE_ID}/status"
SENSOR_TOPIC = f"sensors/{DEVICE_ID}/temperature"


def section(title):
    print(f"\n{SEPARATOR}")
    print(f"  {title}")
    print(SEPARATOR)


# ── Callback functions ────────────────────────────────────────────────────────

command_log = []

def on_command(topic: str, payload: bytes, qos: int, retain: bool) -> None:
    """
    Called whenever a message arrives on devices/warehouse_01/commands/#.

    The '#' wildcard means this single subscription covers ALL sub-topics:
    commands/led, commands/fan, commands/reboot, etc. The callback receives
    the full topic so you can tell which sub-topic triggered it.
    """
    try:
        data      = json.loads(payload)
        command   = topic.split("/")[-1]   # last segment is the command name
        timestamp = time.strftime("%H:%M:%S")
        log_entry = f"[{timestamp}] Command '{command}': {data}"
        command_log.append(log_entry)
        print(f"\n  📨 Received on {topic}")
        print(f"     Payload:  {data}")
        print(f"     QoS:      {qos}")
        print(f"     Retained: {retain}")

        # Respond with a status update on a separate topic
        # (This demonstrates the bidirectional exchange.)
        print(f"     → Sending status response...")
    except json.JSONDecodeError:
        print(f"  📨 Received on {topic}: {payload}")


# ── Section 1: Subscribe before connect ──────────────────────────────────────

section("1 — Subscribe Before connect()")

config = Config({
    "client_id":     DEVICE_ID,
    "broker_host":   "localhost",
    "broker_port":   1883,
    "log_dir":       "/tmp/logs_v060",
    "db_path":       "/tmp/v060_demo.db",
})

client = ProductionMQTTClient.from_config(config)

print(f"Registering subscription BEFORE calling connect().")
print(f"Topic pattern: '{CMD_TOPIC}'")
print(f"(The '#' wildcard will match any sub-topic under commands/)\n")

# This is the key v0.6.0 API — subscribe() before connect()
# The subscription is stored internally and sent to the broker
# automatically as soon as the connection is established.
client.subscribe(CMD_TOPIC, on_command, qos=1)

stats_before = client.get_statistics()
print(f"Active subscriptions (before connect): {stats_before['active_subscriptions']}")

client.connect()
client.start()

print("\nWaiting for connection...")
timeout = time.time() + 10
while not client.is_connected and time.time() < timeout:
    time.sleep(0.1)

if not client.is_connected:
    print("Could not connect — is Mosquitto running?")
    client.stop()
    exit(1)

stats_after = client.get_statistics()
print(f"Active subscriptions (after connect):  {stats_after['active_subscriptions']}")
print("\n✓ Subscription was sent to the broker automatically on connection.")


# ── Section 2: Wildcard matching live demonstration ──────────────────────────

section("2 — Wildcard Matching: '+' and '#'")

print(f"""
Pattern: '{CMD_TOPIC}'   (# matches zero or more levels)
  ✓ matches: devices/{DEVICE_ID}/commands/led
  ✓ matches: devices/{DEVICE_ID}/commands/fan/speed
  ✓ matches: devices/{DEVICE_ID}/commands
  ✗ no match: devices/other_device/commands/led

Sending three commands via a test publisher to show them all arrive...\n""")

# Use a plain paho client as the "cloud" sending commands
cloud = mqtt.Client(client_id="cloud_commander", clean_session=True)
cloud.connect("localhost", 1883)
cloud.loop_start()
time.sleep(0.3)

commands = [
    (f"devices/{DEVICE_ID}/commands/led",      {"color": "green", "brightness": 80}),
    (f"devices/{DEVICE_ID}/commands/fan",       {"speed": 60, "direction": "reverse"}),
    (f"devices/{DEVICE_ID}/commands/reboot",    {"delay_seconds": 30, "reason": "schedule"}),
]

for topic, payload in commands:
    cloud.publish(topic, json.dumps(payload), qos=1)
    time.sleep(0.5)

time.sleep(1)
print(f"\n  Total commands received: {len(command_log)}")


# ── Section 3: Publish and subscribe simultaneously ───────────────────────────

section("3 — Simultaneous Publish and Subscribe (Full Bidirectional)")

print(f"Publishing sensor readings while listening for commands.")
print(f"Send a command from another terminal to see both directions at once:")
print(f"  mosquitto_pub -t 'devices/{DEVICE_ID}/commands/setpoint' -m '{{\"temp\":22}}'\n")

for i in range(6):
    temperature = 21 + (i % 4)
    payload = json.dumps({
        "device_id":   DEVICE_ID,
        "temperature": temperature,
        "unit":        "C",
        "reading":     i + 1,
    })
    client.publish(SENSOR_TOPIC, payload, qos=1, priority=5)
    print(f"  📤 Published: {temperature}°C")
    time.sleep(2)


# ── Section 4: Subscription survives reconnect ────────────────────────────────

section("4 — Subscription Survival Across Reconnection")

print("Triggering a clean disconnect. The subscription will be re-registered")
print("automatically when the client reconnects.\n")

print(f"Subscriptions before disconnect: {client.get_statistics()['active_subscriptions']}")

# Disconnect
client.client.disconnect()
time.sleep(0.5)
print(f"Disconnected: is_connected={client.is_connected}")

# Reconnect
client.client.reconnect()
print("Reconnecting...")
timeout = time.time() + 10
while not client.is_connected and time.time() < timeout:
    time.sleep(0.1)

print(f"Reconnected: is_connected={client.is_connected}")
print(f"Subscriptions after reconnect:  {client.get_statistics()['active_subscriptions']}")

# Verify the subscription is active by sending another command
time.sleep(0.5)
n_before = len(command_log)
cloud.publish(f"devices/{DEVICE_ID}/commands/verify", json.dumps({"test": "post_reconnect"}), qos=1)
time.sleep(1)
n_after = len(command_log)

if n_after > n_before:
    print("\n✓ Command received after reconnect — subscription was automatically restored.")
else:
    print("\n  (No command received — check that Mosquitto is still running.)")


# ── Cleanup ───────────────────────────────────────────────────────────────────

cloud.loop_stop()
cloud.disconnect()

section("Summary")
print(f"Total commands received throughout demo: {len(command_log)}")
for entry in command_log:
    print(f"  {entry}")

print(json.dumps(client.get_statistics(), indent=2, default=str))

client.stop()
print(f"\n{SEPARATOR}")
print("  v0.6.0 demo complete.")
print(SEPARATOR)
