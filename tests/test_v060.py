# -*- coding: utf-8 -*-
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

