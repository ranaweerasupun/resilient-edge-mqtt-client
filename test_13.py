# -*- coding: utf-8 -*-
"""
test_13.py — end-to-end simulation for the production MQTT client.

Publishes temperature readings every 5 seconds and prints queue statistics
every 10 readings. Specifically designed to demonstrate offline behaviour:
stop the broker while it runs, watch messages queue up, restart the broker,
watch the queue drain automatically.

v0.4.0: Updated to use Config and ProductionMQTTClient.from_config(), which
        is now the recommended way to create a client instance. The simulation
        logic itself is unchanged.
"""

import time
import json
from config import Config
from production_client import ProductionMQTTClient


def simulate_sensor_readings(client, duration_seconds=300):
    """
    Simulate a sensor publishing temperature readings at a fixed interval.

    Temperature cycles through a range to produce a mix of routine readings
    and critical alerts, so you can observe how the priority system behaves
    when the queue fills up. Statistics are printed every 10 readings.
    """
    print(f"\n{'='*60}")
    print("SIMULATING EDGE SENSOR DEVICE")
    print(f"{'='*60}\n")

    start_time = time.time()
    reading_number = 0

    while (time.time() - start_time) < duration_seconds:
        reading_number += 1

        # Temperature cycles 20–29 °C across readings
        temperature = 20 + (reading_number % 10)
        is_critical = temperature > 28
        priority = 10 if is_critical else 1

        payload = json.dumps({
            "reading_number": reading_number,
            "temperature":    temperature,
            "timestamp":      time.time(),
            "critical":       is_critical,
        })

        client.publish(
            topic="sensors/warehouse/temperature",
            payload=payload,
            qos=1,
            priority=priority,
        )

        if is_critical:
            print(f"🔥 CRITICAL: Temperature={temperature}°C (priority={priority})")
        else:
            print(f"📊 Reading {reading_number}: Temperature={temperature}°C")

        # Print statistics every 10 readings
        if reading_number % 10 == 0:
            stats = client.get_statistics()
            print(f"\n📈 CLIENT STATISTICS:")
            print(f"   Connected:            {stats['connected']}")
            print(f"   Offline queue:        {stats['offline_queue']['total_messages']} messages")
            print(f"   Queue capacity used:  {stats['offline_queue']['capacity_used_percent']:.1f}%")
            print(f"   Inflight messages:    {stats['inflight_messages']}")
            if stats["offline_queue"]["oldest_message_age_seconds"]:
                age_minutes = stats["offline_queue"]["oldest_message_age_seconds"] / 60
                print(f"   Oldest queued:        {age_minutes:.1f} minutes old")
            print()

        time.sleep(5)


# ------------------------------------------------------------------
# Create the client using Config and from_config()
#
# In v0.4.0, from_config() is the recommended way to instantiate the
# client. It reads all settings from the Config object — broker address,
# queue size, database path, log directory, backoff limits — rather than
# requiring you to pass each value as a separate constructor argument.
#
# If config.json doesn't exist yet, copy config.template.json first:
#   cp config.template.json config.json
#
# For this simulation we override max_queue_size to 50 to make the queue
# fill up quickly and demonstrate overflow handling. In real use, you'd
# leave this at whatever you set in config.json.
# ------------------------------------------------------------------
config = Config({
    "client_id":     "warehouse_sensor_01",
    "broker_host":   "localhost",
    "broker_port":   1883,
    "max_queue_size": 50,     # Small queue to demonstrate overflow
    "log_dir":       "./logs",
    "log_level":     "INFO",
    "db_path":       "./mqtt_client.db",
    "min_backoff":   1,
    "max_backoff":   60,
})

client = ProductionMQTTClient.from_config(config)

# Alternatively, load settings from a file:
#   config = Config.from_file("config.json")
#   client = ProductionMQTTClient.from_config(config)

# Or from environment variables:
#   config = Config.from_env()
#   client = ProductionMQTTClient.from_config(config)

client.connect()
client.start()

print("\nClient starting up... waiting for initial connection...")
time.sleep(3)

print("\n" + "="*60)
print("TEST INSTRUCTIONS:")
print("="*60)
print("\n1. Let it run for 30 seconds with broker running (normal operation)")
print("2. Stop the broker:    sudo systemctl stop mosquitto")
print("3. Watch messages queue up in the offline queue")
print("4. After 60 seconds, restart broker: sudo systemctl start mosquitto")
print("5. Watch the queue drain automatically")
print("\nPress Ctrl+C to stop the test\n")
print("="*60 + "\n")

try:
    simulate_sensor_readings(client, duration_seconds=300)
except KeyboardInterrupt:
    print("\n\nTest interrupted by user")

print("\n\nFinal statistics:")
final_stats = client.get_statistics()
print(json.dumps(final_stats, indent=2))

print("\nShutting down client...")
client.stop()
print("Test complete!")
