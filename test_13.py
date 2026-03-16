# -*- coding: utf-8 -*-
import time
import json
from production_client import ProductionMQTTClient

def simulate_sensor_readings(client, duration_seconds=60):
    """
    Simulate a sensor collecting data over time.
    
    This mimics a real edge device that continuously generates data
    regardless of network conditions. Some readings are marked as
    high priority (like alerts), while others are routine telemetry.
    """
    print(f"\n{'='*60}")
    print("SIMULATING EDGE SENSOR DEVICE")
    print(f"{'='*60}\n")
    
    start_time = time.time()
    reading_number = 0
    
    while (time.time() - start_time) < duration_seconds:
        reading_number += 1
        
        # Simulate temperature sensor reading
        temperature = 20 + (reading_number % 10)
        
        # Determine if this is a critical reading
        is_critical = temperature > 28
        priority = 10 if is_critical else 1
        
        # Create the message payload
        payload = json.dumps({
            'reading_number': reading_number,
            'temperature': temperature,
            'timestamp': time.time(),
            'critical': is_critical
        })
        
        # Publish with appropriate priority
        client.publish(
            topic='sensors/warehouse/temperature',
            payload=payload,
            qos=1,
            priority=priority
        )
        
        if is_critical:
            print(f"🔥 CRITICAL: Temperature={temperature}°C (priority={priority})")
        else:
            print(f"📊 Reading {reading_number}: Temperature={temperature}°C")
        
        # Show statistics every 10 readings
        if reading_number % 10 == 0:
            stats = client.get_statistics()
            print(f"\n📈 CLIENT STATISTICS:")
            print(f"   Connected: {stats['connected']}")
            print(f"   Offline queue: {stats['offline_queue']['total_messages']} messages")
            print(f"   Queue capacity used: {stats['offline_queue']['capacity_used_percent']:.1f}%")
            print(f"   Inflight messages: {stats['inflight_messages']}")
            if stats['offline_queue']['oldest_message_age_seconds']:
                age_minutes = stats['offline_queue']['oldest_message_age_seconds'] / 60
                print(f"   Oldest queued message: {age_minutes:.1f} minutes old")
            print()
        
        # Wait 5 seconds between readings
        time.sleep(5)

# Create the client with a modest queue size to demonstrate queue management
client = ProductionMQTTClient(
    client_id="warehouse_sensor_01",
    max_queue_size=50  # Small queue to demonstrate overflow handling
)

# Connect and start
client.connect()
client.start()

print("\nClient starting up... waiting for initial connection...")
time.sleep(3)

print("\n" + "="*60)
print("TEST INSTRUCTIONS:")
print("="*60)
print("\n1. Let it run for 30 seconds with broker running (normal operation)")
print("2. Stop the broker: sudo systemctl stop mosquitto")
print("3. Watch messages queue up in offline queue")
print("4. After 60 seconds, restart broker: sudo systemctl start mosquitto")
print("5. Watch the queue drain automatically")
print("\nPress Ctrl+C to stop the test\n")
print("="*60 + "\n")

try:
    simulate_sensor_readings(client, duration_seconds=300)  # Run for 5 minutes
except KeyboardInterrupt:
    print("\n\nTest interrupted by user")

print("\n\nFinal statistics:")
final_stats = client.get_statistics()
print(json.dumps(final_stats, indent=2))

print("\nShutting down client...")
client.stop()
print("Test complete!")