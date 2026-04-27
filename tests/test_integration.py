"""
test_integration.py — end-to-end tests requiring a live MQTT broker.

These tests are marked with @pytest.mark.integration and skipped automatically
when no broker is reachable on localhost:1883. To run them:

    1. Start a local Mosquitto broker:  mosquitto -p 1883
    2. Run integration tests only:      pytest -m integration -v
    3. Or run everything:               pytest -v

The integration tests verify behaviour that unit tests cannot: the full
publish → disconnect → reconnect → queue drain cycle, and the subscription
restore behaviour across reconnections. These are the scenarios that matter
most for an edge device in the field.
"""

import json
import time
import threading

import pytest
import paho.mqtt.client as mqtt

from conftest import broker_is_running
from robmqtt.production_client import ProductionMQTTClient
from robmqtt.config import Config


# All tests in this file are skipped when no broker is running.
pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        not broker_is_running(),
        reason="No MQTT broker found on localhost:1883. Start with: mosquitto -p 1883",
    ),
]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def make_test_client(tmp_path, client_id, **overrides):
    """Create a ProductionMQTTClient configured for local integration testing."""
    config = Config({
        "client_id":     client_id,
        "broker_host":   "localhost",
        "broker_port":   1883,
        "max_queue_size": 100,
        "log_dir":       str(tmp_path / "logs"),
        "log_level":     "WARNING",
        "db_path":       str(tmp_path / f"{client_id}.db"),
        **overrides,
    })
    return ProductionMQTTClient.from_config(config)


def wait_for(condition, timeout=5.0, interval=0.1):
    """Poll condition() until it returns True or timeout expires."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        if condition():
            return True
        time.sleep(interval)
    return False


# ---------------------------------------------------------------------------
# Integration tests
# ---------------------------------------------------------------------------

def test_client_connects_and_publishes(tmp_path):
    """
    The client must successfully connect to the broker and publish a message
    at QoS 1 with broker acknowledgment.
    """
    client = make_test_client(tmp_path, "it_connect_test")
    client.connect()
    client.start()

    assert wait_for(lambda: client.is_connected, timeout=5), "Client did not connect"

    client.publish("integration/test", b'{"test": "connect"}', qos=1)
    assert wait_for(lambda: client.get_statistics()["inflight_messages"] == 0, timeout=5), \
        "Message was not acknowledged by broker"

    client.stop()


def test_offline_queue_drains_after_reconnection(tmp_path):
    """
    Messages published while disconnected must be queued locally and then
    delivered to the broker automatically when connectivity is restored.

    This is the core delivery guarantee of the library and the most important
    integration test.
    """
    received = []

    # Set up a separate subscriber to collect messages
    subscriber = mqtt.Client(client_id="it_drain_subscriber", clean_session=True)
    subscriber.on_message = lambda c, u, m: received.append(m.payload)
    subscriber.connect("localhost", 1883)
    subscriber.subscribe("integration/drain_test", qos=1)
    subscriber.loop_start()

    time.sleep(0.5)  # give subscriber time to connect and subscribe

    client = make_test_client(tmp_path, "it_drain_publisher")
    client.connect()
    client.start()
    assert wait_for(lambda: client.is_connected, timeout=5)

    # Disconnect the client to simulate an outage
    client.client.disconnect()
    assert wait_for(lambda: not client.is_connected, timeout=3)

    # Publish messages while offline — these go to the queue
    for i in range(5):
        client.publish("integration/drain_test", f"offline_message_{i}".encode(), qos=1)

    assert client.get_statistics()["offline_queue"]["total_messages"] == 5

    # Reconnect — the queue drainer should replay all 5 messages
    client.client.reconnect()
    assert wait_for(lambda: client.is_connected, timeout=5)
    assert wait_for(lambda: client.get_statistics()["offline_queue"]["total_messages"] == 0, timeout=10), \
        "Offline queue did not drain after reconnection"

    # Give broker time to route messages to subscriber
    time.sleep(1.0)

    assert len(received) == 5, f"Expected 5 messages, received {len(received)}"

    subscriber.loop_stop()
    subscriber.disconnect()
    client.stop()


def test_subscriptions_restored_after_reconnection(tmp_path):
    """
    Subscriptions must be automatically re-registered with the broker after
    reconnection so that incoming messages continue to arrive.
    """
    received = []

    def on_message(topic, payload, qos, retain):
        received.append(payload)

    publisher = mqtt.Client(client_id="it_sub_publisher", clean_session=True)
    publisher.connect("localhost", 1883)
    publisher.loop_start()

    time.sleep(0.3)

    client = make_test_client(tmp_path, "it_sub_test")
    client.subscribe("integration/sub_restore/#", on_message, qos=1)
    client.connect()
    client.start()
    assert wait_for(lambda: client.is_connected, timeout=5)

    # Confirm subscription is active by receiving a message
    publisher.publish("integration/sub_restore/first", b"before_disconnect", qos=1)
    assert wait_for(lambda: len(received) >= 1, timeout=5), "No message received before disconnect"

    # Simulate disconnect and reconnect
    client.client.disconnect()
    assert wait_for(lambda: not client.is_connected, timeout=3)
    client.client.reconnect()
    assert wait_for(lambda: client.is_connected, timeout=5)

    # The subscription must have been restored — another message should arrive
    time.sleep(0.3)
    publisher.publish("integration/sub_restore/second", b"after_reconnect", qos=1)
    assert wait_for(lambda: len(received) >= 2, timeout=5), \
        "No message received after reconnect — subscription was not restored"

    publisher.loop_stop()
    publisher.disconnect()
    client.stop()
