"""
test_production_client.py — unit tests for ProductionMQTTClient.

These tests focus on the client's routing logic and observable behaviour
without requiring a real MQTT broker. paho-mqtt is replaced with a
MagicMock so we can control exactly what the broker "does" and inspect
what the client sends to it.

The health check server is tested with a real HTTP request to a real
(but short-lived) server bound to a dynamically assigned free port,
so there are no hardcoded port numbers and no risk of conflicts.
"""

import http.client
import json
import time
from unittest.mock import MagicMock, patch, call

import pytest

import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__)))
from robmqtt.production_client import ProductionMQTTClient, _DEGRADED_THRESHOLD


def find_free_port():
    """Ask the OS for an available TCP port."""
    import socket
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("", 0))
        return s.getsockname()[1]
from robmqtt.config import Config


# ---------------------------------------------------------------------------
# Fixture: a client with paho mocked out
# ---------------------------------------------------------------------------

@pytest.fixture
def mock_client(tmp_path):
    """
    Provide a ProductionMQTTClient with paho replaced by a MagicMock.

    patching 'production_client.mqtt.Client' replaces the paho Client class
    at the point where production_client.py imports it. The mock instance
    is what self.client becomes inside ProductionMQTTClient.__init__.

    Using tmp_path ensures the SQLite database and log files go to a
    temporary directory that is cleaned up automatically after each test.
    """
    with patch("robmqtt.production_client.mqtt.Client") as MockClass:
        mock_paho = MagicMock()
        MockClass.return_value = mock_paho

        client = ProductionMQTTClient(
            client_id="test_device",
            db_path=str(tmp_path / "test.db"),
            log_dir=str(tmp_path / "logs"),
        )

        yield client, mock_paho

        # Teardown: stop() closes the database.
        # We call loop_stop on the mock rather than a real socket.
        client.stop()


# ---------------------------------------------------------------------------
# publish() routing logic
# ---------------------------------------------------------------------------

def test_publish_while_connected_calls_paho(mock_client):
    """
    When is_connected is True, publish() must call client.publish()
    on the paho client and not touch the offline queue.
    """
    client, mock_paho = mock_client
    mock_paho.publish.return_value = MagicMock(mid=1)
    client.is_connected = True

    client.publish("sensors/temp", b'{"v": 23}', qos=0)

    mock_paho.publish.assert_called_once_with("sensors/temp", b'{"v": 23}', 0, False)

    stats = client.get_statistics()
    assert stats["offline_queue"]["total_messages"] == 0


def test_publish_while_disconnected_routes_to_offline_queue(mock_client):
    """
    When is_connected is False, publish() must write to the offline queue
    without calling paho at all.
    """
    client, mock_paho = mock_client
    client.is_connected = False

    client.publish("sensors/temp", b'{"v": 23}', qos=1, priority=5)

    mock_paho.publish.assert_not_called()

    stats = client.get_statistics()
    assert stats["offline_queue"]["total_messages"] == 1


def test_publish_exception_routes_to_offline_queue(mock_client):
    """
    If paho.publish() raises an exception (the connection dropped in the
    narrow window between the is_connected check and the actual send),
    the message must be caught and routed to the offline queue.

    This is the race condition fix from v0.3.0.
    """
    client, mock_paho = mock_client
    mock_paho.publish.side_effect = ConnectionError("socket broken")
    client.is_connected = True

    client.publish("sensors/temp", b"data", qos=1)

    stats = client.get_statistics()
    assert stats["offline_queue"]["total_messages"] == 1
    # is_connected should have been set to False after the failure
    assert client.is_connected is False


def test_qos1_publish_adds_to_inflight_tracker(mock_client):
    """
    A successful QoS 1 publish must add the message to the inflight tracker
    so it can be resent if the connection drops before acknowledgment.
    """
    client, mock_paho = mock_client
    mock_paho.publish.return_value = MagicMock(mid=42)
    client.is_connected = True

    client.publish("sensors/temp", b"data", qos=1)

    stats = client.get_statistics()
    assert stats["inflight_messages"] == 1


def test_qos0_publish_does_not_add_to_inflight_tracker(mock_client):
    """
    QoS 0 (fire-and-forget) messages must not be tracked in the inflight
    tracker because the broker never sends an acknowledgment for them.
    """
    client, mock_paho = mock_client
    mock_paho.publish.return_value = MagicMock(mid=1)
    client.is_connected = True

    client.publish("sensors/temp", b"data", qos=0)

    stats = client.get_statistics()
    assert stats["inflight_messages"] == 0


# ---------------------------------------------------------------------------
# subscribe() / unsubscribe() registry behaviour
# ---------------------------------------------------------------------------

def test_subscribe_while_disconnected_stores_for_later(mock_client):
    """
    subscribe() called before connect() must store the subscription in the
    registry without sending a SUBSCRIBE packet to paho.
    """
    client, mock_paho = mock_client
    client.is_connected = False
    callback = MagicMock()

    client.subscribe("devices/+/commands", callback, qos=1)

    # Subscription is in the registry
    assert "devices/+/commands" in client._subscriptions
    # paho was NOT called (no connection yet)
    mock_paho.subscribe.assert_not_called()
    # Statistics reflect the pending subscription
    assert client.get_statistics()["active_subscriptions"] == 1


def test_subscribe_while_connected_sends_to_broker(mock_client):
    """
    subscribe() called when already connected must both update the registry
    and send a SUBSCRIBE packet to the broker immediately.
    """
    client, mock_paho = mock_client
    client.is_connected = True
    callback = MagicMock()

    client.subscribe("sensors/#", callback, qos=1)

    assert "sensors/#" in client._subscriptions
    mock_paho.subscribe.assert_called_once_with("sensors/#", 1)


def test_subscribe_twice_replaces_callback(mock_client):
    """
    Calling subscribe() a second time with the same topic must replace the
    old callback with the new one. Only one entry per topic in the registry.
    """
    client, mock_paho = mock_client
    client.is_connected = False
    first_cb  = MagicMock(name="first")
    second_cb = MagicMock(name="second")

    client.subscribe("t/topic", first_cb, qos=1)
    client.subscribe("t/topic", second_cb, qos=2)

    stored_callback, stored_qos = client._subscriptions["t/topic"]
    assert stored_callback is second_cb
    assert stored_qos == 2
    assert client.get_statistics()["active_subscriptions"] == 1


def test_unsubscribe_removes_from_registry(mock_client):
    """
    unsubscribe() must remove the topic from the registry so it is not
    restored on the next reconnection.
    """
    client, mock_paho = mock_client
    client.is_connected = False
    client.subscribe("sensors/#", MagicMock())

    client.unsubscribe("sensors/#")

    assert "sensors/#" not in client._subscriptions
    assert client.get_statistics()["active_subscriptions"] == 0


def test_unsubscribe_while_connected_sends_unsubscribe_packet(mock_client):
    """
    unsubscribe() while connected must send an UNSUBSCRIBE packet to the
    broker so it stops routing messages for that topic immediately.
    """
    client, mock_paho = mock_client
    client.is_connected = True
    client.subscribe("sensors/#", MagicMock(), qos=1)

    client.unsubscribe("sensors/#")

    mock_paho.unsubscribe.assert_called_once_with("sensors/#")


# ---------------------------------------------------------------------------
# get_statistics() shape
# ---------------------------------------------------------------------------

def test_get_statistics_has_expected_keys(mock_client):
    """get_statistics() must return all expected keys for monitoring tools."""
    client, _ = mock_client
    stats = client.get_statistics()

    expected_keys = {
        "connected",
        "current_backoff_seconds",
        "offline_queue",
        "inflight_messages",
        "tls_enabled",
        "active_subscriptions",
    }
    assert set(stats.keys()) == expected_keys


def test_get_statistics_reflects_tls_setting(tmp_path):
    """tls_enabled in statistics must match the constructor parameter."""
    with patch("robmqtt.production_client.mqtt.Client"):
        client = ProductionMQTTClient(
            client_id="tls_test",
            use_tls=True,
            db_path=str(tmp_path / "test.db"),
            log_dir=str(tmp_path / "logs"),
        )
        assert client.get_statistics()["tls_enabled"] is True
        client.stop()


# ---------------------------------------------------------------------------
# Health check server (v0.7.0)
# ---------------------------------------------------------------------------

@pytest.fixture
def health_check_client(tmp_path):
    """
    A client with the health check server enabled on a dynamically assigned
    free port. The server is started via start() and stopped via stop().
    """
    port = find_free_port()
    with patch("robmqtt.production_client.mqtt.Client"):
        client = ProductionMQTTClient(
            client_id="hc_test",
            db_path=str(tmp_path / "test.db"),
            log_dir=str(tmp_path / "logs"),
            enable_health_check=True,
            health_check_port=port,
        )
        client.start()
        time.sleep(0.15)  # give the server thread time to bind
        yield client, port
        client.stop()


def _get_health(port):
    """Make a GET /health request and return (status_code, body_dict)."""
    conn = http.client.HTTPConnection("localhost", port, timeout=3)
    conn.request("GET", "/health")
    resp = conn.getresponse()
    body = json.loads(resp.read().decode())
    conn.close()
    return resp.status, body


def test_health_endpoint_returns_unhealthy_when_disconnected(health_check_client):
    """
    When is_connected is False, GET /health must return HTTP 503 and
    status 'unhealthy'.
    """
    client, port = health_check_client
    client.is_connected = False

    status_code, body = _get_health(port)

    assert status_code == 503
    assert body["status"] == "unhealthy"
    assert body["client_id"] == "hc_test"
    assert "statistics" in body


def test_health_endpoint_returns_healthy_when_connected_and_queue_low(health_check_client):
    """
    When connected and the queue is below the degraded threshold,
    GET /health must return HTTP 200 and status 'healthy'.
    """
    client, port = health_check_client
    client.is_connected = True

    status_code, body = _get_health(port)

    assert status_code == 200
    assert body["status"] == "healthy"


def test_health_endpoint_returns_degraded_when_queue_pressure_high(health_check_client):
    """
    When connected but the offline queue is at or above the degraded threshold
    (80% by default), GET /health must return HTTP 200 and status 'degraded'.

    Degraded returns 200 (not 503) because the client is still alive and
    delivering messages — killing and restarting it is not the right response.
    """
    client, port = health_check_client
    client.is_connected = True

    # Fill the queue to just above the degraded threshold.
    # max_queue_size defaults to 1000, so we need >= 800 messages.
    # Use a small-capacity queue instead by adding messages directly.
    # We manipulate the offline_queue's max_size for this test.
    client.offline_queue.max_size = 10
    for i in range(9):  # 9/10 = 90% — above the 80% threshold
        client.offline_queue.add_message(f"t/{i}", b"x", qos=0)

    status_code, body = _get_health(port)

    assert status_code == 200
    assert body["status"] == "degraded"
    assert body["statistics"]["offline_queue"]["capacity_used_percent"] >= _DEGRADED_THRESHOLD


def test_health_endpoint_returns_404_for_unknown_paths(health_check_client):
    """Unknown paths must return HTTP 404 rather than a server error."""
    client, port = health_check_client
    conn = http.client.HTTPConnection("localhost", port, timeout=3)
    conn.request("GET", "/unknown/path")
    resp = conn.getresponse()
    conn.close()

    assert resp.status == 404


def test_health_endpoint_root_path_works(health_check_client):
    """GET / must be treated the same as GET /health."""
    client, port = health_check_client
    client.is_connected = False

    conn = http.client.HTTPConnection("localhost", port, timeout=3)
    conn.request("GET", "/")
    resp = conn.getresponse()
    body = json.loads(resp.read().decode())
    conn.close()

    assert resp.status == 503
    assert body["status"] == "unhealthy"
