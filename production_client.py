"""
production_client.py — MQTT client with offline queuing and inflight tracking.

Handles the failure modes that standard MQTT clients leave to the caller:
  - Automatic reconnection with exponential backoff
  - Offline message queuing when the broker is unreachable
  - Inflight tracking to guarantee redelivery after reconnection for QoS 1/2
  - Priority-based queue management when storage is constrained
  - Graceful degradation under limited resources

v0.3.0 changes:
  - _resend_inflight_messages() now removes stale tracker entries and inserts
    fresh ones with the new packet IDs assigned by paho after resend. Previously,
    a second disconnection before acknowledgment would silently lose those messages
    because the tracker no longer held a valid entry for them.
  - publish() now wraps client.publish() in a try/except. If the connection drops
    in the narrow window between the is_connected check and the actual send, the
    exception is caught and the message is routed to the offline queue instead of
    being silently discarded.
"""

import paho.mqtt.client as mqtt
import time
import threading
from inflight_tracker import InflightTracker
from offline_queue import OfflineQueue


class ProductionMQTTClient:
    """
    MQTT client with offline queuing and inflight tracking.

    Handles the failure modes that standard MQTT clients leave to the caller:
    - Automatic reconnection with exponential backoff
    - Offline message queuing when the broker is unreachable
    - Inflight tracking to guarantee redelivery after reconnection for QoS 1/2
    - Priority-based queue management when storage is constrained
    - Graceful degradation under limited resources
    """

    def __init__(self, client_id, broker_host="localhost", broker_port=1883,
                 max_queue_size=1000):
        self.client_id = client_id
        self.broker_host = broker_host
        self.broker_port = broker_port

        # Connection state tracking
        self.is_connected = False
        self.connection_lock = threading.Lock()

        # Reconnection backoff parameters
        self.min_backoff = 1
        self.max_backoff = 60
        self.current_backoff = self.min_backoff
        self.reconnect_in_progress = False

        # Initialize storage systems
        self.inflight_tracker = InflightTracker()
        self.offline_queue = OfflineQueue(max_size=max_queue_size)

        # Create MQTT client
        self.client = mqtt.Client(client_id=client_id, clean_session=False)

        # Set up callbacks
        self.client.on_connect = self._on_connect
        self.client.on_disconnect = self._on_disconnect
        self.client.on_publish = self._on_publish

        # Background thread for draining offline queue
        self.queue_drainer_running = False
        self.queue_drainer_thread = None

    def _on_connect(self, client, userdata, flags, rc):
        """Handle successful connection."""
        if rc == 0:
            with self.connection_lock:
                self.is_connected = True

            print(f"✓ Connected to broker (session_present={flags.get('session present', False)})")

            # Reset backoff on successful connection
            self.current_backoff = self.min_backoff
            self.reconnect_in_progress = False

            # First, resend any inflight messages from previous connection
            self._resend_inflight_messages()

            # Then, start draining the offline queue
            self._start_queue_drainer()
        else:
            print(f"✗ Connection failed with code {rc}")

    def _on_disconnect(self, client, userdata, rc):
        """Handle disconnection."""
        with self.connection_lock:
            self.is_connected = False

        self._stop_queue_drainer()

        if rc == 0:
            print("Disconnected cleanly from broker")
        else:
            print(f"✗ Unexpected disconnection (code {rc})")
            self._reconnect_with_backoff()

    def _on_publish(self, client, userdata, mid):
        """Handle message acknowledgment."""
        print(f"✓ Message {mid} acknowledged by broker")
        self.inflight_tracker.remove_message(mid)

    def _resend_inflight_messages(self):
        """
        Resend messages that were inflight during the previous connection.

        Each message gets a brand-new packet ID (mid) from paho when it is
        republished. We remove the stale tracker entry that used the old mid
        and immediately insert a fresh entry with the new mid. Without this
        step, a second disconnection before the broker sends its acknowledgment
        would leave those messages with no tracker entry, so they would never
        be resent again — a silent loss.

        This is the v0.3.0 fix for the resend-tracking gap.
        """
        messages = self.inflight_tracker.get_all_messages()

        if not messages:
            return

        print(f"⟳ Resending {len(messages)} inflight messages...")

        for msg in messages:
            info = self.client.publish(
                topic=msg["topic"],
                payload=msg["payload"],   # already bytes — no encoding needed
                qos=msg["qos"],
                retain=msg["retain"],
            )

            # Remove the old tracker entry (keyed on the previous connection's mid).
            self.inflight_tracker.remove_message(msg["packet_id"])

            # Re-register under the new mid so _on_publish() can clean it up
            # correctly when the broker acknowledges this resent copy.
            # Only QoS 1/2 messages receive acknowledgments; QoS 0 does not.
            if msg["qos"] > 0:
                self.inflight_tracker.add_message(
                    packet_id=info.mid,
                    topic=msg["topic"],
                    payload=msg["payload"],
                    qos=msg["qos"],
                    retain=msg["retain"],
                )

            print(f"  ⟳ Resent: {msg['topic']} (old_mid={msg['packet_id']}, new_mid={info.mid})")

    def _start_queue_drainer(self):
        """
        Start the background thread that drains the offline queue.

        Runs continuously while connected, pulling batches of messages from
        the offline queue and publishing them. Stops itself on disconnection
        and is restarted by _on_connect when the link comes back.
        """
        if self.queue_drainer_running:
            return

        self.queue_drainer_running = True

        def drain_queue():
            print("🔄 Queue drainer started")

            while self.queue_drainer_running and self.is_connected:
                messages = self.offline_queue.get_next_batch(batch_size=10)

                if not messages:
                    time.sleep(1)
                    continue

                print(f"📤 Draining {len(messages)} messages from offline queue...")

                for msg in messages:
                    if not self.is_connected:
                        print("⚠ Connection lost during queue drain")
                        break

                    info = self.client.publish(
                        topic=msg["topic"],
                        payload=msg["payload"],
                        qos=msg["qos"],
                        retain=msg["retain"],
                    )

                    if msg["qos"] > 0:
                        self.inflight_tracker.add_message(
                            packet_id=info.mid,
                            topic=msg["topic"],
                            payload=msg["payload"],
                            qos=msg["qos"],
                            retain=msg["retain"],
                        )

                    self.offline_queue.remove_message(msg["id"])
                    print(f"  📤 Published: {msg['topic']}")
                    time.sleep(0.1)

            print("🔄 Queue drainer stopped")

        self.queue_drainer_thread = threading.Thread(target=drain_queue, daemon=True)
        self.queue_drainer_thread.start()

    def _stop_queue_drainer(self):
        """Stop the queue drainer thread."""
        self.queue_drainer_running = False
        if self.queue_drainer_thread:
            self.queue_drainer_thread.join(timeout=2)

    def _reconnect_with_backoff(self):
        """Attempt to reconnect with exponential backoff."""
        if self.reconnect_in_progress:
            return

        self.reconnect_in_progress = True

        def reconnect_thread():
            while self.reconnect_in_progress:
                print(f"⟳ Waiting {self.current_backoff}s before reconnection attempt...")
                time.sleep(self.current_backoff)

                try:
                    print("⟳ Attempting to reconnect...")
                    self.client.reconnect()
                    break
                except Exception as e:
                    print(f"✗ Reconnection failed: {e}")
                    self.current_backoff = min(self.current_backoff * 2, self.max_backoff)

        thread = threading.Thread(target=reconnect_thread, daemon=True)
        thread.start()

    def connect(self):
        """Establish initial connection to the broker."""
        print(f"Connecting to {self.broker_host}:{self.broker_port}...")
        try:
            self.client.connect(self.broker_host, self.broker_port, keepalive=60)
        except Exception as e:
            print(f"Initial connection failed: {e}")
            self._reconnect_with_backoff()

    def publish(self, topic, payload, qos=0, retain=False, priority=1):
        """
        Publish a message, routing to the broker or the offline queue as appropriate.

        If connected, the message is published directly. For QoS 1/2, it is
        added to the inflight tracker until acknowledged. If disconnected, it
        goes into the offline queue and will be delivered once connectivity returns.

        v0.3.0: The actual client.publish() call is now wrapped in a try/except.
        The problem it solves is a race condition: between checking is_connected
        and calling client.publish(), the connection can drop. Without the try/except,
        that message would be neither published nor queued — silently lost. Now,
        if publish raises (because the socket is dead), we catch it, mark ourselves
        as disconnected, and route the message to the offline queue instead.
        """
        with self.connection_lock:
            connected = self.is_connected

        if connected:
            try:
                info = self.client.publish(topic, payload, qos, retain)

                # Track for QoS 1/2 — these need acknowledgment from the broker.
                if qos > 0:
                    self.inflight_tracker.add_message(
                        packet_id=info.mid,
                        topic=topic,
                        payload=payload,
                        qos=qos,
                        retain=retain,
                    )

                return info

            except Exception as e:
                # The connection dropped in the window between our check above
                # and the actual send. Update our state and fall through to the
                # offline queue so the message is not lost.
                print(f"⚠ publish() failed mid-flight ({e}) — routing to offline queue")
                with self.connection_lock:
                    self.is_connected = False

        # Either we were already disconnected, or the send above just failed.
        success = self.offline_queue.add_message(
            topic=topic,
            payload=payload,
            qos=qos,
            retain=retain,
            priority=priority,
        )

        if not success:
            print(f"⚠ Failed to queue message — queue full and couldn't make room")

        return None

    def get_statistics(self):
        """
        Return a snapshot of current client state.

        Useful for health checks and monitoring dashboards. Covers connection
        status, current backoff interval, offline queue depth, and the number
        of messages awaiting broker acknowledgment.
        """
        queue_stats = self.offline_queue.get_stats()
        inflight_count = self.inflight_tracker.count_messages()

        return {
            "connected": self.is_connected,
            "current_backoff_seconds": self.current_backoff,
            "offline_queue": queue_stats,
            "inflight_messages": inflight_count,
        }

    def start(self):
        """Start the network loop."""
        self.client.loop_start()

    def stop(self):
        """Stop the network loop and close connections."""
        self.reconnect_in_progress = False
        self._stop_queue_drainer()
        self.client.loop_stop()
        self.inflight_tracker.close()
        self.offline_queue.close()
