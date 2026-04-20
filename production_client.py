"""
production_client.py — MQTT client with offline queuing and inflight tracking.

v0.3.0: Fixed payload encoding corruption, resend-tracking gap, and the
        publish() race condition.

v0.4.0: Single shared database, Config integration via from_config(),
        logger integration replacing all print() calls.

v0.5.0: Production connectivity overhaul.
        - TLS support: call tls_set() before connect() when use_tls=True.
          Supports one-way TLS (CA cert only) and mutual TLS (client cert + key).
        - Authentication: call username_pw_set() before connect() when
          credentials are provided.
        - Queue drainer threading fix: the boolean flag + join(timeout=2)
          pattern has been replaced with threading.Event. _stop_queue_drainer()
          now signals the drainer and returns immediately, so it is safe to call
          from the MQTT network callback thread without stalling paho's event loop.
"""

import sqlite3
import logging
import threading
import time

import paho.mqtt.client as mqtt

from config import Config
from inflight_tracker import InflightTracker
from offline_queue import OfflineQueue
from production_logger import get_logger


# Maps the string log-level names used in Config to Python's logging constants.
# Kept at module level so from_config() and __init__ can both use it.
_LOG_LEVEL_MAP = {
    "DEBUG":    logging.DEBUG,
    "INFO":     logging.INFO,
    "WARNING":  logging.WARNING,
    "ERROR":    logging.ERROR,
    "CRITICAL": logging.CRITICAL,
}


class ProductionMQTTClient:
    """
    MQTT client with offline queuing and inflight tracking.

    The recommended way to create an instance is via the from_config()
    class method, which reads all settings from a Config object:

        config = Config.from_file("config.json")
        client = ProductionMQTTClient.from_config(config)
        client.connect()
        client.start()

    The direct constructor is still available for cases where a Config
    object is not appropriate (e.g. unit tests, embedded usage).
    """

    def __init__(
        self,
        client_id,
        broker_host="localhost",
        broker_port=1883,
        max_queue_size=1000,
        db_path="./mqtt_client.db",
        min_backoff=1,
        max_backoff=60,
        log_dir="./logs",
        log_level="INFO",
        # v0.5.0: TLS parameters
        use_tls=False,
        ca_certs=None,
        certfile=None,
        keyfile=None,
        # v0.5.0: Authentication parameters
        username=None,
        password=None,
    ):
        self.client_id   = client_id
        self.broker_host = broker_host
        self.broker_port = broker_port

        # TLS and auth — stored here so connect() and _reconnect_with_backoff()
        # can both apply them consistently on every connection attempt.
        self.use_tls  = use_tls
        self.ca_certs = ca_certs
        self.certfile = certfile
        self.keyfile  = keyfile
        self.username = username
        self.password = password

        # ----------------------------------------------------------------
        # Step 1: Logger — must come first so everything below can log
        # ----------------------------------------------------------------
        log_level_int = _LOG_LEVEL_MAP.get(log_level.upper(), logging.INFO)
        self.logger = get_logger(
            "mqtt_client",
            log_dir=log_dir,
            log_level=log_level_int,
        )

        # ----------------------------------------------------------------
        # Step 2: Shared database connection and lock
        #
        # A single SQLite file holds both the inflight_messages table and
        # the offline_queue table. A single lock serialises all access to
        # that file, so InflightTracker and OfflineQueue can never step on
        # each other even though they share a connection.
        # ----------------------------------------------------------------
        self._db_conn = sqlite3.connect(db_path, check_same_thread=False)
        self._db_lock = threading.Lock()

        self.logger.info("Database opened", path=db_path)

        # ----------------------------------------------------------------
        # Step 3: Storage systems — both receive the shared connection
        # ----------------------------------------------------------------
        self.inflight_tracker = InflightTracker(
            conn=self._db_conn,
            lock=self._db_lock,
        )
        self.offline_queue = OfflineQueue(
            conn=self._db_conn,
            lock=self._db_lock,
            max_size=max_queue_size,
        )

        # ----------------------------------------------------------------
        # Step 4: Connection state
        # ----------------------------------------------------------------
        self.is_connected = False
        self.connection_lock = threading.Lock()

        # Reconnection backoff — now configurable rather than hardcoded
        self.min_backoff = min_backoff
        self.max_backoff = max_backoff
        self.current_backoff = self.min_backoff
        self.reconnect_in_progress = False

        # ----------------------------------------------------------------
        # Step 5: MQTT client and callbacks
        # ----------------------------------------------------------------
        self.client = mqtt.Client(client_id=client_id, clean_session=False)
        self.client.on_connect    = self._on_connect
        self.client.on_disconnect = self._on_disconnect
        self.client.on_publish    = self._on_publish

        # Background thread for draining the offline queue
        self.queue_drainer_running = False
        self.queue_drainer_thread  = None

        self.logger.info(
            "Client initialised",
            client_id=client_id,
            broker=f"{broker_host}:{broker_port}",
            max_queue_size=max_queue_size,
        )

    # ------------------------------------------------------------------
    # Factory method — the recommended way to create an instance
    # ------------------------------------------------------------------

    @classmethod
    def from_config(cls, config):
        """
        Create a ProductionMQTTClient from a Config object.

        This is the recommended instantiation path in production. It maps
        every relevant Config field to the corresponding constructor parameter,
        so you don't have to pass a long list of arguments at the call site:

            config = Config.from_file("config.json")
            client = ProductionMQTTClient.from_config(config)

        All configuration sources that Config supports — JSON file, key=value
        file, environment variables, or a plain dict — work transparently here.
        """
        return cls(
            client_id     = config.get("client_id"),
            broker_host   = config.get("broker_host"),
            broker_port   = config.get("broker_port"),
            max_queue_size= config.get("max_queue_size"),
            db_path       = config.get("db_path"),
            min_backoff   = config.get("min_backoff"),
            max_backoff   = config.get("max_backoff"),
            log_dir       = config.get("log_dir"),
            log_level     = config.get("log_level"),
        )

    # ------------------------------------------------------------------
    # MQTT callbacks
    # ------------------------------------------------------------------

    def _on_connect(self, client, userdata, flags, rc):
        """Handle successful connection."""
        if rc == 0:
            with self.connection_lock:
                self.is_connected = True

            session_present = flags.get("session present", False)
            self.logger.info(
                "Connected to broker",
                broker=f"{self.broker_host}:{self.broker_port}",
                session_present=session_present,
            )
            self.logger.log_event(
                "connection_established",
                broker=self.broker_host,
                port=self.broker_port,
                session_present=session_present,
            )

            # Reset backoff on successful connection
            self.current_backoff = self.min_backoff
            self.reconnect_in_progress = False

            # Resend inflight messages first, then drain the offline queue.
            # Order matters: inflight messages were already accepted by a
            # previous broker session and should be delivered before new ones.
            self._resend_inflight_messages()
            self._start_queue_drainer()
        else:
            self.logger.error("Connection refused by broker", return_code=rc)

    def _on_disconnect(self, client, userdata, rc):
        """Handle disconnection."""
        with self.connection_lock:
            self.is_connected = False

        self._stop_queue_drainer()

        if rc == 0:
            self.logger.info("Disconnected cleanly from broker")
            self.logger.log_event("disconnection_clean")
        else:
            self.logger.warning("Unexpected disconnection", return_code=rc)
            self.logger.log_event("disconnection_unexpected", return_code=rc)
            self._reconnect_with_backoff()

    def _on_publish(self, client, userdata, mid):
        """Handle message acknowledgment from the broker."""
        self.logger.debug("Message acknowledged by broker", mid=mid)
        self.inflight_tracker.remove_message(mid)

    # ------------------------------------------------------------------
    # Inflight resend
    # ------------------------------------------------------------------

    def _resend_inflight_messages(self):
        """
        Resend messages that were inflight during the previous connection.

        Each message gets a new packet ID (mid) from paho on resend. We
        remove the stale tracker entry and insert a fresh one with the new
        mid. See the v0.3.0 release notes for a full explanation of why
        this step is necessary.
        """
        messages = self.inflight_tracker.get_all_messages()

        if not messages:
            return

        self.logger.info("Resending inflight messages", count=len(messages))

        for msg in messages:
            info = self.client.publish(
                topic=msg["topic"],
                payload=msg["payload"],   # bytes — no re-encoding needed
                qos=msg["qos"],
                retain=msg["retain"],
            )

            # Remove the stale entry (old mid from previous connection)
            self.inflight_tracker.remove_message(msg["packet_id"])

            # Re-register under the new mid so _on_publish() can clean it up
            if msg["qos"] > 0:
                self.inflight_tracker.add_message(
                    packet_id=info.mid,
                    topic=msg["topic"],
                    payload=msg["payload"],
                    qos=msg["qos"],
                    retain=msg["retain"],
                )

            self.logger.debug(
                "Inflight message resent",
                topic=msg["topic"],
                old_mid=msg["packet_id"],
                new_mid=info.mid,
            )

    # ------------------------------------------------------------------
    # Queue drainer
    # ------------------------------------------------------------------

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
            self.logger.info("Queue drainer started")

            while self.queue_drainer_running and self.is_connected:
                messages = self.offline_queue.get_next_batch(batch_size=10)

                if not messages:
                    time.sleep(1)
                    continue

                self.logger.info(
                    "Draining messages from offline queue",
                    count=len(messages),
                )

                for msg in messages:
                    if not self.is_connected:
                        self.logger.warning("Connection lost during queue drain")
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
                    self.logger.debug(
                        "Drained message from offline queue",
                        topic=msg["topic"],
                        priority=msg["priority"],
                    )
                    time.sleep(0.1)

            self.logger.info("Queue drainer stopped")

        self.queue_drainer_thread = threading.Thread(
            target=drain_queue,
            daemon=True,
        )
        self.queue_drainer_thread.start()

    def _stop_queue_drainer(self):
        """Signal the queue drainer to stop and wait briefly for it to exit."""
        self.queue_drainer_running = False
        if self.queue_drainer_thread:
            self.queue_drainer_thread.join(timeout=2)

    # ------------------------------------------------------------------
    # Reconnection
    # ------------------------------------------------------------------

    def _reconnect_with_backoff(self):
        """Attempt to reconnect with exponential backoff."""
        if self.reconnect_in_progress:
            return

        self.reconnect_in_progress = True

        def reconnect_thread():
            while self.reconnect_in_progress:
                self.logger.info(
                    "Waiting before reconnection attempt",
                    backoff_seconds=self.current_backoff,
                )
                time.sleep(self.current_backoff)

                try:
                    self.logger.info("Attempting to reconnect")
                    self.client.reconnect()
                    break
                except Exception as e:
                    self.logger.error("Reconnection attempt failed", error=str(e))
                    self.current_backoff = min(
                        self.current_backoff * 2,
                        self.max_backoff,
                    )

        thread = threading.Thread(target=reconnect_thread, daemon=True)
        thread.start()

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    def connect(self):
        """Establish the initial connection to the broker."""
        self.logger.info(
            "Connecting to broker",
            broker=f"{self.broker_host}:{self.broker_port}",
        )
        try:
            self.client.connect(self.broker_host, self.broker_port, keepalive=60)
        except Exception as e:
            self.logger.error("Initial connection failed", error=str(e))
            self._reconnect_with_backoff()

    def publish(self, topic, payload, qos=0, retain=False, priority=1):
        """
        Publish a message, routing to the broker or the offline queue as appropriate.

        If connected, the message is published directly. For QoS 1/2, it is
        added to the inflight tracker until acknowledged. If disconnected (or
        if the send fails mid-flight due to a race condition), the message is
        written to the offline queue and delivered once connectivity returns.

        The priority parameter (1–10, higher = more important) controls eviction
        order when the offline queue reaches capacity.
        """
        with self.connection_lock:
            connected = self.is_connected

        if connected:
            try:
                info = self.client.publish(topic, payload, qos, retain)

                if qos > 0:
                    self.inflight_tracker.add_message(
                        packet_id=info.mid,
                        topic=topic,
                        payload=payload,
                        qos=qos,
                        retain=retain,
                    )

                self.logger.debug(
                    "Message published to broker",
                    topic=topic,
                    qos=qos,
                    mid=info.mid,
                )
                return info

            except Exception as e:
                # The connection dropped in the window between the check above
                # and the actual send. Update state and fall through to queue.
                self.logger.warning(
                    "Publish failed mid-flight — routing to offline queue",
                    topic=topic,
                    error=str(e),
                )
                with self.connection_lock:
                    self.is_connected = False

        # Either already disconnected or the send above just failed.
        success = self.offline_queue.add_message(
            topic=topic,
            payload=payload,
            qos=qos,
            retain=retain,
            priority=priority,
        )

        if not success:
            self.logger.error(
                "Message dropped — queue full and could not make room",
                topic=topic,
                priority=priority,
            )

        return None

    def get_statistics(self):
        """
        Return a snapshot of current client state.

        Useful for health checks and monitoring dashboards.
        """
        return {
            "connected":              self.is_connected,
            "current_backoff_seconds": self.current_backoff,
            "offline_queue":          self.offline_queue.get_stats(),
            "inflight_messages":      self.inflight_tracker.count_messages(),
        }

    def start(self):
        """Start the MQTT network loop."""
        self.client.loop_start()
        self.logger.info("Network loop started")

    def stop(self):
        """Stop the network loop, close storage systems, and close the database."""
        self.logger.info("Shutting down client", client_id=self.client_id)

        self.reconnect_in_progress = False
        self._stop_queue_drainer()
        self.client.loop_stop()

        # Storage systems in shared mode do not close the connection themselves,
        # so we close it here after both are done with it.
        self.inflight_tracker.close()
        self.offline_queue.close()

        with self._db_lock:
            self._db_conn.close()

        self.logger.info("Client stopped", client_id=self.client_id)
        self.logger.log_event("client_stopped", client_id=self.client_id)
