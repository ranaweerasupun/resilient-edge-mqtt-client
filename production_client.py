"""
production_client.py — MQTT client with offline queuing, inflight tracking,
TLS, authentication, bidirectional communication, and a health check endpoint.

v0.3.0: Fixed payload encoding corruption, resend-tracking gap, publish() race condition.
v0.4.0: Single shared database, Config integration, logger integration.
v0.5.0: TLS and authentication. threading.Event for queue drainer.
v0.6.0: Subscribe support with wildcard matching and automatic restoration.
v0.7.0: Health check HTTP server.
        - A minimal HTTP server starts in a background daemon thread when
          enable_health_check=True (set in config or constructor).
        - GET /health returns a JSON body with three possible statuses:
            "healthy"  (HTTP 200) — connected, queue below 80% capacity.
            "degraded" (HTTP 200) — connected, but queue at 80%+ capacity.
            "unhealthy"(HTTP 503) — not connected to the broker.
        - The full get_statistics() snapshot is always included in the body
          so monitoring tools have the context they need.
        - Health check requests are logged at DEBUG level via the structured
          logger rather than printed to stderr.
        - The server is started in start() and stopped in stop(), consistent
          with the rest of the client lifecycle.
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


_LOG_LEVEL_MAP = {
    "DEBUG":    logging.DEBUG,
    "INFO":     logging.INFO,
    "WARNING":  logging.WARNING,
    "ERROR":    logging.ERROR,
    "CRITICAL": logging.CRITICAL,
}


class ProductionMQTTClient:
    """
    MQTT client with offline queuing, inflight tracking, TLS, authentication,
    and bidirectional communication via subscribe/unsubscribe.

    Recommended instantiation:

        config = Config.from_file("config.json")
        client = ProductionMQTTClient.from_config(config)

        def on_command(topic, payload, qos, retain):
            print(f"Command on {topic}: {payload}")

        client.subscribe("devices/my_device/commands/#", on_command)
        client.connect()
        client.start()
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

        self.min_backoff     = min_backoff
        self.max_backoff     = max_backoff
        self.current_backoff = self.min_backoff
        self.reconnect_in_progress = False

        # ----------------------------------------------------------------
        # Step 5: MQTT client and callbacks
        # ----------------------------------------------------------------
        self.client = mqtt.Client(client_id=client_id, clean_session=False)
        self.client.on_connect    = self._on_connect
        self.client.on_disconnect = self._on_disconnect
        self.client.on_publish    = self._on_publish

        # ----------------------------------------------------------------
        # Step 6: Queue drainer state (v0.5.0: threading.Event replaces
        # the plain boolean + join pattern)
        #
        # _stop_drainer_event is the signal mechanism. The drainer thread
        # waits on it with a timeout instead of sleeping unconditionally.
        # When the event is set, the drainer wakes immediately and exits —
        # regardless of how long it was planning to sleep.
        #
        # This means _stop_queue_drainer() can set the event and return
        # immediately, without blocking the calling thread. That matters
        # because _on_disconnect() runs on paho's network thread, and
        # blocking that thread delays all further paho event processing.
        # ----------------------------------------------------------------
        self.queue_drainer_running = False
        self.queue_drainer_thread  = None
        self._stop_drainer_event   = threading.Event()

        self.logger.info(
            "Client initialised",
            client_id=client_id,
            broker=f"{broker_host}:{broker_port}",
            tls=use_tls,
            auth=username is not None,
        )

    # ------------------------------------------------------------------
    # Factory method
    # ------------------------------------------------------------------

    @classmethod
    def from_config(cls, config):
        """
        Create a ProductionMQTTClient from a Config object.

        This is the recommended instantiation path in production. All settings —
        including TLS and authentication credentials added in v0.5.0 — are read
        from the Config object automatically.
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
            # v0.5.0: TLS and authentication
            use_tls       = config.get("use_tls"),
            ca_certs      = config.get("ca_certs"),
            certfile      = config.get("certfile"),
            keyfile       = config.get("keyfile"),
            username      = config.get("username"),
            password      = config.get("password"),
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
                tls=self.use_tls,
                session_present=session_present,
            )
            self.logger.log_event(
                "connection_established",
                broker=self.broker_host,
                port=self.broker_port,
                tls=self.use_tls,
                session_present=session_present,
            )

            self.current_backoff = self.min_backoff
            self.reconnect_in_progress = False

            self._resend_inflight_messages()
            self._start_queue_drainer()
        else:
            self.logger.error("Connection refused by broker", return_code=rc)

    def _on_disconnect(self, client, userdata, rc):
        """
        Handle disconnection.

        This callback runs on paho's network thread. It must return quickly —
        any blocking call here delays paho's ability to process further events.
        _stop_queue_drainer() uses threading.Event so it signals the drainer
        and returns immediately rather than waiting for the drainer to exit.
        """
        with self.connection_lock:
            self.is_connected = False

        # Non-blocking — sets the event and returns immediately
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

        Each message gets a new packet ID on resend. The stale tracker entry
        is removed and a fresh one is inserted with the new ID. See the v0.3.0
        release notes for a full explanation.
        """
        messages = self.inflight_tracker.get_all_messages()

        if not messages:
            return

        self.logger.info("Resending inflight messages", count=len(messages))

        for msg in messages:
            info = self.client.publish(
                topic=msg["topic"],
                payload=msg["payload"],
                qos=msg["qos"],
                retain=msg["retain"],
            )

            self.inflight_tracker.remove_message(msg["packet_id"])

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
    # Queue drainer (v0.5.0: threading.Event-based stop mechanism)
    # ------------------------------------------------------------------

    def _start_queue_drainer(self):
        """
        Start the background thread that drains the offline queue.

        The stop event is cleared before the thread starts so that a fresh
        drainer is not immediately told to stop because of a previous signal.
        """
        if self.queue_drainer_running:
            return

        self.queue_drainer_running = True

        # Always clear the event before starting a new drainer thread.
        # If the previous drainer was stopped by setting this event, the
        # event is still set from that time. A new drainer thread that
        # checked the event immediately would exit before doing any work.
        self._stop_drainer_event.clear()

        def drain_queue():
            self.logger.info("Queue drainer started")

            while not self._stop_drainer_event.is_set() and self.is_connected:
                messages = self.offline_queue.get_next_batch(batch_size=10)

                if not messages:
                    # Queue is empty. Wait up to 1 second before checking again,
                    # but wake immediately if the stop event is set.
                    # This replaces the old unconditional time.sleep(1).
                    self._stop_drainer_event.wait(timeout=1)
                    continue

                self.logger.info(
                    "Draining messages from offline queue",
                    count=len(messages),
                )

                for msg in messages:
                    # Check the stop event before each message so we can exit
                    # mid-batch promptly rather than finishing the whole batch
                    # after a stop signal has been issued.
                    if self._stop_drainer_event.is_set() or not self.is_connected:
                        self.logger.warning("Queue drain interrupted")
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

                    # Interruptible inter-message delay. Replaces time.sleep(0.1).
                    # If a stop signal arrives during this wait, we exit cleanly
                    # on the next loop iteration rather than sleeping through it.
                    self._stop_drainer_event.wait(timeout=0.1)

            self.queue_drainer_running = False
            self.logger.info("Queue drainer stopped")

        self.queue_drainer_thread = threading.Thread(
            target=drain_queue,
            daemon=True,
        )
        self.queue_drainer_thread.start()

    def _stop_queue_drainer(self):
        """
        Signal the queue drainer to stop. Returns immediately.

        This is safe to call from paho's network callback thread because it
        does not block. Setting an Event is an O(1) operation that never waits.

        The drainer thread will exit on its own once it sees the event — either
        at its next loop iteration (within 0.1 seconds during message processing,
        or within 1 second during idle waiting). For a clean blocking shutdown,
        stop() calls this method and then joins the thread itself.
        """
        self._stop_drainer_event.set()

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
        """
        Establish the initial connection to the broker.

        v0.5.0: TLS and authentication are configured here, before the
        connection attempt. Both must be applied to the paho client object
        before connect() is called — they configure the underlying socket
        and the CONNECT packet that paho will send to the broker.

        The order matters:
          1. tls_set()           — configures the SSL socket
          2. username_pw_set()   — sets credentials in the CONNECT packet
          3. client.connect()    — opens the socket and starts the handshake
        """
        self.logger.info(
            "Connecting to broker",
            broker=f"{self.broker_host}:{self.broker_port}",
            tls=self.use_tls,
        )

        try:
            # Step 1: Configure TLS if enabled.
            # tls_set() wraps the socket in SSL and loads the certificates that
            # will be used to verify the broker's identity (ca_certs) and,
            # optionally, to prove this client's identity to the broker (certfile
            # and keyfile, used for mutual TLS / mTLS).
            if self.use_tls:
                self.client.tls_set(
                    ca_certs=self.ca_certs,
                    certfile=self.certfile,   # None unless mTLS is required
                    keyfile=self.keyfile,     # None unless mTLS is required
                )
                self.logger.info(
                    "TLS configured",
                    ca_certs=self.ca_certs,
                    mutual_tls=self.certfile is not None,
                )

            # Step 2: Configure username/password authentication if provided.
            # These become fields in the MQTT CONNECT packet. This is MQTT-level
            # authentication, separate from TLS — TLS secures the transport,
            # username/password authenticates the client to the broker application.
            if self.username is not None:
                self.client.username_pw_set(self.username, self.password)
                self.logger.info("Authentication configured", username=self.username)

            # Step 3: Open the connection.
            self.client.connect(self.broker_host, self.broker_port, keepalive=60)

        except Exception as e:
            self.logger.error("Initial connection failed", error=str(e))
            self._reconnect_with_backoff()

    def publish(self, topic, payload, qos=0, retain=False, priority=1):
        """
        Publish a message, routing to the broker or the offline queue as appropriate.

        If connected, the message is published directly. For QoS 1/2, it is
        added to the inflight tracker until acknowledged. If disconnected (or
        if the send fails mid-flight), the message goes to the offline queue.
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
                self.logger.warning(
                    "Publish failed mid-flight — routing to offline queue",
                    topic=topic,
                    error=str(e),
                )
                with self.connection_lock:
                    self.is_connected = False

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
        """Return a snapshot of current client state."""
        return {
            "connected":               self.is_connected,
            "current_backoff_seconds": self.current_backoff,
            "offline_queue":           self.offline_queue.get_stats(),
            "inflight_messages":       self.inflight_tracker.count_messages(),
            "tls_enabled":             self.use_tls,
        }

    def start(self):
        """Start the MQTT network loop."""
        self.client.loop_start()
        self.logger.info("Network loop started")

    def stop(self):
        """
        Stop the network loop, drain the queue drainer, and close the database.

        Unlike _stop_queue_drainer() (which is non-blocking so it is safe to
        call from paho's callback thread), this method is called by the user
        and may block briefly while waiting for the drainer thread to exit.
        That is acceptable here because stop() is not called from a callback.
        """
        self.logger.info("Shutting down client", client_id=self.client_id)

        self.reconnect_in_progress = False

        # Signal the drainer to stop
        self._stop_drainer_event.set()

        # Now we can safely join — stop() is not called from paho's thread
        if self.queue_drainer_thread and self.queue_drainer_thread.is_alive():
            self.queue_drainer_thread.join(timeout=5)

        self.client.loop_stop()
        self.inflight_tracker.close()
        self.offline_queue.close()

        with self._db_lock:
            self._db_conn.close()

        self.logger.info("Client stopped", client_id=self.client_id)
        self.logger.log_event("client_stopped", client_id=self.client_id)
