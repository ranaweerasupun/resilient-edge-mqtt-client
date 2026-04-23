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

import http.server
import json
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

# Queue capacity thresholds for health status classification.
# Below DEGRADED_THRESHOLD  → healthy.
# At or above it             → degraded (still connected, but trending toward full).
_DEGRADED_THRESHOLD = 80.0   # percent


class ProductionMQTTClient:
    """
    MQTT client with offline queuing, inflight tracking, TLS, authentication,
    bidirectional communication, and an HTTP health check endpoint.

    Recommended instantiation:

        config = Config.from_file("config.json")
        client = ProductionMQTTClient.from_config(config)
        client.connect()
        client.start()

    With health check enabled, GET http://localhost:8080/health returns a JSON
    body describing the client's current state. The HTTP status code is 200 for
    healthy/degraded and 503 for unhealthy (not connected to the broker).
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
        use_tls=False,
        ca_certs=None,
        certfile=None,
        keyfile=None,
        username=None,
        password=None,
        # v0.7.0: health check parameters
        enable_health_check=False,
        health_check_port=8080,
    ):
        self.client_id          = client_id
        self.broker_host        = broker_host
        self.broker_port        = broker_port
        self.use_tls            = use_tls
        self.ca_certs           = ca_certs
        self.certfile           = certfile
        self.keyfile            = keyfile
        self.username           = username
        self.password           = password
        self._enable_health_check = enable_health_check
        self._health_check_port   = health_check_port

        # Step 1: Logger — must come first
        log_level_int = _LOG_LEVEL_MAP.get(log_level.upper(), logging.INFO)
        self.logger = get_logger("mqtt_client", log_dir=log_dir, log_level=log_level_int)

        # Step 2: Shared database connection and lock
        self._db_conn = sqlite3.connect(db_path, check_same_thread=False)
        self._db_lock = threading.Lock()
        self.logger.info("Database opened", path=db_path)

        # Step 3: Storage systems
        self.inflight_tracker = InflightTracker(conn=self._db_conn, lock=self._db_lock)
        self.offline_queue    = OfflineQueue(conn=self._db_conn, lock=self._db_lock, max_size=max_queue_size)

        # Step 4: Connection state
        self.is_connected          = False
        self.connection_lock       = threading.Lock()
        self.min_backoff           = min_backoff
        self.max_backoff           = max_backoff
        self.current_backoff       = self.min_backoff
        self.reconnect_in_progress = False

        # Step 5: Subscription registry
        self._subscriptions     = {}
        self._subscription_lock = threading.Lock()

        # Step 6: MQTT client and callbacks
        self.client = mqtt.Client(client_id=client_id, clean_session=False)
        self.client.on_connect    = self._on_connect
        self.client.on_disconnect = self._on_disconnect
        self.client.on_publish    = self._on_publish
        self.client.on_message    = self._on_message

        # Step 7: Queue drainer state
        self.queue_drainer_running = False
        self.queue_drainer_thread  = None
        self._stop_drainer_event   = threading.Event()

        # Step 8: Health check server state (v0.7.0)
        # The server object and its thread are created in _start_health_check_server(),
        # which is called from start(). They are not created here because __init__
        # should not start background services — that is start()'s job.
        self._health_check_server = None
        self._health_check_thread = None

        self.logger.info(
            "Client initialised",
            client_id=client_id,
            broker=f"{broker_host}:{broker_port}",
            tls=use_tls,
            auth=username is not None,
            health_check=enable_health_check,
            health_check_port=health_check_port if enable_health_check else None,
        )

    # ------------------------------------------------------------------
    # Factory method
    # ------------------------------------------------------------------

    @classmethod
    def from_config(cls, config):
        """Create a ProductionMQTTClient from a Config object (recommended path)."""
        return cls(
            client_id           = config.get("client_id"),
            broker_host         = config.get("broker_host"),
            broker_port         = config.get("broker_port"),
            max_queue_size      = config.get("max_queue_size"),
            db_path             = config.get("db_path"),
            min_backoff         = config.get("min_backoff"),
            max_backoff         = config.get("max_backoff"),
            log_dir             = config.get("log_dir"),
            log_level           = config.get("log_level"),
            use_tls             = config.get("use_tls"),
            ca_certs            = config.get("ca_certs"),
            certfile            = config.get("certfile"),
            keyfile             = config.get("keyfile"),
            username            = config.get("username"),
            password            = config.get("password"),
            enable_health_check = config.get("enable_health_check"),
            health_check_port   = config.get("health_check_port"),
        )

    # ------------------------------------------------------------------
    # Health check server (v0.7.0)
    # ------------------------------------------------------------------

    def _start_health_check_server(self):
        """
        Start a minimal HTTP health check server in a background daemon thread.

        The server responds to GET /health (and GET / as an alias) with a JSON
        body and an HTTP status code that reflects the client's current state:

          200 "healthy"   — connected to broker, queue below 80% capacity.
          200 "degraded"  — connected, but queue at or above 80% capacity.
                            The client is functional but trending toward full;
                            investigate before it becomes a problem.
          503 "unhealthy" — not connected to the broker.

        Healthy and degraded both return HTTP 200 because the process is alive
        and messages are still being accepted and delivered. Kubernetes liveness
        probes, Docker HEALTHCHECK, and most load balancers only care whether
        the status code is 2xx — they will treat both as "alive." Monitoring
        tools that read the response body can distinguish between the two.

        The handler class is defined inside this method so it can reference
        `self` (the ProductionMQTTClient) through a closure without needing
        to pass it as a constructor argument, which BaseHTTPRequestHandler's
        interface does not easily support.
        """
        client = self  # capture reference for the handler closure

        class _HealthCheckHandler(http.server.BaseHTTPRequestHandler):

            def do_GET(self):
                # Only /health and / are valid paths.
                if self.path not in ("/", "/health"):
                    self._send_json(404, {"error": "not found", "hint": "use GET /health"})
                    return

                stats = client.get_statistics()

                # Classify the health state based on connection and queue pressure.
                connected     = stats["connected"]
                queue_percent = stats["offline_queue"]["capacity_used_percent"]

                if not connected:
                    status    = "unhealthy"
                    http_code = 503
                elif queue_percent >= _DEGRADED_THRESHOLD:
                    status    = "degraded"
                    http_code = 200
                else:
                    status    = "healthy"
                    http_code = 200

                self._send_json(http_code, {
                    "status":     status,
                    "client_id":  client.client_id,
                    "statistics": stats,
                })

            def _send_json(self, code, body_dict):
                """Serialise body_dict to JSON and write the full HTTP response."""
                body = json.dumps(body_dict, indent=2).encode("utf-8")
                self.send_response(code)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def log_message(self, format, *args):
                """
                Override the default handler to suppress stderr output and
                route access logs through the structured logger instead.
                """
                client.logger.debug(
                    "Health check request",
                    method=self.command,
                    path=self.path,
                    remote=self.client_address[0],
                )

        try:
            self._health_check_server = http.server.HTTPServer(
                ("", self._health_check_port),
                _HealthCheckHandler,
            )
        except OSError as e:
            # The most common failure is EADDRINUSE — the port is already taken.
            self.logger.error(
                "Failed to start health check server — port may already be in use",
                port=self._health_check_port,
                error=str(e),
            )
            return

        self._health_check_thread = threading.Thread(
            target=self._health_check_server.serve_forever,
            daemon=True,  # exits automatically when the main process exits
            name="health-check-server",
        )
        self._health_check_thread.start()

        self.logger.info(
            "Health check server started",
            port=self._health_check_port,
            url=f"http://localhost:{self._health_check_port}/health",
        )
        self.logger.log_event(
            "health_check_server_started",
            port=self._health_check_port,
        )

    def _stop_health_check_server(self):
        """
        Stop the health check server gracefully.

        shutdown() signals serve_forever() to exit on its next iteration and
        blocks until it does. server_close() then releases the port. This order
        is important — closing the port before shutdown() finishes can cause
        a brief EADDRINUSE window if the process restarts quickly.
        """
        if self._health_check_server is None:
            return

        self._health_check_server.shutdown()    # waits for serve_forever() to exit
        self._health_check_server.server_close() # releases the bound port
        self._health_check_server = None
        self.logger.info("Health check server stopped")

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

            self.current_backoff       = self.min_backoff
            self.reconnect_in_progress = False

            self._resend_inflight_messages()
            self._restore_subscriptions()
            self._start_queue_drainer()
        else:
            self.logger.error("Connection refused by broker", return_code=rc)

    def _on_disconnect(self, client, userdata, rc):
        """Handle disconnection. Safe to call from paho's network thread."""
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

    def _on_message(self, client, userdata, message):
        """
        Route an incoming message to all matching registered callbacks.
        Takes a snapshot of _subscriptions before releasing the lock
        to avoid holding the lock during user callback execution.
        """
        topic  = message.topic
        qos    = message.qos
        retain = bool(message.retain)

        self.logger.debug(
            "Message received",
            topic=topic,
            qos=qos,
            retain=retain,
            size=len(message.payload),
        )

        with self._subscription_lock:
            snapshot = list(self._subscriptions.items())

        matched = False
        for pattern, (callback, _) in snapshot:
            if self._topic_matches(pattern, topic):
                matched = True
                try:
                    callback(topic, message.payload, qos, retain)
                except Exception as e:
                    self.logger.error(
                        "Callback raised an exception",
                        topic=topic,
                        pattern=pattern,
                        error=str(e),
                    )

        if not matched:
            self.logger.warning("Received message with no matching callback", topic=topic)

    # ------------------------------------------------------------------
    # Subscription management
    # ------------------------------------------------------------------

    def subscribe(self, topic, callback, qos=1):
        """
        Subscribe to a topic and register a callback for incoming messages.

        Safe to call before connect(). Subscriptions registered offline are
        stored and sent to the broker automatically on the next connection.

        Callback signature: callback(topic: str, payload: bytes, qos: int, retain: bool)
        """
        with self._subscription_lock:
            self._subscriptions[topic] = (callback, qos)

        if self.is_connected:
            self.client.subscribe(topic, qos)
            self.logger.info("Subscribed to topic", topic=topic, qos=qos)
            self.logger.log_event("subscribed", topic=topic, qos=qos)
        else:
            self.logger.info(
                "Subscription stored — will activate on next connection",
                topic=topic,
                qos=qos,
            )

    def unsubscribe(self, topic):
        """Unsubscribe from a topic and remove its callback."""
        with self._subscription_lock:
            removed = self._subscriptions.pop(topic, None)

        if removed is None:
            self.logger.warning("unsubscribe() called for a topic that was not subscribed", topic=topic)
            return

        if self.is_connected:
            self.client.unsubscribe(topic)
            self.logger.info("Unsubscribed from topic", topic=topic)
            self.logger.log_event("unsubscribed", topic=topic)
        else:
            self.logger.info("Subscription removed from registry (client not connected)", topic=topic)

    def _restore_subscriptions(self):
        """Re-send all stored subscriptions to the broker after reconnection."""
        with self._subscription_lock:
            snapshot = list(self._subscriptions.items())

        if not snapshot:
            return

        self.logger.info("Restoring subscriptions after reconnection", count=len(snapshot))
        for topic, (_, qos) in snapshot:
            self.client.subscribe(topic, qos)
            self.logger.debug("Restored subscription", topic=topic, qos=qos)

    @staticmethod
    def _topic_matches(pattern, topic):
        """
        Return True if the topic matches the MQTT subscription pattern.
        '+' matches exactly one level. '#' matches zero or more levels (must be last).
        """
        pattern_parts = pattern.split("/")
        topic_parts   = topic.split("/")

        for i, part in enumerate(pattern_parts):
            if part == "#":
                return len(topic_parts) >= i
            if i >= len(topic_parts):
                return False
            if part != "+" and part != topic_parts[i]:
                return False

        return len(pattern_parts) == len(topic_parts)

    # ------------------------------------------------------------------
    # Inflight resend
    # ------------------------------------------------------------------

    def _resend_inflight_messages(self):
        """Resend messages inflight during the previous connection."""
        messages = self.inflight_tracker.get_all_messages()
        if not messages:
            return

        self.logger.info("Resending inflight messages", count=len(messages))
        for msg in messages:
            info = self.client.publish(
                topic=msg["topic"], payload=msg["payload"],
                qos=msg["qos"], retain=msg["retain"],
            )
            self.inflight_tracker.remove_message(msg["packet_id"])
            if msg["qos"] > 0:
                self.inflight_tracker.add_message(
                    packet_id=info.mid, topic=msg["topic"],
                    payload=msg["payload"], qos=msg["qos"], retain=msg["retain"],
                )
            self.logger.debug("Inflight message resent", topic=msg["topic"],
                              old_mid=msg["packet_id"], new_mid=info.mid)

    # ------------------------------------------------------------------
    # Queue drainer
    # ------------------------------------------------------------------

    def _start_queue_drainer(self):
        """Start the background thread that drains the offline queue."""
        if self.queue_drainer_running:
            return

        self.queue_drainer_running = True
        self._stop_drainer_event.clear()

        def drain_queue():
            self.logger.info("Queue drainer started")
            while not self._stop_drainer_event.is_set() and self.is_connected:
                messages = self.offline_queue.get_next_batch(batch_size=10)
                if not messages:
                    self._stop_drainer_event.wait(timeout=1)
                    continue

                self.logger.info("Draining messages from offline queue", count=len(messages))
                for msg in messages:
                    if self._stop_drainer_event.is_set() or not self.is_connected:
                        self.logger.warning("Queue drain interrupted")
                        break

                    info = self.client.publish(
                        topic=msg["topic"], payload=msg["payload"],
                        qos=msg["qos"], retain=msg["retain"],
                    )
                    if msg["qos"] > 0:
                        self.inflight_tracker.add_message(
                            packet_id=info.mid, topic=msg["topic"],
                            payload=msg["payload"], qos=msg["qos"], retain=msg["retain"],
                        )
                    self.offline_queue.remove_message(msg["id"])
                    self.logger.debug("Drained message", topic=msg["topic"], priority=msg["priority"])
                    self._stop_drainer_event.wait(timeout=0.1)

            self.queue_drainer_running = False
            self.logger.info("Queue drainer stopped")

        self.queue_drainer_thread = threading.Thread(target=drain_queue, daemon=True)
        self.queue_drainer_thread.start()

    def _stop_queue_drainer(self):
        """Signal the queue drainer to stop. Returns immediately (non-blocking)."""
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
                self.logger.info("Waiting before reconnection attempt", backoff_seconds=self.current_backoff)
                time.sleep(self.current_backoff)
                try:
                    self.logger.info("Attempting to reconnect")
                    self.client.reconnect()
                    break
                except Exception as e:
                    self.logger.error("Reconnection attempt failed", error=str(e))
                    self.current_backoff = min(self.current_backoff * 2, self.max_backoff)

        thread = threading.Thread(target=reconnect_thread, daemon=True)
        thread.start()

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    def connect(self):
        """Establish the initial connection to the broker."""
        self.logger.info("Connecting to broker", broker=f"{self.broker_host}:{self.broker_port}", tls=self.use_tls)
        try:
            if self.use_tls:
                self.client.tls_set(ca_certs=self.ca_certs, certfile=self.certfile, keyfile=self.keyfile)
                self.logger.info("TLS configured", ca_certs=self.ca_certs, mutual_tls=self.certfile is not None)
            if self.username is not None:
                self.client.username_pw_set(self.username, self.password)
                self.logger.info("Authentication configured", username=self.username)
            self.client.connect(self.broker_host, self.broker_port, keepalive=60)
        except Exception as e:
            self.logger.error("Initial connection failed", error=str(e))
            self._reconnect_with_backoff()

    def publish(self, topic, payload, qos=0, retain=False, priority=1):
        """Publish a message, routing to the broker or offline queue as appropriate."""
        with self.connection_lock:
            connected = self.is_connected

        if connected:
            try:
                info = self.client.publish(topic, payload, qos, retain)
                if qos > 0:
                    self.inflight_tracker.add_message(
                        packet_id=info.mid, topic=topic,
                        payload=payload, qos=qos, retain=retain,
                    )
                self.logger.debug("Message published to broker", topic=topic, qos=qos, mid=info.mid)
                return info
            except Exception as e:
                self.logger.warning("Publish failed mid-flight — routing to offline queue", topic=topic, error=str(e))
                with self.connection_lock:
                    self.is_connected = False

        success = self.offline_queue.add_message(
            topic=topic, payload=payload, qos=qos, retain=retain, priority=priority,
        )
        if not success:
            self.logger.error("Message dropped — queue full", topic=topic, priority=priority)
        return None

    def get_statistics(self):
        """
        Return a snapshot of current client state.

        This is the data source for the health check endpoint. All values
        are read atomically under their respective locks so the snapshot
        is self-consistent even under concurrent access.
        """
        with self._subscription_lock:
            subscription_count = len(self._subscriptions)

        return {
            "connected":               self.is_connected,
            "current_backoff_seconds": self.current_backoff,
            "offline_queue":           self.offline_queue.get_stats(),
            "inflight_messages":       self.inflight_tracker.count_messages(),
            "tls_enabled":             self.use_tls,
            "active_subscriptions":    subscription_count,
        }

    def start(self):
        """
        Start the MQTT network loop and, if configured, the health check server.

        Both services are started here rather than in __init__() because
        __init__() should only configure — start() is the explicit signal
        from the caller that they want background services to begin running.
        """
        self.client.loop_start()
        self.logger.info("Network loop started")

        if self._enable_health_check:
            self._start_health_check_server()

    def stop(self):
        """Stop all background services and close the database."""
        self.logger.info("Shutting down client", client_id=self.client_id)

        self.reconnect_in_progress = False
        self._stop_drainer_event.set()

        if self.queue_drainer_thread and self.queue_drainer_thread.is_alive():
            self.queue_drainer_thread.join(timeout=5)

        self._stop_health_check_server()

        self.client.loop_stop()
        self.inflight_tracker.close()
        self.offline_queue.close()

        with self._db_lock:
            self._db_conn.close()

        self.logger.info("Client stopped", client_id=self.client_id)
        self.logger.log_event("client_stopped", client_id=self.client_id)
