"""
inflight_tracker.py — tracks MQTT messages that have been sent but not yet acknowledged.

v0.3.0: Payload stored as BLOB (raw bytes) to prevent binary data corruption.
        Schema migration handles upgrade from TEXT-based v0.2.0 databases.

v0.4.0: Accepts a shared SQLite connection and lock from ProductionMQTTClient,
        so the inflight table and offline queue table live in a single database
        file rather than two separate ones. Falls back to opening its own
        connection if used in standalone mode (backward compatible).
        All print() calls replaced with structured logger output.
"""

import sqlite3
import threading
import time
from production_logger import get_logger


class InflightTracker:
    """
    Track inflight MQTT messages using an SQLite-backed store.

    A message is "inflight" from the moment it is handed to the broker until
    the corresponding PUBACK (QoS 1) or PUBCOMP (QoS 2) is received. Storing
    these in SQLite means they survive reconnections and process restarts, so
    they can be re-sent rather than silently lost.
    """

    def __init__(self, db_path="mqtt_client.db", conn=None, lock=None):
        """
        Initialise the inflight tracker.

        If conn and lock are provided (as they are when called from
        ProductionMQTTClient), the tracker uses the shared connection and lock
        rather than creating its own. Both storage systems then read and write
        to the same SQLite file, coordinated by the same lock.

        If conn is not provided, the tracker opens its own connection to
        db_path. This preserves backward compatibility for code that
        instantiates InflightTracker directly outside of ProductionMQTTClient.

        The _owns_connection flag records which mode we are in so that
        close() knows whether to close the connection or leave it alone.
        """
        # Obtain the logger singleton. If ProductionMQTTClient created it
        # first (as it should), we get the fully configured instance.
        # If this class is used standalone, we get a default instance.
        self.logger = get_logger()

        if conn is not None:
            # Shared mode — ProductionMQTTClient owns the connection lifecycle.
            self.conn = conn
            self.lock = lock
            self._owns_connection = False
        else:
            # Standalone mode — we own the connection and must close it.
            self.conn = sqlite3.connect(db_path, check_same_thread=False)
            self.lock = threading.Lock()
            self._owns_connection = True

        self._migrate_schema()
        self._create_table()

    # ------------------------------------------------------------------
    # Schema setup and migration
    # ------------------------------------------------------------------

    def _migrate_schema(self):
        """
        Detect and remove the old TEXT-based schema from v0.2.0 and earlier.

        The old schema stored payload as TEXT using str(payload), which
        corrupts binary data. Any data stored that way is unrecoverable,
        so the table is dropped and rebuilt with the correct BLOB column.
        See the v0.3.0 release notes for a full explanation.
        """
        with self.lock:
            cursor = self.conn.cursor()
            cursor.execute("PRAGMA table_info(inflight_messages)")
            # PRAGMA table_info returns rows: (cid, name, type, notnull, dflt, pk)
            columns = {row[1]: row[2].upper() for row in cursor.fetchall()}

            if "payload" in columns and columns["payload"] == "TEXT":
                self.logger.warning(
                    "Detected old TEXT-based schema from v0.2.0 — "
                    "dropping table and rebuilding as BLOB. "
                    "Any stored inflight messages were corrupted and cannot be recovered."
                )
                cursor.execute("DROP TABLE inflight_messages")
                self.conn.commit()

    def _create_table(self):
        """Create the inflight messages table with the correct BLOB schema."""
        with self.lock:
            cursor = self.conn.cursor()
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS inflight_messages (
                    packet_id INTEGER PRIMARY KEY,
                    topic     TEXT    NOT NULL,
                    payload   BLOB    NOT NULL,
                    qos       INTEGER NOT NULL,
                    retain    INTEGER NOT NULL,
                    timestamp INTEGER NOT NULL
                )
            """)
            self.conn.commit()

    # ------------------------------------------------------------------
    # Internal helper
    # ------------------------------------------------------------------

    @staticmethod
    def _to_bytes(payload):
        """
        Ensure payload is bytes before storing.

        Strings are UTF-8 encoded. Bytes are passed through unchanged.
        We never call str() on the payload — that was the v0.2.0 bug.
        """
        if isinstance(payload, bytes):
            return payload
        return payload.encode("utf-8")

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    def add_message(self, packet_id, topic, payload, qos, retain):
        """
        Store a message as inflight.

        Uses INSERT OR REPLACE so that if the same packet_id is reused
        (paho wraps the 16-bit mid counter at 65535), the old record is
        safely overwritten rather than raising a UNIQUE constraint error.
        """
        with self.lock:
            cursor = self.conn.cursor()
            cursor.execute(
                """
                INSERT OR REPLACE INTO inflight_messages
                    (packet_id, topic, payload, qos, retain, timestamp)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    packet_id,
                    topic,
                    self._to_bytes(payload),
                    qos,
                    1 if retain else 0,
                    int(time.time()),
                ),
            )
            self.conn.commit()

        self.logger.debug(
            "Stored inflight message",
            packet_id=packet_id,
            topic=topic,
            qos=qos,
        )

    def remove_message(self, packet_id):
        """Remove a message once the broker has acknowledged it."""
        with self.lock:
            cursor = self.conn.cursor()
            cursor.execute(
                "DELETE FROM inflight_messages WHERE packet_id = ?",
                (packet_id,),
            )
            self.conn.commit()

        self.logger.debug("Removed inflight message", packet_id=packet_id)

    def get_all_messages(self):
        """
        Return all stored inflight messages, ordered by timestamp.

        Payload is returned as bytes — exactly as stored — so the caller
        can hand it directly to client.publish() without any re-encoding.
        """
        with self.lock:
            cursor = self.conn.cursor()
            cursor.execute(
                """
                SELECT packet_id, topic, payload, qos, retain, timestamp
                FROM inflight_messages
                ORDER BY timestamp
                """
            )
            rows = cursor.fetchall()

        # Build plain dicts from the row tuples. Index access works correctly
        # whether the connection uses sqlite3.Row or the default tuple factory.
        return [
            {
                "packet_id": row[0],
                "topic":     row[1],
                "payload":   row[2],     # bytes, ready to publish
                "qos":       row[3],
                "retain":    bool(row[4]),
                "timestamp": row[5],
            }
            for row in rows
        ]

    def count_messages(self):
        """Return the number of messages currently awaiting acknowledgment."""
        with self.lock:
            cursor = self.conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM inflight_messages")
            return cursor.fetchone()[0]

    def close(self):
        """
        Close the database connection if we own it.

        In shared connection mode (_owns_connection=False), the connection
        is left open for ProductionMQTTClient to close in its stop() method.
        In standalone mode, we close the connection we opened ourselves.
        """
        if self._owns_connection:
            with self.lock:
                self.conn.close()
            self.logger.debug("InflightTracker closed its database connection")
