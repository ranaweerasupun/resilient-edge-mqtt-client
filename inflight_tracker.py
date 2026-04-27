"""
inflight_tracker.py — tracks MQTT messages that have been sent but not yet acknowledged.

v0.3.0: Payload stored as BLOB (raw bytes) to prevent binary data corruption.
        Schema migration handles upgrade from TEXT-based v0.2.0 databases.
v0.4.0: Accepts a shared SQLite connection and lock from ProductionMQTTClient.
        All print() calls replaced with structured logger output.
v1.0.0: Type hints added throughout.
"""


import sqlite3
import threading
import time
from typing import Any, Dict, List, Optional, Union

from production_logger import get_logger


class InflightTracker:
    """
    Track inflight MQTT messages using an SQLite-backed store.

    A message is "inflight" from the moment it is handed to the broker until
    the corresponding PUBACK (QoS 1) or PUBCOMP (QoS 2) is received. Storing
    these in SQLite means they survive reconnections and process restarts.
    """

    def __init__(
        self,
        db_path: str = "mqtt_client.db",
        conn: Optional[sqlite3.Connection] = None,
        lock: Optional[threading.Lock] = None,
    ) -> None:
        """
        Initialise the inflight tracker.

        If conn and lock are provided (as they are when called from
        ProductionMQTTClient), the tracker uses the shared connection and lock.
        If conn is not provided, the tracker opens its own connection to db_path,
        preserving backward compatibility for standalone use.
        """
        self.logger = get_logger()

        if conn is not None:
            self.conn = conn
            self.lock = lock  # type: ignore[assignment]
            self._owns_connection = False
        else:
            self.conn = sqlite3.connect(db_path, check_same_thread=False)
            self.lock = threading.Lock()
            self._owns_connection = True

        self._migrate_schema()
        self._create_table()

    def _migrate_schema(self) -> None:
        """
        Detect and remove the old TEXT-based schema from v0.2.0 and earlier.

        The old schema stored payload as TEXT using str(payload), which
        corrupts binary data. Any data stored that way is unrecoverable,
        so the table is dropped and rebuilt with the correct BLOB column.
        """
        with self.lock:
            cursor = self.conn.cursor()
            cursor.execute("PRAGMA table_info(inflight_messages)")
            columns = {row[1]: row[2].upper() for row in cursor.fetchall()}

            if "payload" in columns and columns["payload"] == "TEXT":
                self.logger.warning(
                    "Detected old TEXT-based schema from v0.2.0 — "
                    "dropping table and rebuilding as BLOB. "
                    "Any stored inflight messages were corrupted and cannot be recovered."
                )
                cursor.execute("DROP TABLE inflight_messages")
                self.conn.commit()

    def _create_table(self) -> None:
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

    @staticmethod
    def _to_bytes(payload: Union[str, bytes]) -> bytes:
        """
        Ensure payload is bytes before storing.

        Strings are UTF-8 encoded. Bytes are passed through unchanged.
        We never call str() on the payload — that was the v0.2.0 bug.
        """
        if isinstance(payload, bytes):
            return payload
        return payload.encode("utf-8")

    def add_message(
        self,
        packet_id: int,
        topic: str,
        payload: Union[str, bytes],
        qos: int,
        retain: bool,
    ) -> None:
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

    def remove_message(self, packet_id: int) -> None:
        """Remove a message once the broker has acknowledged it."""
        with self.lock:
            cursor = self.conn.cursor()
            cursor.execute(
                "DELETE FROM inflight_messages WHERE packet_id = ?",
                (packet_id,),
            )
            self.conn.commit()

        self.logger.debug("Removed inflight message", packet_id=packet_id)

    def get_all_messages(self) -> List[Dict[str, Any]]:
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

        return [
            {
                "packet_id": row[0],
                "topic":     row[1],
                "payload":   row[2],
                "qos":       row[3],
                "retain":    bool(row[4]),
                "timestamp": row[5],
            }
            for row in rows
        ]

    def count_messages(self) -> int:
        """Return the number of messages currently awaiting acknowledgment."""
        with self.lock:
            cursor = self.conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM inflight_messages")
            return int(cursor.fetchone()[0])

    def close(self) -> None:
        """
        Close the database connection if we own it.

        In shared connection mode, the connection is left open for
        ProductionMQTTClient to close in its stop() method.
        """
        if self._owns_connection:
            with self.lock:
                self.conn.close()
            self.logger.debug("InflightTracker closed its database connection")
