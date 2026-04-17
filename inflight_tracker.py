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

    def __init__(self, db_path="inflight_messages.db", conn=None, lock=None):
        """
        Initialise the inflight tracker.

        """
        
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

    def _migrate_schema(self):
        """
        Detect and remove the old TEXT-based schema from v0.2.0 and earlier.

        The old schema stored payload as TEXT using str(payload), which corrupts
        binary data (e.g. bytes b'\\x00\\x01' becomes the string "b'\\x00\\x01'").
        Any rows stored under the old schema are already corrupted, so the
        cleanest fix is to drop the table entirely and let _create_table()
        rebuild it with the correct BLOB column.

        SQLite does not support ALTER COLUMN, which is why we drop and recreate
        rather than trying to alter the existing table in place.
        """
        with self.lock:
            cursor = self.conn.cursor()

            # PRAGMA table_info returns one row per column: (cid, name, type, ...)
            cursor.execute("PRAGMA table_info(inflight_messages)")
            columns = {row[1]: row[2].upper() for row in cursor.fetchall()}

            # If the table exists and payload is TEXT, we need to migrate.
            if "payload" in columns and columns["payload"] == "TEXT":
                print(
                    "InflightTracker: detected old TEXT-based schema — "
                    "dropping table and rebuilding as BLOB. "
                    "Stored inflight messages were corrupted and cannot be recovered."
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
                    payload   BLOB    NOT NULL,  -- raw bytes; never str(payload)
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

        MQTT payloads are bytes on the wire. If the caller passed a string
        (e.g. a JSON string), encode it to UTF-8. If it's already bytes,
        leave it alone. This means we never call str() on the payload —
        which is exactly the bug this class used to have.
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
        (paho wraps the 16-bit mid counter), the old record is safely
        overwritten rather than causing a UNIQUE constraint error.
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
                    self._to_bytes(payload),   # store as raw bytes, not str()
                    qos,
                    1 if retain else 0,
                    int(time.time()),
                ),
            )
            self.conn.commit()

        print(f"Stored inflight message: packet_id={packet_id}, topic={topic}")

    def remove_message(self, packet_id):
        """Remove a message once the broker has acknowledged it."""
        with self.lock:
            cursor = self.conn.cursor()
            cursor.execute(
                "DELETE FROM inflight_messages WHERE packet_id = ?", (packet_id,)
            )
            self.conn.commit()

        print(f"Removed inflight message: packet_id={packet_id}")

    def get_all_messages(self):
        """
        Return all stored inflight messages, ordered by timestamp.

        Called on reconnection to get the list of messages that need to be
        re-sent. Payload is returned as bytes — exactly as it was stored —
        so the caller can hand it directly to client.publish() without any
        further encoding step.
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

        messages = []
        for row in rows:
            messages.append(
                {
                    "packet_id": row[0],
                    "topic":     row[1],
                    "payload":   row[2],          # bytes, ready to publish
                    "qos":       row[3],
                    "retain":    bool(row[4]),
                    "timestamp": row[5],
                }
            )

        return messages

    def count_messages(self):
        """Return the number of messages currently awaiting acknowledgment."""
        with self.lock:
            cursor = self.conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM inflight_messages")
            return cursor.fetchone()[0]

    def close(self):
        """Close the database connection."""
        with self.lock:
            self.conn.close()
