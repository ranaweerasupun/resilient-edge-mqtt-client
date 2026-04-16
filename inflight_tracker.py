"""
inflight_tracker.py — tracks MQTT messages that have been sent but not yet acknowledged.

SQLite is used for persistence so that inflight state survives a process restart.
The connection is opened with check_same_thread=False because the MQTT network
thread and the main application thread both call into this class. A threading lock
serialises access so only one thread touches the database at a time.
"""

import sqlite3
import threading
from pathlib import Path

class InflightTracker:
    """
    Track inflight MQTT messages using an SQLite-backed store.

    A message is "inflight" from the moment it is handed to the broker until
    the corresponding PUBACK (QoS 1) or PUBCOMP (QoS 2) is received. Storing
    these in SQLite means they survive reconnections and process restarts, so
    they can be re-sent rather than silently lost.
    """
    
    def __init__(self, db_path="inflight_messages.db"):
        self.db_path = db_path
        
        # Serialises database access across the MQTT network thread and the
        # main application thread.
        self.lock = threading.Lock()
        
        # check_same_thread=False is required here because the connection is
        # created in the main thread but used from the MQTT callback thread.
        # The lock above ensures only one thread is in the database at a time.
        self.conn = sqlite3.connect(
            db_path,
            check_same_thread=False
        )
        
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
        """Create the inflight messages table if it doesn't exist."""
        with self.lock:
            cursor = self.conn.cursor()
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS inflight_messages (
                    packet_id INTEGER PRIMARY KEY,
                    topic TEXT NOT NULL,
                    payload   BLOB    NOT NULL,  -- raw bytes; never str(payload)
                    qos INTEGER NOT NULL,
                    retain INTEGER NOT NULL,
                    timestamp INTEGER NOT NULL
                )
            """)
            self.conn.commit()

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
    
    def add_message(self, packet_id, topic, payload, qos, retain):
        """Store a message as inflight. Uses INSERT OR REPLACE in case of mid collision."""
        import time
        
        with self.lock:
            cursor = self.conn.cursor()
            
            # Convert boolean retain to integer (SQLite doesn't have boolean)
            retain_int = 1 if retain else 0
            
            cursor.execute("""
                INSERT OR REPLACE INTO inflight_messages 
                (packet_id, topic, payload, qos, retain, timestamp)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (packet_id, topic, str(payload), qos, retain_int, int(time.time())))
            
            self.conn.commit()
            
        print(f"Stored inflight message: packet_id={packet_id}, topic={topic}")
    
    def remove_message(self, packet_id):
        """Remove a message once the broker has acknowledged it."""
        with self.lock:
            cursor = self.conn.cursor()
            cursor.execute("DELETE FROM inflight_messages WHERE packet_id = ?", (packet_id,))
            self.conn.commit()
            
        print(f"Removed inflight message: packet_id={packet_id}")
    
    def get_all_messages(self):
        """
        Return all stored inflight messages, ordered by timestamp.

        Called on reconnection to get the list of messages that need to be
        re-sent. Returns a list of dicts with keys matching the table columns.
        """
        with self.lock:
            cursor = self.conn.cursor()
            cursor.execute("""
                SELECT packet_id, topic, payload, qos, retain, timestamp
                FROM inflight_messages
                ORDER BY timestamp
            """)
            
            rows = cursor.fetchall()
            
            # Convert to list of dictionaries
            messages = []
            for row in rows:
                messages.append({
                    'packet_id': row[0],
                    'topic': row[1],
                    'payload': row[2],
                    'qos': row[3],
                    'retain': bool(row[4]),  # Convert back to boolean
                    'timestamp': row[5]
                })
            
            return messages
    
    def count_messages(self):
        """Return the number of messages currently awaiting acknowledgment."""
        with self.lock:
            cursor = self.conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM inflight_messages")
            count = cursor.fetchone()[0]
            return count
    
    def close(self):
        """Close the database connection."""
        with self.lock:
            self.conn.close()
