# -*- coding: utf-8 -*-
"""
offline_queue.py — holds messages that couldn't be published due to loss of connectivity.

v0.4.0: Shared SQLite connection and lock. Logger integration.
v1.0.0: Type hints added throughout.
"""


import sqlite3
import threading
from datetime import datetime
from typing import Any, Dict, List, Optional, Union

from .production_logger import get_logger


class OfflineQueue:
    """
    Holds messages that couldn't be published due to loss of connectivity.

    When the device goes offline, messages accumulate here. When the
    connection returns, the queue drainer in ProductionMQTTClient pulls
    them out in batches and feeds them back into the normal publish path.
    """

    def __init__(
        self,
        db_path: str = "mqtt_client.db",
        max_size: int = 1000,
        conn: Optional[sqlite3.Connection] = None,
        lock: Optional[threading.Lock] = None,
    ) -> None:
        """
        Initialise the offline queue.

        If conn and lock are provided, the queue uses the shared connection
        and lock from ProductionMQTTClient. Otherwise it opens its own
        connection to db_path (backward compatible standalone mode).

        max_size limits how many messages can be queued at once. As a rough
        guide: 1000 messages at ~500 bytes each is around 500 KB.
        """
        self.logger = get_logger()
        self.max_size = max_size

        if conn is not None:
            self.conn = conn
            self.conn.row_factory = sqlite3.Row
            self.lock = lock  # type: ignore[assignment]
            self._owns_connection = False
        else:
            self.conn = sqlite3.connect(db_path, check_same_thread=False)
            self.conn.row_factory = sqlite3.Row
            self.lock = threading.Lock()
            self._owns_connection = True

        self._create_tables()

    def _create_tables(self) -> None:
        """Create the offline queue table and its indexes."""
        with self.lock:
            cursor = self.conn.cursor()
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS offline_queue (
                    id        INTEGER PRIMARY KEY AUTOINCREMENT,
                    topic     TEXT    NOT NULL,
                    payload   BLOB    NOT NULL,
                    qos       INTEGER NOT NULL,
                    retain    INTEGER NOT NULL,
                    priority  INTEGER DEFAULT 1,
                    timestamp TEXT    NOT NULL
                )
            """)
            cursor.execute(
                "CREATE INDEX IF NOT EXISTS idx_timestamp ON offline_queue(timestamp)"
            )
            cursor.execute(
                "CREATE INDEX IF NOT EXISTS idx_priority ON offline_queue(priority DESC, timestamp ASC)"
            )
            self.conn.commit()

    def add_message(
        self,
        topic: str,
        payload: Union[str, bytes],
        qos: int,
        retain: bool = False,
        priority: int = 1,
    ) -> bool:
        """
        Add a message to the offline queue.

        Returns True if the message was stored, False only in the pathological
        case where the queue is somehow both at capacity and empty (unreachable
        in normal operation). In practice, the eviction fallback always makes
        room by dropping the oldest message.
        """
        with self.lock:
            cursor = self.conn.cursor()
            current_size = self._get_queue_size_unsafe()

            if current_size >= self.max_size:
                if not self._make_room_for_message_unsafe(priority):
                    self.logger.warning(
                        "Queue full — message dropped",
                        topic=topic,
                        priority=priority,
                        queue_size=current_size,
                    )
                    return False

            if isinstance(payload, str):
                payload = payload.encode("utf-8")

            timestamp = datetime.now().isoformat()
            cursor.execute(
                """
                INSERT INTO offline_queue
                    (topic, payload, qos, retain, priority, timestamp)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (topic, payload, qos, 1 if retain else 0, priority, timestamp),
            )
            self.conn.commit()

        self.logger.debug(
            "Message queued offline",
            topic=topic,
            priority=priority,
            queue_size=current_size + 1,
        )
        return True

    def _get_queue_size_unsafe(self) -> int:
        """Count rows without acquiring the lock. Caller must hold self.lock."""
        cursor = self.conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM offline_queue")
        return int(cursor.fetchone()[0])

    def _make_room_for_message_unsafe(self, new_message_priority: int) -> bool:
        """
        Attempt to free one slot in the queue. Caller must hold self.lock.

        Eviction strategy:
          1. Drop the lowest-priority message below the incoming priority.
          2. If none found, drop the oldest message regardless of priority.
          3. Returns True if a slot was freed, False if the queue is empty
             (which cannot happen in normal operation since max_size > 0).
        """
        cursor = self.conn.cursor()

        cursor.execute(
            """
            SELECT id FROM offline_queue
            WHERE priority < ?
            ORDER BY priority ASC, timestamp ASC
            LIMIT 1
            """,
            (new_message_priority,),
        )
        row = cursor.fetchone()
        if row:
            cursor.execute("DELETE FROM offline_queue WHERE id = ?", (row["id"],))
            self.conn.commit()
            self.logger.debug("Evicted lower-priority message to make room")
            return True

        cursor.execute(
            "SELECT id FROM offline_queue ORDER BY timestamp ASC LIMIT 1"
        )
        row = cursor.fetchone()
        if row:
            cursor.execute("DELETE FROM offline_queue WHERE id = ?", (row["id"],))
            self.conn.commit()
            self.logger.debug("Evicted oldest message to make room")
            return True

        return False

    def get_next_batch(self, batch_size: int = 10) -> List[Dict[str, Any]]:
        """
        Retrieve the next batch of messages to publish.

        Ordered by priority descending, then timestamp ascending — high-priority
        messages first, and within the same priority level older before newer.
        """
        with self.lock:
            cursor = self.conn.cursor()
            cursor.execute(
                """
                SELECT * FROM offline_queue
                ORDER BY priority DESC, timestamp ASC
                LIMIT ?
                """,
                (batch_size,),
            )
            rows = cursor.fetchall()

        return [
            {
                "id":        row["id"],
                "topic":     row["topic"],
                "payload":   row["payload"],
                "qos":       row["qos"],
                "retain":    bool(row["retain"]),
                "priority":  row["priority"],
                "timestamp": row["timestamp"],
            }
            for row in rows
        ]

    def remove_message(self, message_id: int) -> None:
        """
        Remove a message after it has been handed off to the publish pipeline.

        At this point the message may still be in the inflight tracker waiting
        for broker acknowledgment, but it's no longer the queue's responsibility.
        """
        with self.lock:
            cursor = self.conn.cursor()
            cursor.execute("DELETE FROM offline_queue WHERE id = ?", (message_id,))
            self.conn.commit()

    def get_stats(self) -> Dict[str, Any]:
        """
        Return queue depth and age statistics.

        Exposed via ProductionMQTTClient.get_statistics() for health checks.
        """
        with self.lock:
            cursor = self.conn.cursor()

            cursor.execute("SELECT COUNT(*) as total FROM offline_queue")
            total: int = cursor.fetchone()["total"]

            cursor.execute(
                """
                SELECT priority, COUNT(*) as count
                FROM offline_queue
                GROUP BY priority
                ORDER BY priority DESC
                """
            )
            by_priority: Dict[int, int] = {
                row["priority"]: row["count"] for row in cursor.fetchall()
            }

            cursor.execute(
                "SELECT timestamp FROM offline_queue ORDER BY timestamp ASC LIMIT 1"
            )
            oldest = cursor.fetchone()
            oldest_age: Optional[float] = None
            if oldest:
                oldest_time = datetime.fromisoformat(oldest["timestamp"])
                oldest_age = (datetime.now() - oldest_time).total_seconds()

            return {
                "total_messages":             total,
                "by_priority":                by_priority,
                "oldest_message_age_seconds": oldest_age,
                "capacity_used_percent":      (total / self.max_size * 100) if self.max_size > 0 else 0.0,
            }

    def clear(self) -> None:
        """Clear all messages from the queue. Use with caution."""
        with self.lock:
            cursor = self.conn.cursor()
            cursor.execute("DELETE FROM offline_queue")
            self.conn.commit()
        self.logger.warning("Offline queue cleared")

    def close(self) -> None:
        """
        Close the database connection if we own it.

        In shared mode, the connection is left for ProductionMQTTClient to close.
        """
        if self._owns_connection:
            with self.lock:
                self.conn.close()
            self.logger.debug("OfflineQueue closed its database connection")
