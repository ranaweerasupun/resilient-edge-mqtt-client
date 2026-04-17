# -*- coding: utf-8 -*-
"""
offline_queue.py — holds messages that couldn't be published due to loss of connectivity.

v0.4.0: Accepts a shared SQLite connection and lock from ProductionMQTTClient,
        so both storage systems live in a single database file. Falls back to
        creating its own connection if used standalone (backward compatible).
        All print() calls replaced with structured logger output.
"""

import sqlite3
import threading
from datetime import datetime
from production_logger import get_logger


class OfflineQueue:
    """
    Holds messages that couldn't be published due to loss of connectivity.

    When the device goes offline, messages accumulate here. When the
    connection returns, the queue drainer in ProductionMQTTClient pulls
    them out in batches and feeds them back into the normal publish path.

    Thread-safe: all database operations are serialised through a lock,
    since the MQTT network thread and the application thread both write here.
    """

    def __init__(self, db_path="mqtt_client.db", max_size=1000, conn=None, lock=None):
        """
        Initialise the offline queue.

        If conn and lock are provided (as they are when called from
        ProductionMQTTClient), the queue uses the shared connection and lock
        rather than creating its own. Both storage systems then read and write
        to the same SQLite file, coordinated by the same lock.

        If conn is not provided, the queue opens its own connection to
        db_path. This preserves backward compatibility for code that
        instantiates OfflineQueue directly outside of ProductionMQTTClient.

        The max_size limit exists to protect devices with limited flash or SD
        card storage. As a rough guide: 1000 messages at ~500 bytes each is
        around 500 KB. At 10 messages per minute, that covers roughly 100
        minutes of outage.
        """
        self.logger = get_logger()
        self.max_size = max_size

        if conn is not None:
            # Shared mode — ProductionMQTTClient owns the connection lifecycle.
            # row_factory may already be set; setting it again is harmless.
            self.conn = conn
            self.conn.row_factory = sqlite3.Row
            self.lock = lock
            self._owns_connection = False
        else:
            # Standalone mode — we own the connection and must close it.
            self.conn = sqlite3.connect(db_path, check_same_thread=False)
            self.conn.row_factory = sqlite3.Row
            self.lock = threading.Lock()
            self._owns_connection = True

        self._create_tables()

    def _create_tables(self):
        """
        Create the offline queue table and its indexes.

        Unlike the inflight table, rows here use an auto-incrementing id
        because messages don't have a packet_id until they're actually sent.
        The priority and timestamp indexes are both used in the ordering
        query inside get_next_batch.
        """
        with self.lock:
            cursor = self.conn.cursor()
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS offline_queue (
                    id        INTEGER PRIMARY KEY AUTOINCREMENT,
                    topic     TEXT    NOT NULL,
                    payload   BLOB    NOT NULL,
                    qos       INTEGER NOT NULL,
                    retain    INTEGER NOT NULL,
                    priority  INTEGER DEFAULT 1,
                    timestamp TEXT    NOT NULL
                )
            ''')

            # Index for ordering by arrival time
            cursor.execute('''
                CREATE INDEX IF NOT EXISTS idx_timestamp
                ON offline_queue(timestamp)
            ''')

            # Compound index used by the priority-ordered fetch in get_next_batch
            cursor.execute('''
                CREATE INDEX IF NOT EXISTS idx_priority
                ON offline_queue(priority DESC, timestamp ASC)
            ''')

            self.conn.commit()

    def add_message(self, topic, payload, qos, retain=False, priority=1):
        """
        Add a message to the offline queue.

        Called when publish() is attempted while disconnected. If the queue
        is at capacity, the eviction policy in _make_room_for_message_unsafe
        runs first. Returns True if the message was stored, False if it was
        dropped because no room could be made.
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
                '''
                INSERT INTO offline_queue
                    (topic, payload, qos, retain, priority, timestamp)
                VALUES (?, ?, ?, ?, ?, ?)
                ''',
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

    def _get_queue_size_unsafe(self):
        """Count rows without acquiring the lock. Caller must hold self.lock."""
        cursor = self.conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM offline_queue")
        return cursor.fetchone()[0]

    def _make_room_for_message_unsafe(self, new_message_priority):
        """
        Attempt to free one slot in the queue. Caller must hold self.lock.

        Eviction strategy:
        1. Drop the lowest-priority message below the incoming priority.
        2. If none found, drop the oldest message regardless of priority.
        3. Returns True if a slot was freed, False if the message should be dropped.
        """
        cursor = self.conn.cursor()

        # Try to find a lower-priority message to drop
        cursor.execute(
            '''
            SELECT id FROM offline_queue
            WHERE priority < ?
            ORDER BY priority ASC, timestamp ASC
            LIMIT 1
            ''',
            (new_message_priority,),
        )
        row = cursor.fetchone()

        if row:
            cursor.execute("DELETE FROM offline_queue WHERE id = ?", (row["id"],))
            self.conn.commit()
            self.logger.debug("Evicted lower-priority message to make room")
            return True

        # No lower-priority messages — fall back to dropping the oldest
        cursor.execute(
            '''
            SELECT id FROM offline_queue
            ORDER BY timestamp ASC
            LIMIT 1
            '''
        )
        row = cursor.fetchone()

        if row:
            cursor.execute("DELETE FROM offline_queue WHERE id = ?", (row["id"],))
            self.conn.commit()
            self.logger.debug("Evicted oldest message to make room")
            return True

        return False

    def get_next_batch(self, batch_size=10):
        """
        Retrieve the next batch of messages to publish.

        Ordered by priority descending, then timestamp ascending — so
        high-priority messages go first, and within the same priority level
        older messages go before newer ones.
        """
        with self.lock:
            cursor = self.conn.cursor()
            cursor.execute(
                '''
                SELECT * FROM offline_queue
                ORDER BY priority DESC, timestamp ASC
                LIMIT ?
                ''',
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

    def remove_message(self, message_id):
        """
        Remove a message after it has been handed off to the publish pipeline.

        At this point the message may still be in the inflight tracker waiting
        for broker acknowledgment, but it's no longer the queue's responsibility.
        """
        with self.lock:
            cursor = self.conn.cursor()
            cursor.execute("DELETE FROM offline_queue WHERE id = ?", (message_id,))
            self.conn.commit()

    def get_stats(self):
        """
        Return queue depth and age statistics.

        Exposed via ProductionMQTTClient.get_statistics() for health checks.
        The capacity_used_percent field is useful for triggering alerts before
        the queue fills up entirely.
        """
        with self.lock:
            cursor = self.conn.cursor()

            cursor.execute("SELECT COUNT(*) as total FROM offline_queue")
            total = cursor.fetchone()["total"]

            cursor.execute(
                '''
                SELECT priority, COUNT(*) as count
                FROM offline_queue
                GROUP BY priority
                ORDER BY priority DESC
                '''
            )
            by_priority = {row["priority"]: row["count"] for row in cursor.fetchall()}

            cursor.execute(
                '''
                SELECT timestamp FROM offline_queue
                ORDER BY timestamp ASC
                LIMIT 1
                '''
            )
            oldest = cursor.fetchone()
            oldest_age = None
            if oldest:
                oldest_time = datetime.fromisoformat(oldest["timestamp"])
                oldest_age = (datetime.now() - oldest_time).total_seconds()

            return {
                "total_messages":            total,
                "by_priority":               by_priority,
                "oldest_message_age_seconds": oldest_age,
                "capacity_used_percent":      (total / self.max_size * 100) if self.max_size > 0 else 0,
            }

    def clear(self):
        """Clear all messages from the queue. Use with caution."""
        with self.lock:
            cursor = self.conn.cursor()
            cursor.execute("DELETE FROM offline_queue")
            self.conn.commit()
        self.logger.warning("Offline queue cleared")

    def close(self):
        """
        Close the database connection if we own it.

        In shared connection mode (_owns_connection=False), the connection
        is left open for ProductionMQTTClient to close in its stop() method.
        """
        if self._owns_connection:
            with self.lock:
                self.conn.close()
            self.logger.debug("OfflineQueue closed its database connection")
