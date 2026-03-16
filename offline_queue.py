# -*- coding: utf-8 -*-
import sqlite3
import json
import threading  # ← ADD THIS
from datetime import datetime
from pathlib import Path

class OfflineQueue:
    """
    Manages messages that couldn't be published due to lack of connectivity.
    
    This is the first line of defense against message loss. When the device
    is offline, messages accumulate here. When connectivity returns, they
    drain from here into the normal publishing pipeline.
    
    Thread-safe version with proper locking.
    """
    
    def __init__(self, db_path="mqtt_client.db", max_size=1000):
        """
        Initialize the offline queue.
        
        The max_size parameter is critical for edge devices with limited
        storage. Setting it too low means losing data during outages.
        Setting it too high risks filling up the disk. Choose based on:
        - How frequently messages are generated
        - How long outages typically last
        - How much disk space is available
        - The average message size
        
        For example: 1000 messages * 500 bytes each = ~500KB
        If you generate 10 messages/minute, this holds ~100 minutes of data
        """
        self.db_path = db_path
        self.max_size = max_size
        
        # ADD THREADING LOCK
        self.lock = threading.Lock()
        
        self.conn = sqlite3.connect(db_path, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self._create_tables()
    
    def _create_tables(self):
        """
        Create the offline queue table.
        
        Note the differences from the inflight table:
        - id is auto-incrementing (messages don't have packet_id yet)
        - priority field for smart queue management
        - We track creation timestamp to implement time-based policies
        """
        with self.lock:  # ← ADD LOCK
            cursor = self.conn.cursor()
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS offline_queue (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    topic TEXT NOT NULL,
                    payload BLOB NOT NULL,
                    qos INTEGER NOT NULL,
                    retain INTEGER NOT NULL,
                    priority INTEGER DEFAULT 1,
                    timestamp TEXT NOT NULL
                )
            ''')
            
            # Create an index on timestamp for efficient ordering
            cursor.execute('''
                CREATE INDEX IF NOT EXISTS idx_timestamp 
                ON offline_queue(timestamp)
            ''')
            
            # Create an index on priority for efficient priority-based operations
            cursor.execute('''
                CREATE INDEX IF NOT EXISTS idx_priority 
                ON offline_queue(priority DESC, timestamp ASC)
            ''')
            
            self.conn.commit()
    
    def add_message(self, topic, payload, qos, retain=False, priority=1):
        """
        Add a message to the offline queue.
        
        This is called when we try to publish but have no connection.
        The message waits here until connectivity returns.
        
        Returns True if message was added, False if queue is full
        and message was dropped according to the policy.
        """
        with self.lock:  # ← ADD LOCK
            cursor = self.conn.cursor()
            
            # Check if queue is at capacity
            current_size = self._get_queue_size_unsafe()
            
            if current_size >= self.max_size:
                # Queue is full - we need to make room
                if not self._make_room_for_message_unsafe(priority):
                    # Couldn't make room - drop this message
                    print(f"⚠ Queue full! Dropped message to {topic}")
                    return False
            
            # Convert payload to bytes if needed
            if isinstance(payload, str):
                payload = payload.encode('utf-8')
            
            timestamp = datetime.now().isoformat()
            
            cursor.execute('''
                INSERT INTO offline_queue 
                (topic, payload, qos, retain, priority, timestamp)
                VALUES (?, ?, ?, ?, ?, ?)
            ''', (topic, payload, qos, 1 if retain else 0, priority, timestamp))
            
            self.conn.commit()
            print(f"📥 Queued offline: {topic} (priority={priority}, queue_size={current_size + 1})")
            return True
    
    def _get_queue_size(self):
        """Return the current number of messages in the queue."""
        with self.lock:  # ← ADD LOCK
            return self._get_queue_size_unsafe()
    
    def _get_queue_size_unsafe(self):
        """Internal method - assumes lock is already held."""
        cursor = self.conn.cursor()
        cursor.execute('SELECT COUNT(*) FROM offline_queue')
        return cursor.fetchone()[0]
    
    def _make_room_for_message_unsafe(self, new_message_priority):
        """
        Internal method - assumes lock is already held.
        
        Try to make room in the queue for a new message.
        
        Strategy:
        1. First, try to drop messages with priority lower than the new message
        2. If no lower-priority messages exist, drop the oldest message
        3. If the new message itself is lowest priority, drop it instead
        
        Returns True if room was made, False if new message should be dropped.
        """
        cursor = self.conn.cursor()
        
        # Try to find a lower-priority message to drop
        cursor.execute('''
            SELECT id FROM offline_queue 
            WHERE priority < ? 
            ORDER BY priority ASC, timestamp ASC 
            LIMIT 1
        ''', (new_message_priority,))
        
        row = cursor.fetchone()
        
        if row:
            # Found a lower-priority message to drop
            cursor.execute('DELETE FROM offline_queue WHERE id = ?', (row['id'],))
            self.conn.commit()
            print(f"📤 Dropped lower-priority message to make room")
            return True
        
        # No lower-priority messages - check if we should drop oldest
        # regardless of priority
        cursor.execute('''
            SELECT id, priority FROM offline_queue 
            ORDER BY timestamp ASC 
            LIMIT 1
        ''')
        
        row = cursor.fetchone()
        
        if row:
            # Drop oldest message
            cursor.execute('DELETE FROM offline_queue WHERE id = ?', (row['id'],))
            self.conn.commit()
            print(f"📤 Dropped oldest message to make room")
            return True
        
        # This shouldn't happen, but handle it gracefully
        return False
    
    def get_next_batch(self, batch_size=10):
        """
        Retrieve the next batch of messages to publish.
        
        We fetch in batches rather than one-at-a-time for efficiency.
        Messages are ordered by priority (high to low), then by timestamp
        (oldest first). This ensures important messages go first, and
        within the same priority level, we maintain order.
        """
        with self.lock:  # ← ADD LOCK
            cursor = self.conn.cursor()
            cursor.execute('''
                SELECT * FROM offline_queue 
                ORDER BY priority DESC, timestamp ASC 
                LIMIT ?
            ''', (batch_size,))
            
            rows = cursor.fetchall()
            
            messages = []
            for row in rows:
                messages.append({
                    'id': row['id'],
                    'topic': row['topic'],
                    'payload': row['payload'],
                    'qos': row['qos'],
                    'retain': bool(row['retain']),
                    'priority': row['priority'],
                    'timestamp': row['timestamp']
                })
            
            return messages
    
    def remove_message(self, message_id):
        """
        Remove a message from the queue after it's been successfully published.
        
        This is called after we've moved the message from the offline queue
        into the normal publishing pipeline. The message might still be in
        the inflight tracker waiting for acknowledgment, but it's no longer
        "offline" - it's been sent.
        """
        with self.lock:  # ← ADD LOCK
            cursor = self.conn.cursor()
            cursor.execute('DELETE FROM offline_queue WHERE id = ?', (message_id,))
            self.conn.commit()
    
    def get_stats(self):
        """
        Get statistics about the offline queue.
        
        Useful for monitoring and debugging. You can expose these stats
        through your health monitoring system to track queue depth over time.
        """
        with self.lock:  # ← ADD LOCK
            cursor = self.conn.cursor()
            
            # Total count
            cursor.execute('SELECT COUNT(*) as total FROM offline_queue')
            total = cursor.fetchone()['total']
            
            # Count by priority
            cursor.execute('''
                SELECT priority, COUNT(*) as count 
                FROM offline_queue 
                GROUP BY priority 
                ORDER BY priority DESC
            ''')
            by_priority = {row['priority']: row['count'] for row in cursor.fetchall()}
            
            # Oldest message age
            cursor.execute('''
                SELECT timestamp FROM offline_queue 
                ORDER BY timestamp ASC 
                LIMIT 1
            ''')
            oldest = cursor.fetchone()
            oldest_age = None
            if oldest:
                oldest_time = datetime.fromisoformat(oldest['timestamp'])
                oldest_age = (datetime.now() - oldest_time).total_seconds()
            
            return {
                'total_messages': total,
                'by_priority': by_priority,
                'oldest_message_age_seconds': oldest_age,
                'capacity_used_percent': (total / self.max_size * 100) if self.max_size > 0 else 0
            }
    
    def clear(self):
        """Clear all messages from the queue. Use with caution!"""
        with self.lock:  # ← ADD LOCK
            cursor = self.conn.cursor()
            cursor.execute('DELETE FROM offline_queue')
            self.conn.commit()
            print("⚠ Offline queue cleared")
    
    def close(self):
        """Clean up database connection."""
        with self.lock:  # ← ADD LOCK
            self.conn.close()