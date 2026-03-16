# Fixed inflight_tracker.py - Thread-Safe Version

"""
Threading Issue Explanation:
═══════════════════════════════════════════════════════════════════════════════

THE PROBLEM:
1. Main thread creates RobustMQTTClient
2. RobustMQTTClient creates InflightTracker
3. InflightTracker opens SQLite connection (in main thread)
4. client.start() starts MQTT background thread
5. Background thread calls _on_connect callback
6. _on_connect tries to use SQLite connection from different thread
7. SQLite raises: "SQLite objects created in a thread can only be used in that same thread"

THE SOLUTION:
Add check_same_thread=False to SQLite connection. This tells SQLite to allow
the connection to be used from any thread. We add a threading lock to ensure
only one thread accesses the database at a time.
"""

import sqlite3
import threading
from pathlib import Path

class InflightTracker:
    """
    Track inflight MQTT messages using SQLite database.
    Thread-safe version with proper locking.
    """
    
    def __init__(self, db_path="inflight_messages.db"):
        self.db_path = db_path
        
        # Create a lock to ensure thread-safe database access
        # This prevents two threads from accessing the database simultaneously
        self.lock = threading.Lock()
        
        # Open SQLite connection with check_same_thread=False
        # This allows the connection to be used from any thread
        # We use the lock above to ensure thread safety
        self.conn = sqlite3.connect(
            db_path,
            check_same_thread=False  # ← THIS IS THE KEY FIX!
        )
        
        self._create_table()
    
    def _create_table(self):
        """Create the inflight messages table if it doesn't exist."""
        with self.lock:  # Acquire lock before database access
            cursor = self.conn.cursor()
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS inflight_messages (
                    packet_id INTEGER PRIMARY KEY,
                    topic TEXT NOT NULL,
                    payload TEXT NOT NULL,
                    qos INTEGER NOT NULL,
                    retain INTEGER NOT NULL,
                    timestamp INTEGER NOT NULL
                )
            """)
            self.conn.commit()
    
    def add_message(self, packet_id, topic, payload, qos, retain):
        """Add a message to the inflight tracker."""
        import time
        
        with self.lock:  # Thread-safe access
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
        """Remove a message from the inflight tracker (when acknowledged)."""
        with self.lock:  # Thread-safe access
            cursor = self.conn.cursor()
            cursor.execute("DELETE FROM inflight_messages WHERE packet_id = ?", (packet_id,))
            self.conn.commit()
            
        print(f"Removed inflight message: packet_id={packet_id}")
    
    def get_all_messages(self):
        """Get all inflight messages."""
        with self.lock:  # Thread-safe access
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
        """Count how many messages are currently inflight."""
        with self.lock:  # Thread-safe access
            cursor = self.conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM inflight_messages")
            count = cursor.fetchone()[0]
            return count
    
    def close(self):
        """Close the database connection."""
        with self.lock:  # Thread-safe access
            self.conn.close()
