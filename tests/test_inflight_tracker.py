"""
test_inflight_tracker.py — unit tests for InflightTracker.

The most important things to verify about InflightTracker are:
  - Binary payloads are stored and retrieved as bytes (the v0.3.0 BLOB fix).
  - The schema migration from v0.2.0's TEXT column is correctly detected
    and rebuilt.
  - The resend tracking pattern works: remove old packet_id, insert new one.
    This is the specific v0.3.0 fix for the "second disconnect loses message" bug.
  - INSERT OR REPLACE handles duplicate packet IDs correctly.
"""

import sqlite3
import threading
import time

import pytest

from inflight_tracker import InflightTracker


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def make_tracker(db_connection):
    conn, lock = db_connection
    return InflightTracker(conn=conn, lock=lock)


# ---------------------------------------------------------------------------
# Basic add / retrieve / remove
# ---------------------------------------------------------------------------

def test_add_and_retrieve_message(db_connection):
    """A message stored via add_message should be returned by get_all_messages."""
    t = make_tracker(db_connection)
    t.add_message(packet_id=42, topic="sensors/temp", payload=b"hello", qos=1, retain=False)

    messages = t.get_all_messages()

    assert len(messages) == 1
    assert messages[0]["packet_id"] == 42
    assert messages[0]["topic"] == "sensors/temp"
    assert messages[0]["payload"] == b"hello"
    assert messages[0]["qos"] == 1
    assert messages[0]["retain"] is False


def test_remove_message(db_connection):
    """remove_message() must delete the matching row so it is not returned again."""
    t = make_tracker(db_connection)
    t.add_message(1, "t/topic", b"data", 1, False)
    t.remove_message(1)

    assert t.get_all_messages() == []


def test_count_messages(db_connection):
    """count_messages() must return the exact number of stored messages."""
    t = make_tracker(db_connection)

    assert t.count_messages() == 0

    t.add_message(1, "a", b"x", 1, False)
    t.add_message(2, "b", b"y", 1, False)
    assert t.count_messages() == 2

    t.remove_message(1)
    assert t.count_messages() == 1


def test_messages_returned_in_timestamp_order(db_connection):
    """get_all_messages() must return messages ordered by timestamp ascending."""
    t = make_tracker(db_connection)
    t.add_message(1, "first", b"1", 1, False)
    time.sleep(0.01)  # ensure distinct timestamps
    t.add_message(2, "second", b"2", 1, False)
    time.sleep(0.01)
    t.add_message(3, "third", b"3", 1, False)

    messages = t.get_all_messages()
    packet_ids = [m["packet_id"] for m in messages]

    assert packet_ids == [1, 2, 3]


# ---------------------------------------------------------------------------
# Payload storage correctness (v0.3.0 fix verification)
# ---------------------------------------------------------------------------

def test_binary_payload_survives_roundtrip(db_connection):
    """
    Binary payloads must come back byte-for-byte unchanged.

    This is the core test for the v0.3.0 BLOB fix. In v0.2.0, payloads were
    stored as str(payload), turning b'\\x00\\xff' into the string "b'\\x00\\xff'".
    After the fix, the raw bytes are stored and returned without modification.
    """
    raw = bytes(range(256))  # all 256 byte values
    t = make_tracker(db_connection)
    t.add_message(1, "binary/topic", raw, 1, False)

    messages = t.get_all_messages()
    assert messages[0]["payload"] == raw


def test_string_payload_encoded_to_utf8_bytes(db_connection):
    """
    String payloads must be encoded to UTF-8 bytes before storage and returned
    as bytes. The tracker always stores bytes — the conversion is one-way.
    """
    t = make_tracker(db_connection)
    t.add_message(1, "json/topic", '{"value": 42}', 1, False)

    messages = t.get_all_messages()
    assert messages[0]["payload"] == b'{"value": 42}'


def test_retain_flag_roundtrip(db_connection):
    """The retain flag must survive storage as int and return as bool."""
    t = make_tracker(db_connection)
    t.add_message(1, "t", b"v", 1, retain=True)

    messages = t.get_all_messages()
    assert messages[0]["retain"] is True


# ---------------------------------------------------------------------------
# INSERT OR REPLACE behaviour
# ---------------------------------------------------------------------------

def test_duplicate_packet_id_replaces_existing(db_connection):
    """
    Adding a message with an existing packet_id must replace the old record
    rather than failing with a UNIQUE constraint error.

    paho-mqtt wraps its 16-bit mid counter at 65535 and reuses IDs. The
    INSERT OR REPLACE handles this gracefully.
    """
    t = make_tracker(db_connection)
    t.add_message(42, "first/topic", b"original", 1, False)
    t.add_message(42, "second/topic", b"replacement", 1, False)

    messages = t.get_all_messages()

    assert len(messages) == 1
    assert messages[0]["topic"] == "second/topic"
    assert messages[0]["payload"] == b"replacement"


# ---------------------------------------------------------------------------
# Resend tracking pattern (v0.3.0 fix — the key integration of concepts)
# ---------------------------------------------------------------------------

def test_resend_tracking_removes_old_and_inserts_new(db_connection):
    """
    This test directly verifies the fix for the v0.3.0 resend-tracking bug.

    The scenario: a message is inflight with packet_id=10 from connection 1.
    The client disconnects and reconnects. paho assigns packet_id=1 to the
    resent message. The correct fix is to:
      1. Remove the old entry (packet_id=10)
      2. Insert a new entry (packet_id=1)

    Without step 1, the tracker holds a stale ID that can never be acknowledged.
    Without step 2, a second disconnection would lose the message entirely.
    After the fix, the tracker always reflects live state.
    """
    t = make_tracker(db_connection)

    # Step 1: message is inflight from connection 1 with packet_id=10
    t.add_message(packet_id=10, topic="sensors/temp", payload=b"reading", qos=1, retain=False)
    assert t.count_messages() == 1

    # Step 2: client reconnects, resends the message — paho assigns packet_id=1
    old_messages = t.get_all_messages()
    for msg in old_messages:
        # This is exactly what _resend_inflight_messages() does
        t.remove_message(msg["packet_id"])    # remove old (packet_id=10)
        t.add_message(                         # add new (packet_id=1)
            packet_id=1,
            topic=msg["topic"],
            payload=msg["payload"],
            qos=msg["qos"],
            retain=msg["retain"],
        )

    # Tracker should now contain exactly one entry with the new packet_id
    live_messages = t.get_all_messages()
    assert len(live_messages) == 1
    assert live_messages[0]["packet_id"] == 1
    assert live_messages[0]["topic"] == "sensors/temp"


# ---------------------------------------------------------------------------
# Schema migration (v0.3.0 — upgrade from the old TEXT-based schema)
# ---------------------------------------------------------------------------

def test_schema_migration_detects_and_rebuilds_old_text_schema():
    """
    When InflightTracker is initialised against a database that has the old
    TEXT-based payload column (from v0.2.0), it must:
      1. Detect the old schema via PRAGMA table_info.
      2. Drop the old table (its data is corrupted and unrecoverable).
      3. Recreate the table with the correct BLOB column.

    After migration, new messages must be stored and retrieved correctly.
    """
    conn = sqlite3.connect(":memory:", check_same_thread=False)

    # Create the old broken schema manually — same as v0.2.0 created it
    conn.execute("""
        CREATE TABLE inflight_messages (
            packet_id INTEGER PRIMARY KEY,
            topic     TEXT    NOT NULL,
            payload   TEXT    NOT NULL,
            qos       INTEGER NOT NULL,
            retain    INTEGER NOT NULL,
            timestamp INTEGER NOT NULL
        )
    """)
    # Insert a corrupted row that str(bytes) would have produced
    conn.execute(
        "INSERT INTO inflight_messages VALUES (1, 'old/topic', \"b'\\x00\\x01'\", 1, 0, 1000)"
    )
    conn.commit()

    # Initialising the tracker should trigger the migration
    lock = threading.Lock()
    tracker = InflightTracker(conn=conn, lock=lock)

    # The corrupted old row must be gone
    assert tracker.count_messages() == 0

    # New messages must now be stored as proper BLOB bytes
    tracker.add_message(99, "new/topic", b"\x00\x01\x02", 1, False)
    messages = tracker.get_all_messages()
    assert len(messages) == 1
    assert messages[0]["payload"] == b"\x00\x01\x02"

    conn.close()


def test_no_migration_needed_if_schema_already_correct(db_connection):
    """
    When the schema is already correct (BLOB column), _migrate_schema()
    must leave existing data untouched.
    """
    t = make_tracker(db_connection)
    t.add_message(1, "t/topic", b"existing data", 1, False)

    # Calling _migrate_schema() again should be a no-op
    t._migrate_schema()

    messages = t.get_all_messages()
    assert len(messages) == 1
    assert messages[0]["payload"] == b"existing data"


# ---------------------------------------------------------------------------
# Standalone mode
# ---------------------------------------------------------------------------

def test_standalone_mode_and_close(tmp_path):
    """
    InflightTracker must work in standalone mode (no shared connection),
    and close() must release the connection without error.
    """
    db_path = str(tmp_path / "inflight_standalone.db")
    t = InflightTracker(db_path=db_path)

    t.add_message(1, "standalone/topic", b"hello", 1, False)
    assert t.count_messages() == 1

    t.close()  # must not raise

    # The db file should exist on disk
    import os
    assert os.path.exists(db_path)
