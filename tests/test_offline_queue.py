"""
test_offline_queue.py — unit tests for OfflineQueue.

Every test uses an in-memory SQLite database so there is no disk I/O and no
state leaks between tests. The db_connection fixture from conftest.py provides
a fresh connection and lock for each test function.

The most important things to verify about OfflineQueue are:
  - Messages are returned in priority-descending, timestamp-ascending order.
  - Eviction under capacity correctly prefers dropping lower-priority messages.
  - Payloads survive storage and retrieval without modification.
  - Statistics accurately reflect the queue's current state.
"""

import sqlite3
import threading

import pytest

from robmqtt.offline_queue import OfflineQueue


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def make_queue(db_connection, max_size=10):
    conn, lock = db_connection
    return OfflineQueue(conn=conn, lock=lock, max_size=max_size)


# ---------------------------------------------------------------------------
# Basic add / retrieve / remove
# ---------------------------------------------------------------------------

def test_add_and_retrieve_single_message(db_connection):
    """A message added to an empty queue should be returned by get_next_batch."""
    q = make_queue(db_connection)
    q.add_message("sensors/temp", b'{"value": 23.5}', qos=1)

    batch = q.get_next_batch(batch_size=10)

    assert len(batch) == 1
    assert batch[0]["topic"] == "sensors/temp"
    assert batch[0]["payload"] == b'{"value": 23.5}'
    assert batch[0]["qos"] == 1


def test_remove_message_does_not_appear_in_next_batch(db_connection):
    """After remove_message(), that message must not be returned again."""
    q = make_queue(db_connection)
    q.add_message("sensors/temp", b"hello", qos=1)

    batch = q.get_next_batch(batch_size=10)
    assert len(batch) == 1

    q.remove_message(batch[0]["id"])

    batch_after = q.get_next_batch(batch_size=10)
    assert len(batch_after) == 0


def test_get_next_batch_respects_batch_size(db_connection):
    """get_next_batch must return at most batch_size messages."""
    q = make_queue(db_connection, max_size=20)
    for i in range(10):
        q.add_message(f"topic/{i}", b"x", qos=0)

    batch = q.get_next_batch(batch_size=3)
    assert len(batch) == 3


# ---------------------------------------------------------------------------
# Priority ordering
# ---------------------------------------------------------------------------

def test_high_priority_message_returned_before_low_priority(db_connection):
    """
    Messages must be returned in priority-descending order.

    This is the core contract of the priority queue: a critical alert
    (priority 10) should always be sent before routine telemetry (priority 1),
    even if the telemetry was queued first.
    """
    q = make_queue(db_connection)
    q.add_message("low", b"low", qos=1, priority=1)
    q.add_message("high", b"high", qos=1, priority=10)
    q.add_message("mid", b"mid", qos=1, priority=5)

    batch = q.get_next_batch(batch_size=10)
    topics = [msg["topic"] for msg in batch]

    assert topics == ["high", "mid", "low"]


def test_same_priority_messages_returned_oldest_first(db_connection):
    """
    Within the same priority level, older messages must come first.

    This preserves the natural delivery order for messages of equal importance.
    Without this, a queue under steady load would deliver in unpredictable order.
    """
    q = make_queue(db_connection)
    q.add_message("first", b"1", qos=1, priority=5)
    q.add_message("second", b"2", qos=1, priority=5)
    q.add_message("third", b"3", qos=1, priority=5)

    batch = q.get_next_batch(batch_size=10)
    topics = [msg["topic"] for msg in batch]

    assert topics == ["first", "second", "third"]


# ---------------------------------------------------------------------------
# Eviction policy
# ---------------------------------------------------------------------------

def test_eviction_drops_lowest_priority_when_full(db_connection):
    """
    When the queue is at capacity, adding a higher-priority message should
    evict the lowest-priority existing message to make room.

    This is the core promise of priority-based eviction: a critical alert
    can always get into the queue, even when it is full of low-priority data.
    """
    q = make_queue(db_connection, max_size=3)
    q.add_message("low-1", b"1", qos=1, priority=1)
    q.add_message("low-2", b"2", qos=1, priority=1)
    q.add_message("low-3", b"3", qos=1, priority=1)

    # Queue is now full at max_size=3 with all priority-1 messages.
    # Adding a priority-10 message should evict one priority-1 message.
    added = q.add_message("critical", b"!", qos=1, priority=10)
    assert added is True

    batch = q.get_next_batch(batch_size=10)
    topics = [msg["topic"] for msg in batch]

    assert "critical" in topics
    assert len(topics) == 3  # still 3, one low was evicted


def test_low_priority_message_evicts_oldest_when_queue_full_of_higher_priority(db_connection):
    """
    When the queue is full and every existing message has higher priority than
    the incoming message, the eviction policy falls back to dropping the oldest
    message to make room — add_message still returns True.

    This is the designed fallback: the policy prefers to drop lower-priority
    messages, but when none exist, it drops the oldest rather than refusing the
    incoming message entirely. The queue always accepts new messages as long as
    it is not empty.
    """
    q = make_queue(db_connection, max_size=3)
    q.add_message("oldest-high", b"1", qos=1, priority=10)
    q.add_message("middle-high", b"2", qos=1, priority=10)
    q.add_message("newest-high", b"3", qos=1, priority=10)

    # Adding a low-priority message to a full high-priority queue:
    # the fallback evicts the OLDEST high-priority message.
    added = q.add_message("low-incoming", b"x", qos=1, priority=1)
    assert added is True

    batch = q.get_next_batch(batch_size=10)
    topics = [msg["topic"] for msg in batch]

    # The oldest was evicted; the new low-priority message got in
    assert "oldest-high" not in topics
    assert "low-incoming" in topics
    assert len(topics) == 3


def test_eviction_fallback_drops_oldest_at_equal_priority(db_connection):
    """
    When the queue is full and the incoming message has the same priority as
    all existing messages, the oldest message is evicted.

    The fallback path exists to prevent the queue from refusing all new
    messages once it fills up with equal-priority data.
    """
    q = make_queue(db_connection, max_size=3)
    q.add_message("oldest", b"1", qos=1, priority=5)
    q.add_message("middle", b"2", qos=1, priority=5)
    q.add_message("newest", b"3", qos=1, priority=5)

    added = q.add_message("incoming", b"4", qos=1, priority=5)
    assert added is True

    batch = q.get_next_batch(batch_size=10)
    topics = [msg["topic"] for msg in batch]

    # The oldest message should have been evicted to make room
    assert "oldest" not in topics
    assert "incoming" in topics
    assert len(topics) == 3


# ---------------------------------------------------------------------------
# Payload storage
# ---------------------------------------------------------------------------

def test_binary_payload_survives_roundtrip(db_connection):
    """
    Binary payloads must be stored and retrieved byte-for-byte unchanged.

    This verifies the BLOB column type is working correctly. The v0.2.0 bug
    stored payloads as str(payload), which would corrupt these bytes into
    the string representation "b'\\x00\\xff'".
    """
    binary_payload = bytes(range(256))  # all 256 byte values
    q = make_queue(db_connection)
    q.add_message("binary/topic", binary_payload, qos=1)

    batch = q.get_next_batch(batch_size=1)
    assert batch[0]["payload"] == binary_payload


def test_string_payload_encoded_as_utf8(db_connection):
    """String payloads must be encoded to UTF-8 bytes before storage."""
    q = make_queue(db_connection)
    q.add_message("string/topic", '{"value": 23.5}', qos=1)

    batch = q.get_next_batch(batch_size=1)
    assert batch[0]["payload"] == b'{"value": 23.5}'


def test_retain_flag_survives_roundtrip(db_connection):
    """The retain flag must be stored and retrieved correctly."""
    q = make_queue(db_connection)
    q.add_message("r/topic", b"val", qos=1, retain=True)

    batch = q.get_next_batch(batch_size=1)
    assert batch[0]["retain"] is True


# ---------------------------------------------------------------------------
# Statistics
# ---------------------------------------------------------------------------

def test_stats_capacity_percent_is_accurate(db_connection):
    """capacity_used_percent must accurately reflect how full the queue is."""
    q = make_queue(db_connection, max_size=10)

    for i in range(4):
        q.add_message(f"t/{i}", b"x", qos=0)

    stats = q.get_stats()
    assert stats["total_messages"] == 4
    assert stats["capacity_used_percent"] == pytest.approx(40.0)


def test_stats_empty_queue(db_connection):
    """An empty queue should report zero messages and no oldest age."""
    q = make_queue(db_connection)
    stats = q.get_stats()

    assert stats["total_messages"] == 0
    assert stats["capacity_used_percent"] == pytest.approx(0.0)
    assert stats["oldest_message_age_seconds"] is None


def test_stats_by_priority_groups_correctly(db_connection):
    """by_priority must show accurate counts per priority level."""
    q = make_queue(db_connection)
    q.add_message("a", b"x", qos=1, priority=10)
    q.add_message("b", b"x", qos=1, priority=10)
    q.add_message("c", b"x", qos=1, priority=1)

    stats = q.get_stats()
    assert stats["by_priority"][10] == 2
    assert stats["by_priority"][1] == 1


def test_clear_empties_queue(db_connection):
    """clear() must remove all messages."""
    q = make_queue(db_connection)
    q.add_message("a", b"x", qos=1)
    q.add_message("b", b"x", qos=1)

    q.clear()

    stats = q.get_stats()
    assert stats["total_messages"] == 0


# ---------------------------------------------------------------------------
# Standalone mode (no shared connection)
# ---------------------------------------------------------------------------

def test_standalone_mode_creates_own_connection(tmp_path):
    """
    OfflineQueue must work correctly when no connection is passed,
    creating its own SQLite connection to the given db_path.
    """
    db_file = str(tmp_path / "test_standalone.db")
    q = OfflineQueue(db_path=db_file, max_size=5)

    q.add_message("standalone/topic", b"hello", qos=1)
    batch = q.get_next_batch(batch_size=10)

    assert len(batch) == 1
    assert batch[0]["topic"] == "standalone/topic"

    q.close()
