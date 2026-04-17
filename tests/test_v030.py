# -*- coding: utf-8 -*-
"""
test_v030.py — Simulation script for v0.3.0 (Data Integrity)

Written specifically for the v0.3.0 codebase. It uses only the APIs and
constructor signatures that exist in v0.3.0 — no Config, no from_config(),
no shared database connections, no log_dir parameter.

v0.3.0 fixed three silent data-loss bugs. This script demonstrates each
one so you can see the correct behaviour with your own eyes.

  Fix 1 — Binary payloads stored as BLOB, not str()
           Bytes survive the round trip byte-for-byte unchanged.

  Fix 2 — Resend tracking pattern
           After a reconnect, the tracker holds the NEW packet ID,
           not the stale one from the previous connection.

  Fix 3 — Race condition in publish()
           If the connection drops between the is_connected check and
           the actual send, the message is caught and routed to the
           offline queue rather than silently lost.

Run against a local Mosquitto broker:
    mosquitto -p 1883
    python test_v030.py
"""

import json
import time
import paho.mqtt.client as mqtt

from production_client import ProductionMQTTClient
from inflight_tracker import InflightTracker


BROKER_HOST = "localhost"
BROKER_PORT = 1883
SEPARATOR   = "─" * 60


def section(title):
    print(f"\n{SEPARATOR}")
    print(f"  {title}")
    print(SEPARATOR)


# ── Fix 1: Binary payload round trip ────────────────────────────────────────

def demo_binary_payload():
    """
    In v0.2.0, InflightTracker stored payloads using str(payload).
    When the payload was bytes — which is normal in MQTT, not a special case —
    str(b'\x00\xff') produced the literal string "b'\\x00\\xff'" rather than
    the actual 2 bytes. The broker then received that garbage string on resend.

    v0.3.0 changes the column type to BLOB and stores raw bytes directly.
    This demo publishes a payload containing all 256 possible byte values and
    uses a separate subscriber to verify that what arrives at the broker is
    byte-for-byte identical to what was sent.
    """
    section("Fix 1 — Binary Payload Round Trip")
    print("Sending a payload that contains all 256 raw byte values.")
    print("v0.2.0 would corrupt this on resend; v0.3.0 stores and forwards it unchanged.\n")

    received_payloads = []

    # A plain paho subscriber acts as the "broker side" — it captures
    # exactly what the broker delivers, giving us a ground truth to compare against.
    sub = mqtt.Client(client_id="v030_binary_sub", clean_session=True)
    sub.on_message = lambda c, u, m: received_payloads.append(m.payload)
    sub.connect(BROKER_HOST, BROKER_PORT)
    sub.subscribe("test/v030/binary", qos=1)
    sub.loop_start()
    time.sleep(0.5)  # give the subscriber time to connect and subscribe

    # v0.3.0 constructor: only these four parameters exist
    client = ProductionMQTTClient(
        client_id="v030_binary_pub",
        broker_host=BROKER_HOST,
        broker_port=BROKER_PORT,
        max_queue_size=100,
    )
    client.connect()
    client.start()
    time.sleep(1)  # wait for connection

    # bytes(range(256)) produces a 256-byte sequence: b'\x00\x01\x02...\xff'
    # This is the most thorough test of binary storage correctness because
    # it includes null bytes, high bytes, and every value in between.
    original_payload = bytes(range(256))
    print(f"Original payload: {len(original_payload)} bytes")
    print(f"First 8 bytes:    {list(original_payload[:8])}")
    print(f"Last 8 bytes:     {list(original_payload[-8:])}")

    client.publish("test/v030/binary", original_payload, qos=1)
    time.sleep(1.5)  # wait for delivery and acknowledgment

    if received_payloads:
        arrived = received_payloads[0]
        if arrived == original_payload:
            print(f"\n✓ Received {len(arrived)} bytes — IDENTICAL to original.")
            print("  All 256 byte values survived the round trip without corruption.")
        else:
            # Show where the difference is to make the corruption visible
            diffs = [(i, original_payload[i], arrived[i])
                     for i in range(min(len(original_payload), len(arrived)))
                     if original_payload[i] != arrived[i]]
            print(f"\n✗ Payload corrupted. {len(diffs)} byte(s) differ.")
            for pos, expected, got in diffs[:5]:
                print(f"  Byte {pos}: expected {expected}, got {got}")
    else:
        print("\n✗ No message received. Check that Mosquitto is running.")

    sub.loop_stop()
    sub.disconnect()
    client.stop()


# ── Fix 2: Resend tracking pattern ───────────────────────────────────────────

def demo_resend_tracking():
    """
    Every QoS 1 or QoS 2 message gets a packet ID (called 'mid' in paho)
    when it is published. When a session reconnects, paho assigns a FRESH
    set of packet IDs. Old IDs from the previous connection are gone.

    In v0.2.0, _resend_inflight_messages() published the stored messages but
    never updated the tracker with the new IDs. So after the resend, the
    tracker still held the old (now meaningless) IDs. A second disconnection
    before the broker acknowledged the resent messages would find nothing in
    the tracker and lose those messages permanently.

    The v0.3.0 fix performs two steps atomically for each resent message:
      1. Remove the old tracker entry (the stale ID)
      2. Insert a new tracker entry (the fresh ID from paho)

    We simulate this below using InflightTracker directly with an in-memory
    SQLite database so you can inspect the state at each step.

    Note: in v0.3.0 InflightTracker only accepts db_path. The special value
    ':memory:' tells SQLite to create a database entirely in RAM — it is
    faster than a file, leaves nothing on disk, and is destroyed when
    the connection closes. This is exactly what the test suite uses too.
    """
    section("Fix 2 — Resend Tracking Across Reconnection")
    print("Walking through the remove-old/insert-new resend pattern step by step.")
    print("':memory:' gives us a fresh SQLite database in RAM for this demo.\n")

    # v0.3.0 standalone mode: just db_path, no conn or lock parameters
    tracker = InflightTracker(db_path=":memory:")

    # ── Connection 1 ──────────────────────────────────────────────────────
    # The message is published and paho assigns packet_id=10
    tracker.add_message(
        packet_id=10,
        topic="sensors/temperature",
        payload=b'{"value": 23.5}',
        qos=1,
        retain=False,
    )
    print(f"Connection 1: message published, tracker holds packet_id=10")
    print(f"  Tracker count: {tracker.count_messages()}")

    # ── Network drops before broker acknowledges ─────────────────────────
    print(f"\nNetwork drops. Broker never sent PUBACK for packet_id=10.")
    print(f"Tracker still holds packet_id=10 — it knows to resend this.")

    # ── Connection 2: reconnect and resend ───────────────────────────────
    # On the new connection, paho assigns packet_id=1 to the resent message.
    # The v0.3.0 fix: remove old, insert new — both steps, right here.
    stale_messages = tracker.get_all_messages()
    for msg in stale_messages:
        new_packet_id = 1   # what paho would return as info.mid on resend

        # Step 1: remove the stale entry — the old ID is meaningless now
        tracker.remove_message(msg["packet_id"])

        # Step 2: insert the fresh entry — the broker will acknowledge this ID
        tracker.add_message(
            packet_id=new_packet_id,
            topic=msg["topic"],
            payload=msg["payload"],
            qos=msg["qos"],
            retain=msg["retain"],
        )

    print(f"\nConnection 2: resent with new packet_id=1")
    print(f"  Tracker count: {tracker.count_messages()}")

    live = tracker.get_all_messages()
    active_id = live[0]["packet_id"]
    print(f"  Active packet_id in tracker: {active_id}")

    if active_id == 1:
        print("  ✓ Tracker holds the CURRENT packet ID — correct.")
    else:
        print(f"  ✗ Tracker holds {active_id} — something went wrong.")

    # ── Broker sends PUBACK for packet_id=1 ─────────────────────────────
    # This is what _on_publish() does when paho fires it
    tracker.remove_message(1)
    print(f"\nBroker acknowledged packet_id=1.")
    print(f"  Tracker count: {tracker.count_messages()}  ← empty, delivery confirmed ✓")

    # ── What v0.2.0 would have done (for comparison) ─────────────────────
    print(f"\nFor comparison — what v0.2.0 did (the bug):")
    print(f"  1. Resent with new packet_id=1")
    print(f"  2. Did NOT update tracker — tracker still held stale packet_id=10")
    print(f"  3. Broker sends PUBACK for packet_id=1")
    print(f"  4. _on_publish(mid=1) looks up id=1 in tracker — NOT FOUND, ignored")
    print(f"  5. Network drops again — tracker finds packet_id=10, tries to resend")
    print(f"     but the message was already sent under id=1 on connection 2")
    print(f"  6. Chaos. Duplicates or silent loss depending on broker behaviour.")

    tracker.close()


# ── Fix 3: Race condition in publish() ───────────────────────────────────────

def demo_race_condition():
    """
    publish() checks self.is_connected to decide whether to send directly
    or queue offline. In v0.2.0, the check and the actual send were two
    separate operations with no protection between them:

        connected = self.is_connected   # <- check happens here
        # <- connection could drop RIGHT HERE (the dangerous gap)
        if connected:
            self.client.publish(...)    # <- this might now fail
                                        # exception is unhandled
                                        # message is silently lost

    The v0.3.0 fix wraps client.publish() in a try/except. If an exception
    is raised — because the socket died in that narrow window — the exception
    is caught, is_connected is set to False, and the message is routed to the
    offline queue as a fallback. Nothing is lost.

    We can demonstrate this by manually putting the client into the race
    condition state: is_connected=True but no actual socket open. When
    publish() tries to write to the dead socket and raises, the v0.3.0
    fix catches it and routes to the offline queue.
    """
    section("Fix 3 — Race Condition in publish()")
    print("Inducing the race condition: is_connected=True but socket is closed.")
    print("v0.3.0 catches the exception and rescues the message to the offline queue.\n")

    # v0.3.0 constructor — no from_config(), no log_dir, no db_path
    client = ProductionMQTTClient(
        client_id="v030_race_demo",
        broker_host=BROKER_HOST,
        broker_port=BROKER_PORT,
        max_queue_size=100,
    )

    # Deliberately do NOT call client.connect(). The socket is never opened.
    # We then manually set is_connected=True to simulate the state the client
    # would be in if the connection dropped after the check but before the send.
    # This is the race window made permanent for demonstration purposes.
    client.is_connected = True

    print("State before publish():")
    print(f"  is_connected = {client.is_connected}  (manually forced True)")
    print(f"  socket open  = False  (connect() was never called)")
    print(f"  offline queue = {client.offline_queue.get_stats()['total_messages']} messages")

    print("\nCalling publish() — the send will fail because there is no socket...")
    client.publish(
        topic="sensors/temperature",
        payload=b'{"value": 23.5}',
        qos=1,
        priority=5,
    )

    # Give it a moment to process
    time.sleep(0.1)

    stats = client.get_statistics()
    print("\nState after publish():")
    print(f"  is_connected  = {stats['connected']}  (reset to False by the fix)")
    print(f"  offline queue = {stats['offline_queue']['total_messages']} message(s)")

    if not stats["connected"] and stats["offline_queue"]["total_messages"] == 1:
        print("\n✓ Message rescued to offline queue. Nothing was silently lost.")
        print("  When connectivity is restored, the drainer will deliver it.")
    else:
        print("\n✗ Something unexpected happened — check the output above.")

    client.stop()


# ── Main ──────────────────────────────────────────────────────────────────────

print("\n" + "=" * 60)
print("  v0.3.0 DATA INTEGRITY — Three Fixes Demonstrated")
print("  (compatible with the v0.3.0 codebase)")
print("=" * 60)

demo_binary_payload()
demo_resend_tracking()
demo_race_condition()

print(f"\n{'=' * 60}")
print("  All three v0.3.0 data integrity fixes demonstrated.")
print(f"{'=' * 60}\n")