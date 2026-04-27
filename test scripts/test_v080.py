# -*- coding: utf-8 -*-
"""
test_v080.py — Guide to the v0.8.0 Test Suite

v0.8.0 introduced a proper pytest suite — 73 tests across five files.
This script is different from the other demo scripts: instead of connecting
to a broker and showing runtime behaviour, it explains how to run the tests,
what each file covers, and why specific tests exist.

Run this script to get an annotated guide to the test suite:
    python test_v080.py

To actually run the tests:
    pip install -r dev-requirements.txt  (or: pip install ".[dev]")
    pytest                               (all unit tests, ~4 seconds)
    pytest -v                            (verbose — see every test name)
    pytest -m integration                (needs Mosquitto on localhost:1883)
"""

import subprocess
import sys
import os


SEPARATOR = "─" * 60


def section(title):
    print(f"\n{SEPARATOR}")
    print(f"  {title}")
    print(SEPARATOR)


def show_file(path, description):
    print(f"\n  {path}")
    print(f"  {description}")
    if os.path.exists(path):
        with open(path) as f:
            lines = f.readlines()
        test_lines = [l.strip() for l in lines if l.strip().startswith("def test_")]
        print(f"  Contains {len(test_lines)} test(s):")
        for t in test_lines[:8]:
            name = t.replace("def ", "").split("(")[0]
            print(f"    • {name}")
        if len(test_lines) > 8:
            print(f"    ... and {len(test_lines) - 8} more")
    else:
        print(f"  (file not found — run from the project root directory)")


# ── Overview ──────────────────────────────────────────────────────────────────

print("\n" + "=" * 60)
print("  v0.8.0 TEST SUITE — Annotated Guide")
print("=" * 60)

print("""
The test suite has 73 tests. None of them require a running broker except
those in test_integration.py (which skip automatically if no broker is found).
All unit and component tests run in about 4 seconds on any machine.

The test philosophy for an edge device library is different from a web app:
bugs found in the field can mean a physical site visit to update firmware.
The tests specifically target the scenarios that are hard to spot in manual
testing — binary payloads, second disconnects, wildcard edge cases.
""")


# ── Test files ────────────────────────────────────────────────────────────────

section("The Five Test Files")

show_file(
    "tests/test_offline_queue.py",
    "28 unit tests — OfflineQueue storage, priority ordering, eviction.",
)

show_file(
    "tests/test_inflight_tracker.py",
    "12 unit tests — InflightTracker storage, BLOB fix, schema migration, resend pattern.",
)

show_file(
    "tests/test_topic_matching.py",
    "33 parametrized cases — every MQTT wildcard rule, including the tricky ones.",
)

show_file(
    "tests/test_production_client.py",
    "16 tests — paho mocked, routing logic, health check HTTP server.",
)

show_file(
    "tests/test_integration.py",
    "3 tests — full connect/publish/subscribe cycle against a real broker.",
)


# ── Key tests explained ───────────────────────────────────────────────────────

section("Three Tests Worth Understanding")

print("""
1. test_schema_migration_detects_and_rebuilds_old_text_schema
   (in test_inflight_tracker.py)

   This test manually creates a SQLite table with the old TEXT payload column
   from v0.2.0, inserts a corrupted row, then initialises an InflightTracker
   against that database. It verifies that the tracker detects the old schema,
   drops the table, and rebuilds it with the correct BLOB column. This is the
   kind of test that would have caught the v0.2.0 bug before it shipped.

2. test_topic_matches[sensors/#-sensors-True]
   (in test_topic_matching.py)

   This is the "tricky case" from the v0.7.0 teaching doc. Many people assume
   that sensors/# requires at least one sub-level. The MQTT spec says '#'
   matches zero or more levels, so sensors/# does match sensors. The
   parametrize table makes this one case among 33, clearly labelled.

3. test_publish_exception_routes_to_offline_queue
   (in test_production_client.py)

   This test uses MagicMock to make client.publish() raise a ConnectionError,
   simulating the race condition window. It verifies that the exception is
   caught, is_connected becomes False, and the message ends up in the offline
   queue. Without mocking, this timing-dependent bug is nearly impossible to
   trigger reliably in a test.
""")


# ── Running the tests ─────────────────────────────────────────────────────────

section("Running the Tests")

print("Useful pytest invocations:\n")
commands = [
    ("pytest",                          "Run all unit and component tests"),
    ("pytest -v",                       "Verbose: show each test name"),
    ("pytest -q",                       "Quiet: just pass/fail count"),
    ("pytest tests/test_topic_matching.py",   "Run one file"),
    ("pytest -k binary",                "Run tests with 'binary' in the name"),
    ("pytest -m integration",           "Run integration tests (needs broker)"),
    ("pytest --tb=short",               "Shorter tracebacks on failure"),
]
for cmd, description in commands:
    print(f"  {cmd}")
    print(f"      → {description}\n")


# ── Live run ──────────────────────────────────────────────────────────────────

section("Live Test Run (unit tests only)")

print("Attempting to run the test suite now...\n")

# Try running pytest via subprocess
result = subprocess.run(
    [sys.executable, "-m", "pytest", "tests/",
     "--ignore=tests/test_integration.py", "-q", "--tb=short"],
    capture_output=True,
    text=True,
)

if result.returncode == 0:
    print(result.stdout)
    print("✓ All tests passed.")
elif "no tests ran" in result.stdout or "error" in result.stderr.lower():
    print("Could not run pytest from this directory.")
    print("Make sure you are in the project root and pytest is installed:")
    print("  pip install -r dev-requirements.txt")
    print("  pytest")
else:
    print(result.stdout)
    if result.returncode != 0:
        print("\n(Some tests failed. See output above.)")

print(f"\n{SEPARATOR}")
print("  v0.8.0 test suite guide complete.")
print(SEPARATOR)
