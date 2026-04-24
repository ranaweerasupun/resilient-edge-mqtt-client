"""
conftest.py — shared pytest fixtures for the production MQTT client test suite.

pytest automatically discovers and makes these fixtures available to every test
file in the tests/ directory without any explicit import. Fixtures are functions
that set up a resource, yield it to the test, and then clean it up afterward —
the yield is the hand-off point between setup and teardown.

Two key decisions are made here:

1. The logger singleton is initialised once for the entire test session at
   WARNING level, writing to a temporary directory. This prevents test runs
   from creating a ./logs/ directory in the project root and keeps test
   output clean. The autouse=True parameter means every test gets this
   automatically, even ones that don't explicitly request it.

2. db_connection provides a fresh in-memory SQLite connection per test.
   SQLite's ':memory:' mode creates a database that lives entirely in RAM
   and is destroyed when the connection closes. Combined with function scope
   (the default), this means each test starts with a clean slate — no data
   leaking from one test to the next, and no teardown SQL needed.
"""

import logging
import sqlite3
import threading
import socket

import pytest


# ---------------------------------------------------------------------------
# Logger setup — session-scoped so it runs once for all tests
# ---------------------------------------------------------------------------

@pytest.fixture(scope="session", autouse=True)
def configure_logger(tmp_path_factory):
    """
    Initialise the ProductionLogger singleton before any test runs.

    Using scope="session" means this fixture runs exactly once per pytest
    invocation, no matter how many tests there are. Using autouse=True means
    it runs for every test without needing to be listed as a parameter.

    We point the logger at a temporary directory and set the level to WARNING
    so that INFO and DEBUG messages from the library don't appear in test
    output. If a test genuinely fails, ERROR-level messages will still surface.
    """
    tmp_log_dir = tmp_path_factory.mktemp("mqtt_logs")
    from production_logger import get_logger
    get_logger(
        "mqtt_client",
        log_dir=str(tmp_log_dir),
        log_level=logging.WARNING,
    )


# ---------------------------------------------------------------------------
# Database fixtures — function-scoped (fresh per test)
# ---------------------------------------------------------------------------

@pytest.fixture
def db_connection():
    """
    Provide a fresh in-memory SQLite connection and threading lock per test.

    Both InflightTracker and OfflineQueue accept a shared connection and lock
    in their constructors (added in v0.4.0). Passing ':memory:' gives each
    test a completely isolated database with zero setup cost and no disk I/O.

    The connection is closed after each test via the yield/finally pattern,
    which pytest translates into automatic teardown even if the test fails.
    """
    conn = sqlite3.connect(":memory:", check_same_thread=False)
    lock = threading.Lock()
    yield conn, lock
    conn.close()


# ---------------------------------------------------------------------------
# Integration test helpers
# ---------------------------------------------------------------------------

def broker_is_running(host="localhost", port=1883, timeout=1.0):
    """
    Return True if something is listening on the given host:port.

    Used by integration tests to decide whether to run or skip. A test
    decorated with @pytest.mark.integration and a skipif using this function
    will run when a broker is available (e.g. in CI with Mosquitto installed)
    and skip gracefully on developer machines that don't have one running.
    """
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except (ConnectionRefusedError, OSError):
        return False


def find_free_port():
    """
    Ask the operating system for a free TCP port and return its number.

    Binding to port 0 causes the OS to assign an available port. We immediately
    close the socket so the port is free for our health check server to bind to.
    There is a small TOCTOU window between closing and re-binding, but in a
    test environment this is an acceptable tradeoff for avoiding hardcoded ports.
    """
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("", 0))
        return s.getsockname()[1]
