"""
test_topic_matching.py — parametrized tests for ProductionMQTTClient._topic_matches().

_topic_matches() is a static method implementing the MQTT wildcard specification.
It is a pure function — no I/O, no state, no dependencies — which makes it the
ideal candidate for parametrized testing. One table of (pattern, topic, expected)
tuples covers every edge case without any boilerplate.

The MQTT wildcard rules are:
  '+' matches exactly one topic level.
  '#' matches zero or more topic levels and must be the last character.
  All other characters must match exactly, including '/'.

The tricky cases — the ones that are worth thinking hard about — are marked
with comments so the intent is explicit.
"""

import pytest

from robmqtt.production_client import ProductionMQTTClient

# Alias for readability in the parametrize table
matches = ProductionMQTTClient._topic_matches


@pytest.mark.parametrize("pattern,topic,expected", [

    # ------------------------------------------------------------------
    # Exact matches — no wildcards, pure literal comparison
    # ------------------------------------------------------------------
    ("sensors/temp",        "sensors/temp",        True),
    ("sensors/temp",        "sensors/humidity",    False),
    ("a/b/c/d",             "a/b/c/d",             True),
    ("a/b/c/d",             "a/b/c/e",             False),

    # ------------------------------------------------------------------
    # Single-level wildcard: '+'
    # '+' stands in for exactly one level — no more, no less.
    # ------------------------------------------------------------------
    ("sensors/+/temp",      "sensors/room1/temp",     True),
    ("sensors/+/temp",      "sensors/room2/temp",     True),
    ("sensors/+/temp",      "sensors/room1/humidity", False),  # wrong last level
    ("sensors/+/temp",      "sensors/room1/b/temp",   False),  # + cannot span two levels
    ("sensors/+",           "sensors/room1",           True),
    ("sensors/+",           "sensors",                 False), # + needs one level, topic has zero
    ("+/temp",              "sensors/temp",            True),
    ("+/+",                 "a/b",                     True),
    ("+/+",                 "a/b/c",                   False), # two +'s, three levels

    # ------------------------------------------------------------------
    # Multi-level wildcard: '#'
    # '#' matches the current level and all sub-levels.
    # The subtle case: sensors/# also matches sensors (zero sub-levels).
    # ------------------------------------------------------------------
    ("sensors/#",           "sensors/temp",            True),
    ("sensors/#",           "sensors/room1/temp",      True),
    ("sensors/#",           "sensors/room1/floor/temp",True),
    ("sensors/#",           "sensors",                 True),  # the tricky one: zero sub-levels
    ("sensors/#",           "devices/001",             False), # different root

    # '#' alone matches absolutely everything
    ("#",                   "any/topic/at/all",        True),
    ("#",                   "single",                  True),
    ("#",                   "a/b/c/d/e/f",             True),

    # ------------------------------------------------------------------
    # Mixed wildcards
    # ------------------------------------------------------------------
    ("sensors/+/#",         "sensors/room1/temp",      True),
    ("sensors/+/#",         "sensors/room1/a/b/c",     True),
    ("sensors/+/#",         "sensors/room1",           True),  # # after + covers zero levels
    ("+/#",                 "a/b/c",                   True),
    ("+/#",                 "a",                       True),

    # ------------------------------------------------------------------
    # No false positives — a common source of bugs
    # Making sure exact topics don't accidentally match longer patterns.
    # ------------------------------------------------------------------
    ("sensors/temp",        "sensors/temp/extra",      False),
    ("sensors/temp/value",  "sensors/temp",            False),

])
def test_topic_matches(pattern, topic, expected):
    """
    Each row in the parametrize table is an independent test.
    If this test fails, pytest reports exactly which (pattern, topic) pair
    produced the wrong result, making failures easy to diagnose.
    """
    result = matches(pattern, topic)
    assert result == expected, (
        f"_topic_matches({pattern!r}, {topic!r}) returned {result}, expected {expected}"
    )
