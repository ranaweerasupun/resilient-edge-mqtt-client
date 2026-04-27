"""
robmqtt — a resilient MQTT client for edge devices.

Public API:
    ProductionMQTTClient — the main client class
    Config               — configuration loader
    OfflineQueue         — SQLite-backed offline message queue
    InflightTracker      — tracks sent-but-unacknowledged messages
    ProductionLogger     — rotating-file logger with metrics
"""

from .production_client import ProductionMQTTClient
from .config import Config
from .offline_queue import OfflineQueue
from .inflight_tracker import InflightTracker
from .production_logger import ProductionLogger

__version__ = "1.0.0"

__all__ = [
    "ProductionMQTTClient",
    "Config",
    "OfflineQueue",
    "InflightTracker",
    "ProductionLogger",
]