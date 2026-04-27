# production_logger.py
# -*- coding: utf-8 -*-
"""
Logging for the production MQTT client.

Wraps Python's standard logging with rotating file handlers, a structured
key-value context format, and a separate JSON Lines metrics file for
ingestion by monitoring tools (Prometheus, Loki, etc.).

v1.0.0: Type hints added throughout.
"""


import logging
import logging.handlers
import os
import json
from datetime import datetime
from typing import Any
import threading


class ProductionLogger:
    """
    Logger with file rotation, structured context, and metrics output.

    Usage:
        logger = ProductionLogger("mqtt_client", log_dir="/var/log/mqtt")
        logger.info("Connected to broker", broker="mqtt.example.com")
        logger.error("Publish failed", topic="sensors/temp", error=str(e))
    """

    def __init__(
        self,
        name: str,
        log_dir: str = "./logs",
        log_level: int = logging.INFO,
        max_bytes: int = 10 * 1024 * 1024,
        backup_count: int = 5,
    ) -> None:
        """
        Initialise the logger.

        max_bytes controls when the log file rotates (default 10 MB).
        backup_count is how many rotated files to keep before the oldest
        is deleted (default 5, so up to 50 MB of log history).
        """
        self.name = name
        self.log_dir = log_dir
        self.lock = threading.Lock()

        os.makedirs(log_dir, exist_ok=True)

        self.logger = logging.getLogger(name)
        self.logger.setLevel(log_level)

        if self.logger.handlers:
            return

        log_file = os.path.join(log_dir, f"{name}.log")
        file_handler = logging.handlers.RotatingFileHandler(
            log_file,
            maxBytes=max_bytes,
            backupCount=backup_count,
        )
        file_handler.setLevel(log_level)

        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.WARNING)

        formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - [%(threadName)s] - %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )

        file_handler.setFormatter(formatter)
        console_handler.setFormatter(formatter)

        self.logger.addHandler(file_handler)
        self.logger.addHandler(console_handler)

        self.metrics_file = os.path.join(log_dir, f"{name}_metrics.jsonl")

    def _log_with_context(self, level: int, message: str, **context: Any) -> None:
        """Format context kwargs as key=value pairs appended to the message."""
        if context:
            context_str = " | ".join(f"{k}={v}" for k, v in context.items())
            full_message = f"{message} | {context_str}"
        else:
            full_message = message
        self.logger.log(level, full_message)

    def debug(self, message: str, **context: Any) -> None:
        """Log at DEBUG level with optional structured context."""
        self._log_with_context(logging.DEBUG, message, **context)

    def info(self, message: str, **context: Any) -> None:
        """Log at INFO level with optional structured context."""
        self._log_with_context(logging.INFO, message, **context)

    def warning(self, message: str, **context: Any) -> None:
        """Log at WARNING level with optional structured context."""
        self._log_with_context(logging.WARNING, message, **context)

    def error(self, message: str, **context: Any) -> None:
        """Log at ERROR level with optional structured context."""
        self._log_with_context(logging.ERROR, message, **context)

    def critical(self, message: str, **context: Any) -> None:
        """Log at CRITICAL level with optional structured context."""
        self._log_with_context(logging.CRITICAL, message, **context)

    def log_metric(self, metric_name: str, value: float, **tags: Any) -> None:
        """
        Write a numeric metric to the structured metrics file.

        Each metric is appended as a JSON line. Tags are arbitrary key-value
        pairs attached to the measurement (e.g. topic, qos, queue_type).
        """
        with self.lock:
            metric = {
                "timestamp": datetime.now().isoformat(),
                "metric": metric_name,
                "value": value,
                **tags,
            }
            with open(self.metrics_file, "a") as f:
                f.write(json.dumps(metric) + "\n")

    def log_event(self, event_type: str, **details: Any) -> None:
        """
        Record a structured event to both the main log and the metrics file.

        Use for significant state transitions: connections, disconnections,
        queue overflow, process startup, etc.
        """
        event = {
            "timestamp": datetime.now().isoformat(),
            "event": event_type,
            **details,
        }
        self.info(f"EVENT: {event_type}", **details)
        with self.lock:
            with open(self.metrics_file, "a") as f:
                f.write(json.dumps(event) + "\n")


# Global logger instance (singleton pattern)
_global_logger: Any = None
_logger_lock = threading.Lock()


def get_logger(name: str = "mqtt_client", **kwargs: Any) -> ProductionLogger:
    """
    Return the shared logger instance, creating it on first call.

    All modules should obtain their logger through this function rather than
    instantiating ProductionLogger directly. This ensures log output from
    every part of the application goes through the same handlers and into
    the same files.
    """
    global _global_logger

    with _logger_lock:
        if _global_logger is None:
            _global_logger = ProductionLogger(name, **kwargs)
        return _global_logger
