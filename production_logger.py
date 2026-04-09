# production_logger.py
# -*- coding: utf-8 -*-
"""
Logging for the production MQTT client.

Wraps Python's standard logging with rotating file handlers, a structured
key-value context format, and a separate JSON Lines metrics file for
ingestion by monitoring tools (Prometheus, Loki, etc.).
"""

import logging
import logging.handlers
import os
import json
from datetime import datetime
import threading

class ProductionLogger:
    """
    Logger with file rotation, structured context, and metrics output.

    Usage:
        logger = ProductionLogger("mqtt_client", log_dir="/var/log/mqtt")
        logger.info("Connected to broker", broker="mqtt.example.com")
        logger.error("Publish failed", topic="sensors/temp", error=str(e))
    """
    
    def __init__(self, name, log_dir="./logs", log_level=logging.INFO, 
                 max_bytes=10*1024*1024, backup_count=5):
        """
        Initialise the logger.

        max_bytes controls when the log file rotates (default 10 MB).
        backup_count is how many rotated files to keep before the oldest
        is deleted (default 5, so up to 50 MB of log history).
        """
        self.name = name
        self.log_dir = log_dir
        self.lock = threading.Lock()
        
        # Create log directory if it doesn't exist
        os.makedirs(log_dir, exist_ok=True)
        
        # Create logger
        self.logger = logging.getLogger(name)
        self.logger.setLevel(log_level)
        
        # Prevent duplicate handlers if logger already configured
        if self.logger.handlers:
            return
        
        # File handler with rotation
        log_file = os.path.join(log_dir, f"{name}.log")
        file_handler = logging.handlers.RotatingFileHandler(
            log_file,
            maxBytes=max_bytes,
            backupCount=backup_count
        )
        file_handler.setLevel(log_level)
        
        # Console handler (for important messages)
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.WARNING)  # Only warnings and above to console
        
        # Formatter with timestamp, level, thread, and message
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - [%(threadName)s] - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        
        file_handler.setFormatter(formatter)
        console_handler.setFormatter(formatter)
        
        # Add handlers
        self.logger.addHandler(file_handler)
        self.logger.addHandler(console_handler)
        
        # Metrics file for structured data
        self.metrics_file = os.path.join(log_dir, f"{name}_metrics.jsonl")
    
    def _log_with_context(self, level, message, **context):
        """Format context kwargs as key=value pairs appended to the message."""
        if context:
            context_str = " | ".join(f"{k}={v}" for k, v in context.items())
            full_message = f"{message} | {context_str}"
        else:
            full_message = message
        
        self.logger.log(level, full_message)
    
    def debug(self, message, **context):
        """Log at DEBUG level."""
        self._log_with_context(logging.DEBUG, message, **context)
    
    def info(self, message, **context):
        """Log at INFO level."""
        self._log_with_context(logging.INFO, message, **context)
    
    def warning(self, message, **context):
        """Log at WARNING level."""
        self._log_with_context(logging.WARNING, message, **context)
    
    def error(self, message, **context):
        """Log at ERROR level."""
        self._log_with_context(logging.ERROR, message, **context)
    
    def critical(self, message, **context):
        """Log at CRITICAL level."""
        self._log_with_context(logging.CRITICAL, message, **context)
    
    def log_metric(self, metric_name, value, **tags):
        """
        Write a numeric metric to the structured metrics file.

        Each metric is appended as a JSON line. Tags are arbitrary key-value
        pairs attached to the measurement (e.g. topic, qos, queue_type).
        The .jsonl format makes it straightforward to tail-pipe into a log
        aggregator or parse with standard tools.
        """
        with self.lock:
            metric = {
                'timestamp': datetime.now().isoformat(),
                'metric': metric_name,
                'value': value,
                **tags
            }
            
            with open(self.metrics_file, 'a') as f:
                f.write(json.dumps(metric) + '\n')
    
    def log_event(self, event_type, **details):
        """
        Record a structured event to both the main log and the metrics file.

        Use this for significant state transitions: connections, disconnections,
        queue overflow, process startup, etc. Events are written as JSON lines
        alongside metrics so they appear in the same timeline when analysing logs.
        """
        event = {
            'timestamp': datetime.now().isoformat(),
            'event': event_type,
            **details
        }
        
        # Log to main log file
        self.info(f"EVENT: {event_type}", **details)
        
        # Also write to metrics file for analysis
        with self.lock:
            with open(self.metrics_file, 'a') as f:
                f.write(json.dumps(event) + '\n')


# Global logger instance (singleton pattern)
_global_logger = None
_logger_lock = threading.Lock()

def get_logger(name="mqtt_client", **kwargs):
    """
    Return the shared logger instance, creating it on first call.

    All modules should obtain their logger through this function rather
    than instantiating ProductionLogger directly. This ensures log output
    from every part of the application goes through the same handlers and
    into the same files.
    """
    global _global_logger
    
    with _logger_lock:
        if _global_logger is None:
            _global_logger = ProductionLogger(name, **kwargs)
        return _global_logger


# Example usage
if __name__ == "__main__":
    # Test the logger
    logger = get_logger("test_logger", log_level=logging.DEBUG)
    
    logger.debug("This is a debug message", component="logger_test")
    logger.info("Application started", version="1.0", pid=os.getpid())
    logger.warning("Queue is 80% full", queue_size=800, max_size=1000)
    logger.error("Connection failed", broker="mqtt.example.com", error="timeout")
    logger.critical("Database corrupted", db_path="/var/lib/mqtt/data.db")
    
    # Log metrics
    logger.log_metric("messages_published", 150, topic="sensors/temp", qos=1)
    logger.log_metric("queue_depth", 42, queue_type="offline")
    
    # Log events
    logger.log_event("connection_established", broker="localhost", port=1883)
    logger.log_event("message_queued", topic="sensors/temp", priority=5)
    
    print("\n✓ Logs written to ./logs/test_logger.log")
    print("✓ Metrics written to ./logs/test_logger_metrics.jsonl")
