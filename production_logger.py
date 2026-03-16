# production_logger.py
# -*- coding: utf-8 -*-
"""
Production-grade logging for MQTT client.

Features:
- File-based logging with rotation
- Multiple log levels (DEBUG, INFO, WARNING, ERROR, CRITICAL)
- Structured logging with context
- Performance metrics logging
- Thread-safe
"""

import logging
import logging.handlers
import os
import json
from datetime import datetime
import threading

class ProductionLogger:
    """
    Production-ready logger with file rotation and structured logging.
    
    Usage:
        logger = ProductionLogger("mqtt_client", log_dir="/var/log/mqtt")
        logger.info("Connected to broker", broker="mqtt.example.com")
        logger.error("Publish failed", topic="sensors/temp", error=str(e))
    """
    
    def __init__(self, name, log_dir="./logs", log_level=logging.INFO, 
                 max_bytes=10*1024*1024, backup_count=5):
        """
        Initialize logger.
        
        Args:
            name: Logger name (appears in logs)
            log_dir: Directory for log files
            log_level: Minimum level to log (DEBUG, INFO, WARNING, ERROR, CRITICAL)
            max_bytes: Maximum log file size before rotation (default 10MB)
            backup_count: Number of backup files to keep (default 5)
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
        """Log message with additional context as key-value pairs."""
        # Build context string
        if context:
            context_str = " | ".join(f"{k}={v}" for k, v in context.items())
            full_message = f"{message} | {context_str}"
        else:
            full_message = message
        
        # Log at appropriate level
        self.logger.log(level, full_message)
    
    def debug(self, message, **context):
        """Log debug message."""
        self._log_with_context(logging.DEBUG, message, **context)
    
    def info(self, message, **context):
        """Log info message."""
        self._log_with_context(logging.INFO, message, **context)
    
    def warning(self, message, **context):
        """Log warning message."""
        self._log_with_context(logging.WARNING, message, **context)
    
    def error(self, message, **context):
        """Log error message."""
        self._log_with_context(logging.ERROR, message, **context)
    
    def critical(self, message, **context):
        """Log critical message."""
        self._log_with_context(logging.CRITICAL, message, **context)
    
    def log_metric(self, metric_name, value, **tags):
        """
        Log a metric to structured metrics file.
        
        Metrics are written as JSON lines for easy parsing by monitoring tools.
        
        Args:
            metric_name: Name of the metric (e.g., "message_published")
            value: Metric value (number)
            **tags: Additional tags (e.g., topic="sensors/temp", qos=1)
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
        Log a structured event.
        
        Events are significant occurrences like connections, disconnections, errors.
        
        Args:
            event_type: Type of event (e.g., "connection_established")
            **details: Event details
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
    Get or create global logger instance.
    
    This ensures all parts of the application use the same logger.
    
    Args:
        name: Logger name
        **kwargs: Passed to ProductionLogger constructor
    
    Returns:
        ProductionLogger instance
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