# config.py
# -*- coding: utf-8 -*-
"""
Configuration management for the production MQTT client.

Supports loading from a JSON file, a simple key=value file, environment
variables, or a plain dictionary. All sources are merged with the same
precedence order and validated on load.

v0.5.0: Added certfile and keyfile to DEFAULTS to support mutual TLS
        (client certificate authentication), used by brokers such as AWS IoT Core.
"""

import os
import json
import sys
from pathlib import Path

class Config:
    """
    Configuration loader and validator with support for multiple sources.

    Precedence, highest to lowest:
    1. Command-line arguments
    2. Environment variables
    3. Configuration file
    4. Defaults

    Usage:
        config = Config.from_file("config.json")
        broker_host = config.get("broker_host")

        # Or with environment variables:
        config = Config.from_env()

        # Or programmatically:
        config = Config({
            "broker_host": "mqtt.example.com",
            "client_id": "device_001"
        })
    """

    # Default configuration values
    DEFAULTS = {
        # MQTT connection
        "broker_host": "localhost",
        "broker_port": 1883,
        "client_id": None,          # Must be provided
        "keepalive": 60,
        "clean_session": False,

        # Authentication (optional)
        "username": None,
        "password": None,

        # TLS (optional)
        # Set use_tls=True and provide at minimum ca_certs to verify the broker.
        # certfile and keyfile are only needed for mutual TLS (mTLS), where the
        # broker also verifies the client's identity using a client certificate.
        # AWS IoT Core is the most common broker that requires mTLS.
        "use_tls": False,
        "ca_certs": None,           # Path to CA certificate file
        "certfile": None,           # Path to client certificate file (mTLS only)
        "keyfile": None,            # Path to client private key file (mTLS only)

        # Queue management
        "max_queue_size": 1000,
        "queue_batch_size": 10,
        "queue_drain_interval": 1.0,

        # Reconnection
        "min_backoff": 1,
        "max_backoff": 60,
        "reconnect_on_failure": True,

        # Logging
        "log_dir": "./logs",
        "log_level": "INFO",        # DEBUG, INFO, WARNING, ERROR, CRITICAL
        "log_max_bytes": 10485760,  # 10MB
        "log_backup_count": 5,

        # Database
        "db_path": "./mqtt_client.db",
        "db_timeout": 30.0,

        # Performance
        "message_rate_limit": 100,  # messages per second
        "max_inflight_messages": 20,

        # Health check
        "health_check_port": 8080,
        "enable_health_check": False,
    }

    # Required fields (must be provided by the caller)
    REQUIRED = [
        "client_id",
    ]

    def __init__(self, config_dict=None):
        """
        Initialise with an optional dictionary of overrides.

        Starts from DEFAULTS and applies config_dict on top. Call
        from_file() or from_env() instead if loading from an external source.
        """
        self.config = {}
        self.config.update(self.DEFAULTS)

        if config_dict:
            self.config.update(config_dict)

        self._validate()

    def _validate(self):
        """Validate configuration, raising ValueError on any constraint violation."""
        for field in self.REQUIRED:
            if not self.config.get(field):
                raise ValueError(f"Required configuration field missing: {field}")

        if not isinstance(self.config["broker_port"], int):
            raise ValueError("broker_port must be an integer")

        if self.config["broker_port"] < 1 or self.config["broker_port"] > 65535:
            raise ValueError("broker_port must be between 1 and 65535")

        if not isinstance(self.config["max_queue_size"], int) or self.config["max_queue_size"] < 1:
            raise ValueError("max_queue_size must be a positive integer")

        valid_log_levels = ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]
        if self.config["log_level"] not in valid_log_levels:
            raise ValueError(f"log_level must be one of {valid_log_levels}")

        # TLS validation: if use_tls is True, ca_certs should be provided.
        # We warn rather than raise here because some brokers allow TLS without
        # CA verification (though this is strongly discouraged in production).
        if self.config["use_tls"] and not self.config["ca_certs"]:
            import warnings
            warnings.warn(
                "use_tls is True but ca_certs is not set. "
                "The broker certificate will not be verified. "
                "This is insecure and should not be used in production.",
                UserWarning,
            )

        # Mutual TLS validation: certfile and keyfile must both be present or both absent.
        has_certfile = bool(self.config.get("certfile"))
        has_keyfile  = bool(self.config.get("keyfile"))
        if has_certfile != has_keyfile:
            raise ValueError(
                "certfile and keyfile must both be provided for mutual TLS, "
                "or both must be omitted."
            )

    def get(self, key, default=None):
        """Return the value for key, or default if not present."""
        return self.config.get(key, default)

    def set(self, key, value):
        """Set a configuration value and re-run validation."""
        self.config[key] = value
        self._validate()

    def to_dict(self):
        """Return a copy of the full configuration as a plain dictionary."""
        return self.config.copy()

    @classmethod
    def from_file(cls, file_path):
        """
        Load configuration from a file.

        Accepts either JSON (.json extension) or a simple key=value format
        for other extensions. Raises FileNotFoundError if the file doesn't exist.
        """
        file_path = Path(file_path)

        if not file_path.exists():
            raise FileNotFoundError(f"Configuration file not found: {file_path}")

        with open(file_path, 'r') as f:
            if file_path.suffix == '.json':
                config_dict = json.load(f)
            else:
                config_dict = {}
                for line in f:
                    line = line.strip()
                    if line and not line.startswith('#'):
                        if '=' in line:
                            key, value = line.split('=', 1)
                            value = value.strip()
                            if value.lower() == 'true':
                                value = True
                            elif value.lower() == 'false':
                                value = False
                            elif value.isdigit():
                                value = int(value)
                            elif value.replace('.', '').isdigit():
                                value = float(value)
                            config_dict[key.strip()] = value

        return cls(config_dict)

    @classmethod
    def from_env(cls, prefix="MQTT_"):
        """
        Load configuration from environment variables.

        Variables should be prefixed (default: MQTT_) and named after the
        corresponding config key in uppercase, e.g. MQTT_BROKER_HOST maps
        to broker_host. Type conversion follows the type of the default value.
        """
        config_dict = {}

        for key in cls.DEFAULTS.keys():
            env_var = prefix + key.upper()
            value = os.getenv(env_var)

            if value is not None:
                default_value = cls.DEFAULTS[key]
                if isinstance(default_value, bool):
                    value = value.lower() in ('true', '1', 'yes')
                elif isinstance(default_value, int):
                    value = int(value)
                elif isinstance(default_value, float):
                    value = float(value)

                config_dict[key] = value

        return cls(config_dict)

    def save_to_file(self, file_path):
        """
        Write the current configuration to a file.

        Uses JSON format for .json extensions, key=value for everything else.
        """
        file_path = Path(file_path)

        with open(file_path, 'w') as f:
            if file_path.suffix == '.json':
                json.dump(self.config, f, indent=2)
            else:
                for key, value in sorted(self.config.items()):
                    f.write(f"{key}={value}\n")

    def __str__(self):
        """String representation with sensitive fields redacted."""
        safe_config = self.config.copy()
        if safe_config.get('password'):
            safe_config['password'] = '***REDACTED***'
        if safe_config.get('keyfile'):
            safe_config['keyfile'] = '***REDACTED***'
        return json.dumps(safe_config, indent=2)
