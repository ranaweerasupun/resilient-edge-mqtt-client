# config.py
# -*- coding: utf-8 -*-
"""
Configuration management for the production MQTT client.

Supports loading from a JSON file, a simple key=value file, environment
variables, or a plain dictionary. All sources are merged with the same
precedence order and validated on load.

v0.5.0: Added certfile and keyfile to DEFAULTS to support mutual TLS.
v1.0.0: Type hints added throughout.
"""


import os
import json
import warnings
from pathlib import Path
from typing import Any, Dict, Optional, Union


class Config:
    """
    Configuration loader and validator with support for multiple sources.

    Precedence, highest to lowest:
      1. Command-line arguments (applied via Config({...}))
      2. Environment variables (Config.from_env())
      3. Configuration file (Config.from_file())
      4. Defaults (Config.DEFAULTS)
    """

    DEFAULTS: Dict[str, Any] = {
        # MQTT connection
        "broker_host":   "localhost",
        "broker_port":   1883,
        "client_id":     None,
        "keepalive":     60,
        "clean_session": False,

        # Authentication
        "username": None,
        "password": None,

        # TLS
        "use_tls":  False,
        "ca_certs": None,
        "certfile": None,
        "keyfile":  None,

        # Queue management
        "max_queue_size":      1000,
        "queue_batch_size":    10,
        "queue_drain_interval": 1.0,

        # Reconnection
        "min_backoff":          1,
        "max_backoff":          60,
        "reconnect_on_failure": True,

        # Logging
        "log_dir":          "./logs",
        "log_level":        "INFO",
        "log_max_bytes":    10485760,
        "log_backup_count": 5,

        # Database
        "db_path":    "./mqtt_client.db",
        "db_timeout": 30.0,

        # Performance
        "message_rate_limit":    100,
        "max_inflight_messages": 20,

        # Health check
        "health_check_port":  8080,
        "enable_health_check": False,
    }

    REQUIRED = ["client_id"]

    def __init__(self, config_dict: Optional[Dict[str, Any]] = None) -> None:
        """
        Initialise with an optional dictionary of overrides.

        Starts from DEFAULTS and applies config_dict on top. Call
        from_file() or from_env() instead if loading from an external source.
        """
        self.config: Dict[str, Any] = {}
        self.config.update(self.DEFAULTS)
        if config_dict:
            self.config.update(config_dict)
        self._validate()

    def _validate(self) -> None:
        """Validate configuration, raising ValueError on any constraint violation."""
        for field in self.REQUIRED:
            if not self.config.get(field):
                raise ValueError(f"Required configuration field missing: {field}")

        if not isinstance(self.config["broker_port"], int):
            raise ValueError("broker_port must be an integer")

        if not (1 <= self.config["broker_port"] <= 65535):
            raise ValueError("broker_port must be between 1 and 65535")

        if not isinstance(self.config["max_queue_size"], int) or self.config["max_queue_size"] < 1:
            raise ValueError("max_queue_size must be a positive integer")

        valid_log_levels = ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]
        if self.config["log_level"] not in valid_log_levels:
            raise ValueError(f"log_level must be one of {valid_log_levels}")

        if self.config["use_tls"] and not self.config["ca_certs"]:
            warnings.warn(
                "use_tls is True but ca_certs is not set. "
                "The broker certificate will not be verified. "
                "This is insecure and should not be used in production.",
                UserWarning,
            )

        has_certfile = bool(self.config.get("certfile"))
        has_keyfile  = bool(self.config.get("keyfile"))
        if has_certfile != has_keyfile:
            raise ValueError(
                "certfile and keyfile must both be provided for mutual TLS, "
                "or both must be omitted."
            )

    def get(self, key: str, default: Any = None) -> Any:
        """Return the value for key, or default if not present."""
        return self.config.get(key, default)

    def set(self, key: str, value: Any) -> None:
        """Set a configuration value and re-run validation."""
        self.config[key] = value
        self._validate()

    def to_dict(self) -> Dict[str, Any]:
        """Return a copy of the full configuration as a plain dictionary."""
        return self.config.copy()

    @classmethod
    def from_file(cls, file_path: Union[str, Path]) -> "Config":
        """
        Load configuration from a file.

        Accepts either JSON (.json extension) or a simple key=value format
        for other extensions. Raises FileNotFoundError if the file doesn't exist.
        """
        path = Path(file_path)

        if not path.exists():
            raise FileNotFoundError(f"Configuration file not found: {path}")

        with open(path, "r") as f:
            if path.suffix == ".json":
                config_dict: Dict[str, Any] = json.load(f)
            else:
                config_dict = {}
                for line in f:
                    line = line.strip()
                    if line and not line.startswith("#") and "=" in line:
                        key, value_str = line.split("=", 1)
                        v = value_str.strip()
                        value: Any
                        if v.lower() == "true":
                            value = True
                        elif v.lower() == "false":
                            value = False
                        elif v.isdigit():
                            value = int(v)
                        elif v.replace(".", "").isdigit():
                            value = float(v)
                        else:
                            value = v
                        config_dict[key.strip()] = value

        return cls(config_dict)

    @classmethod
    def from_env(cls, prefix: str = "MQTT_") -> "Config":
        """
        Load configuration from environment variables.

        Variables should be prefixed (default: MQTT_) and named after the
        corresponding config key in uppercase, e.g. MQTT_BROKER_HOST maps
        to broker_host. Type conversion follows the type of the default value.
        """
        config_dict: Dict[str, Any] = {}

        for key in cls.DEFAULTS.keys():
            env_var = prefix + key.upper()
            raw = os.getenv(env_var)

            if raw is not None:
                default_value = cls.DEFAULTS[key]
                value: Any
                if isinstance(default_value, bool):
                    value = raw.lower() in ("true", "1", "yes")
                elif isinstance(default_value, int):
                    value = int(raw)
                elif isinstance(default_value, float):
                    value = float(raw)
                else:
                    value = raw
                config_dict[key] = value

        return cls(config_dict)

    def save_to_file(self, file_path: Union[str, Path]) -> None:
        """Write the current configuration to a file."""
        path = Path(file_path)
        with open(path, "w") as f:
            if path.suffix == ".json":
                json.dump(self.config, f, indent=2)
            else:
                for key, value in sorted(self.config.items()):
                    f.write(f"{key}={value}\n")

    def __str__(self) -> str:
        """String representation with sensitive fields redacted."""
        safe_config = self.config.copy()
        if safe_config.get("password"):
            safe_config["password"] = "***REDACTED***"
        if safe_config.get("keyfile"):
            safe_config["keyfile"] = "***REDACTED***"
        return json.dumps(safe_config, indent=2)
