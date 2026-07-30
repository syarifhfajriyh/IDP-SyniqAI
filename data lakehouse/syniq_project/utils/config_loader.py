import os
import yaml
import logging
from dotenv import load_dotenv
from typing import Any, Dict, List, Optional


class ConfigLoader:
    """
    Loads YAML configuration files with environment variable substitution.
    Implements caching, type conversion, and validation.
    
    Usage:
        loader = ConfigLoader()
        db_config = loader.load_config("config/db_config.yaml")
        minio_config = loader.load_config("config/minio_config.yaml")
    """

    def __init__(self, env_path: str = "config/.env"):
        """
        Initialize config loader and load environment variables.
        
        Args:
            env_path: Path to .env file (optional)
            
        Note:
            .env file is optional - will use system environment variables if not found
        """
        self.logger = logging.getLogger(__name__)
        
        if os.path.exists(env_path):
            load_dotenv(env_path)
            self.logger.info(f"Loaded environment variables from {env_path}")
        else:
            self.logger.warning(f".env file not found at {env_path}, using system environment variables")
        
        self._config_cache: Dict[str, Dict[str, Any]] = {}

    def load_config(self, config_path: str, use_cache: bool = True) -> Dict[str, Any]:
        """
        Load YAML config file and substitute environment variables.
        
        Args:
            config_path: Path to YAML configuration file
            use_cache: If True, return cached config if available
            
        Returns:
            Dictionary containing configuration
            
        Raises:
            FileNotFoundError: If config file doesn't exist
            ValueError: If config is invalid or missing required fields
        """
        # Check cache first
        if use_cache and config_path in self._config_cache:
            self.logger.debug(f"Using cached config for {config_path}")
            return self._config_cache[config_path]
        
        self.logger.info(f"Loading configuration from {config_path}")
        
        if not os.path.exists(config_path):
            raise FileNotFoundError(f"Config file not found: {config_path}")

        # Load YAML
        with open(config_path, "r") as file:
            config = yaml.safe_load(file)

        # Substitute environment variables
        config = self._substitute_env_vars(config)
        
        # Validate configuration
        self._validate_config(config, config_path)

        # Cache configuration
        self._config_cache[config_path] = config
        
        self.logger.info(f"Successfully loaded config from {config_path}")

        return config
    
    def clear_cache(self) -> None:
        """Clear configuration cache. Useful for testing or reloading configs."""
        self._config_cache.clear()
        self.logger.debug("Configuration cache cleared")

    def _substitute_env_vars(self, value: Any) -> Any:
        """
        Recursively substitute ${VAR} or ${VAR:default} with environment variables.
        Automatically converts types (int, float, bool, None).
        
        Args:
            value: Configuration value (can be dict, list, str, or primitive)
            
        Returns:
            Configuration value with environment variables substituted
        """
        if isinstance(value, dict):
            return {k: self._substitute_env_vars(v) for k, v in value.items()}

        elif isinstance(value, list):
            return [self._substitute_env_vars(v) for v in value]

        elif isinstance(value, str):
            if value.startswith("${") and value.endswith("}"):
                inner = value[2:-1]

                # Parse variable name and optional default
                if ":" in inner:
                    var_name, default = inner.split(":", 1)
                else:
                    var_name, default = inner, None

                env_value = os.getenv(var_name, default)

                if env_value is None:
                    raise ValueError(
                        f"Environment variable '{var_name}' not set and no default provided."
                    )

                # Auto-convert type
                return self._auto_convert_type(env_value)

            return value

        else:
            return value

    def _auto_convert_type(self, value: str) -> Any:
        """
        Automatically convert string values to appropriate types.
        
        Conversion order:
        1. None/null
        2. Boolean (true/false)
        3. Integer
        4. Float
        5. String (fallback)
        
        Args:
            value: String value to convert
            
        Returns:
            Value converted to appropriate type
        """
        # Handle None/null
        if value.lower() in ('none', 'null', ''):
            return None
        
        # Handle booleans
        if value.lower() in ('true', 'yes', '1', 'on'):
            return True
        if value.lower() in ('false', 'no', '0', 'off'):
            return False
        
        # Try integer
        try:
            return int(value)
        except ValueError:
            pass
        
        # Try float
        try:
            return float(value)
        except ValueError:
            pass
        
        # Return as string
        return value

    def _validate_config(self, config: Dict[str, Any], config_path: Optional[str] = None) -> None:
        """
        Validate configuration structure and required fields.
        
        Args:
            config: Configuration dictionary
            config_path: Path to config file (used to determine validation rules)
            
        Raises:
            ValueError: If configuration is invalid
        """
        if not config:
            raise ValueError("Loaded configuration is empty.")

        if not isinstance(config, dict):
            raise ValueError("Configuration file must return a dictionary.")
        
        # Apply specific validation based on config type
        if config_path:
            config_path_str = str(config_path)
            if "db_config" in config_path_str:
                self._validate_db_config(config)
            elif "minio_config" in config_path_str:
                self._validate_minio_config(config)
            elif "ingestion_config" in config_path_str:
                self._validate_ingestion_config(config)

    def _validate_db_config(self, config: Dict[str, Any]) -> None:
        """
        Validate database configuration.
        
        Args:
            config: Database configuration dictionary
            
        Raises:
            ValueError: If required fields are missing
        """
        required_fields = {
            "postgresql": ["host", "port", "database", "user", "password"],
            "mariadb": ["host", "port", "database", "user", "password"],
            "mariadb_cloud": ["host", "port", "database", "user", "password"],
            "mongodb": ["uri", "database"],
            "aws_s3": ["access_key_id", "secret_access_key", "region", "bucket"]
        }
        
        for db_type, fields in required_fields.items():
            if db_type in config:
                for field in fields:
                    if field not in config[db_type]:
                        raise ValueError(
                            f"Missing required field '{field}' in {db_type} configuration"
                        )
                self.logger.debug(f"Validated {db_type} configuration")

    def _validate_minio_config(self, config: Dict[str, Any]) -> None:
        """
        Validate MinIO configuration.
        
        Args:
            config: MinIO configuration dictionary
            
        Raises:
            ValueError: If required fields are missing
        """
        if "minio" not in config:
            raise ValueError("MinIO configuration missing 'minio' section")
        
        required_fields = ["endpoint", "access_key", "secret_key"]
        for field in required_fields:
            if field not in config["minio"]:
                raise ValueError(f"Missing required MinIO field: {field}")
        
        # Check for either bucket or buckets
        if "bucket" not in config["minio"] and "buckets" not in config["minio"]:
            raise ValueError("MinIO configuration must have either 'bucket' or 'buckets' field")
        
        self.logger.debug("Validated MinIO configuration")

    def _validate_ingestion_config(self, config: Dict[str, Any]) -> None:
        """
        Validate ingestion configuration.
        
        Args:
            config: Ingestion configuration dictionary
            
        Raises:
            ValueError: If configuration structure is invalid
        """
        # Check that at least one source is configured
        source_types = ["postgresql", "mariadb", "mariadb_cloud", "mongodb", "aws_s3"]
        has_source = any(source in config for source in source_types)
        
        if not has_source:
            raise ValueError(
                f"Ingestion config must contain at least one source: {', '.join(source_types)}"
            )
        
        self.logger.debug("Validated ingestion configuration")

    def get_db_connection_string(self, db_type: str, config: Dict[str, Any]) -> str:
        """
        Generate database connection string from configuration.
        
        Args:
            db_type: Type of database (postgresql, mariadb, mongodb)
            config: Database configuration dictionary
            
        Returns:
            Connection string
            
        Raises:
            ValueError: If db_type is not supported
        """
        if db_type == "postgresql":
            return (
                f"postgresql://{config['user']}:{config['password']}"
                f"@{config['host']}:{config['port']}/{config['database']}"
            )
        elif db_type in ["mariadb", "mariadb_cloud"]:
            return (
                f"mysql+pymysql://{config['user']}:{config['password']}"
                f"@{config['host']}:{config['port']}/{config['database']}"
            )
        elif db_type == "mongodb":
            return config["uri"]
        else:
            raise ValueError(f"Unsupported database type: {db_type}")


# ============================================================
# Convenience function for single config loading
# ============================================================

def load_config(config_path: str, env_path: str = "config/.env") -> Dict[str, Any]:
    """
    Convenience function to load a config file.
    
    Usage:
        config = load_config("config/db_config.yaml")
    
    Args:
        config_path: Path to YAML configuration file
        env_path: Path to .env file (default: config/.env)
        
    Returns:
        Configuration dictionary
    """
    loader = ConfigLoader(env_path=env_path)
    return loader.load_config(config_path)