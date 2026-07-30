"""
Configuration management for SyniqAI Lakehouse Platform
Handles database, storage, and service configurations
"""
import os
from dataclasses import dataclass
from typing import Optional
import json


@dataclass
class PostgreSQLConfig:
    """PostgreSQL metadata database configuration"""
    host: str = "localhost"
    port: int = 5432
    database: str = "syniqai_metadata"
    user: str = "syniqai_user"
    password: str = "syniqai_password"
    
    @property
    def connection_string(self) -> str:
        """Generate PostgreSQL connection string"""
        return f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"
    
    @classmethod
    def from_env(cls) -> 'PostgreSQLConfig':
        """Load configuration from environment variables"""
        return cls(
            host=os.getenv("POSTGRES_HOST", "localhost"),
            port=int(os.getenv("POSTGRES_PORT", "5432")),
            database=os.getenv("POSTGRES_DB", "syniqai_metadata"),
            user=os.getenv("POSTGRES_USER", "syniqai_user"),
            password=os.getenv("POSTGRES_PASSWORD", "syniqai_password")
        )


@dataclass
class MinIOConfig:
    """MinIO/S3 object storage configuration"""
    endpoint: str = "localhost:9000"
    access_key: str = "admin"
    secret_key: str = "password123"
    secure: bool = False
    
    # Bucket naming convention
    bronze_bucket: str = "syniqai-bronze"
    silver_bucket: str = "syniqai-silver"
    gold_bucket: str = "syniqai-gold"
    quarantine_bucket: str = "syniqai-quarantine"
    
    @classmethod
    def from_env(cls) -> 'MinIOConfig':
        """Load configuration from environment variables"""
        return cls(
            endpoint=os.getenv("MINIO_ENDPOINT", "localhost:9000"),
            access_key=os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
            secret_key=os.getenv("MINIO_SECRET_KEY", "minioadmin"),
            secure=os.getenv("MINIO_SECURE", "false").lower() == "true",
            bronze_bucket=os.getenv("MINIO_BRONZE_BUCKET", "syniqai-bronze"),
            silver_bucket=os.getenv("MINIO_SILVER_BUCKET", "syniqai-silver"),
            gold_bucket=os.getenv("MINIO_GOLD_BUCKET", "syniqai-gold"),
            quarantine_bucket=os.getenv("MINIO_QUARANTINE_BUCKET", "syniqai-quarantine")
        )
    
    def get_bucket_for_layer(self, layer: str) -> str:
        """Get bucket name for specific data layer"""
        layer_lower = layer.lower()
        if layer_lower == "bronze":
            return self.bronze_bucket
        elif layer_lower == "silver":
            return self.silver_bucket
        elif layer_lower == "gold":
            return self.gold_bucket
        elif layer_lower == "quarantine":
            return self.quarantine_bucket
        else:
            raise ValueError(f"Unknown layer: {layer}")


@dataclass
class AppConfig:
    """Global application configuration"""
    # Service settings
    api_host: str = "localhost"
    api_port: int = 8000
    debug: bool = True
    
    # Data processing
    max_workers: int = 4
    chunk_size: int = 10000
    
    # Rule engine
    rule_timeout_seconds: int = 300
    enable_rule_caching: bool = True
    
    # Domain settings
    available_domains: list = None
    
    def __post_init__(self):
        if self.available_domains is None:
            self.available_domains = ["finance", "healthcare", "general"]
    
    @classmethod
    def from_env(cls) -> 'AppConfig':
        """Load configuration from environment variables"""
        return cls(
            api_host=os.getenv("API_HOST", "localhost"),
            api_port=int(os.getenv("API_PORT", "8000")),
            debug=os.getenv("DEBUG", "true").lower() == "true",
            max_workers=int(os.getenv("MAX_WORKERS", "4")),
            chunk_size=int(os.getenv("CHUNK_SIZE", "10000")),
            rule_timeout_seconds=int(os.getenv("RULE_TIMEOUT", "300")),
            enable_rule_caching=os.getenv("RULE_CACHING", "true").lower() == "true"
        )


class ConfigManager:
    """Centralized configuration manager"""
    
    _instance = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._initialized = False
        return cls._instance
    
    def __init__(self):
        if self._initialized:
            return
        
        self.postgres = PostgreSQLConfig.from_env()
        self.minio = MinIOConfig.from_env()
        self.app = AppConfig.from_env()
        self._initialized = True
    
    def reload(self):
        """Reload configuration from environment"""
        self.postgres = PostgreSQLConfig.from_env()
        self.minio = MinIOConfig.from_env()
        self.app = AppConfig.from_env()
    
    def to_dict(self) -> dict:
        """Export configuration as dictionary"""
        return {
            "postgres": {
                "host": self.postgres.host,
                "port": self.postgres.port,
                "database": self.postgres.database,
                "user": self.postgres.user
                # Don't expose password
            },
            "minio": {
                "endpoint": self.minio.endpoint,
                "secure": self.minio.secure,
                "buckets": {
                    "bronze": self.minio.bronze_bucket,
                    "silver": self.minio.silver_bucket,
                    "gold": self.minio.gold_bucket,
                    "quarantine": self.minio.quarantine_bucket
                }
            },
            "app": {
                "api_host": self.app.api_host,
                "api_port": self.app.api_port,
                "debug": self.app.debug,
                "domains": self.app.available_domains
            }
        }


# Global config instance
config = ConfigManager()


if __name__ == "__main__":
    # Test configuration
    print("=== SyniqAI Configuration ===")
    print(json.dumps(config.to_dict(), indent=2))
