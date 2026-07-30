"""
Configuration files for Syniq Lakehouse.

Files:
- .env: Environment variables (DO NOT COMMIT)
- .env.example: Template for environment variables
- db_config.yaml: Database connection settings
- minio_config.yaml: Storage configuration
- ingestion_config.yaml: Table-level ingestion control

Usage:
    from utils.config_loader import load_config
    config = load_config("config/db_config.yaml")
"""

import os

# Get config directory path
CONFIG_DIR = os.path.dirname(os.path.abspath(__file__))

# Helper to get config file paths
def get_config_path(filename: str) -> str:
    """Get full path to config file."""
    return os.path.join(CONFIG_DIR, filename)


__all__ = ["CONFIG_DIR", "get_config_path"]