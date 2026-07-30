"""
Test suite for Syniq Lakehouse.

Test structure:
- test_config_loader.py: Configuration loading tests
- test_connectors.py: Database connector tests
- test_metadata.py: Metadata detection tests
- test_ingestion.py: Ingestion pipeline tests

Run tests:
    pytest tests/ -v
    pytest tests/test_config_loader.py -v
"""

import os
import sys

# Add project root to Python path for imports
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

# Shared test fixtures (optional)
import pytest


@pytest.fixture
def sample_config():
    """Sample configuration for testing."""
    return {
        "postgresql": {
            "host": "test-host",
            "port": 5432,
            "database": "test_db",
            "user": "test_user",
            "password": "test_pass",
        }
    }


@pytest.fixture
def temp_env_file(tmp_path):
    """Create temporary .env file for testing."""
    env_file = tmp_path / ".env"
    env_file.write_text(
        "TEST_VAR=test_value\n"
        "TEST_INT=42\n"
        "TEST_BOOL=true\n"
    )
    return str(env_file)


# Export fixtures
__all__ = ["sample_config", "temp_env_file"]