import os
import sys
import pytest

# ✅ Add project root to Python path
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

from utils.config_loader import ConfigLoader, load_config


def test_load_db_config():
    """Test loading database configuration."""
    loader = ConfigLoader()
    config = loader.load_config("config/db_config.yaml")
    
    # Verify PostgreSQL config loaded
    assert "postgresql" in config
    assert config["postgresql"]["port"] == 5432  # Should be int, not string
    assert isinstance(config["postgresql"]["pool_size"], int)
    
    print("✅ DB config loaded successfully")
    print(f"PostgreSQL host: {config['postgresql']['host']}")
    print(f"Port type: {type(config['postgresql']['port'])}")


def test_load_minio_config():
    """Test loading MinIO configuration."""
    loader = ConfigLoader()
    config = loader.load_config("config/minio_config.yaml")
    
    assert "minio" in config
    assert config["minio"]["bucket"] == "syniq-lake"
    assert isinstance(config["minio"]["secure"], bool)
    
    print("✅ MinIO config loaded successfully")


def test_type_conversion():
    """Test automatic type conversion."""
    # Set test environment variables
    os.environ["TEST_INT"] = "42"
    os.environ["TEST_FLOAT"] = "3.14"
    os.environ["TEST_BOOL_TRUE"] = "true"
    os.environ["TEST_BOOL_FALSE"] = "false"
    os.environ["TEST_NONE"] = "none"
    
    loader = ConfigLoader()
    
    assert loader._auto_convert_type("42") == 42
    assert loader._auto_convert_type("3.14") == 3.14
    assert loader._auto_convert_type("true") is True
    assert loader._auto_convert_type("false") is False
    assert loader._auto_convert_type("none") is None
    
    print("✅ Type conversion works correctly")


def test_cache():
    """Test configuration caching."""
    loader = ConfigLoader()
    
    # First load
    config1 = loader.load_config("config/db_config.yaml")
    
    # Second load should use cache
    config2 = loader.load_config("config/db_config.yaml")
    
    assert config1 is config2  # Same object in memory
    
    print("✅ Config caching works")


def test_convenience_function():
    """Test convenience function."""
    config = load_config("config/db_config.yaml")
    
    assert "postgresql" in config
    
    print("✅ Convenience function works")


if __name__ == "__main__":
    test_load_db_config()
    test_load_minio_config()
    test_type_conversion()
    test_cache()
    test_convenience_function()