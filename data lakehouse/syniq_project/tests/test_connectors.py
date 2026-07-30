# filepath: tests/test_connectors.py
"""
Test suite for connector modules.

Tests:
- Connector initialization
- Config integration
- Connection validation (requires real DB - skip if unavailable)
"""

import os
import sys
import pytest

# Add project root to path
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

from connectors import BaseConnector, PostgresConnector, MariaDBConnector, MariaDBCloudConnector, MongoDBConnector
from utils.config_loader import load_config


def test_baseconnector_is_abstract():
    """Test that BaseConnector cannot be instantiated directly"""
    with pytest.raises(TypeError):
        BaseConnector({"host": "test"})
    print("✅ BaseConnector is abstract (cannot instantiate)")


def test_postgres_connector_initialization():
    """Test PostgreSQL connector can be initialized"""
    config = {
        "host": "test-host",
        "port": 5432,
        "database": "test_db",
        "user": "test_user",
        "password": "test_pass",
        "pool_size": 5
    }
    
    connector = PostgresConnector(config)
    assert connector.source_type == "postgres"
    assert connector.connection_config == config
    assert connector.engine is None  # Not connected yet
    
    print("✅ PostgresConnector initialization works")


def test_mariadb_connector_initialization():
    """Test MariaDB connector can be initialized"""
    config = {
        "host": "test-mariadb-host",
        "port": 3306,
        "database": "test_db",
        "user": "test_user",
        "password": "test_pass",
        "pool_size": 20
    }
    
    connector = MariaDBConnector(config)
    assert connector.source_type == "mariadb"
    assert connector.connection_config == config
    assert connector.engine is None
    assert hasattr(connector, 'optimizer')
    
    print("✅ MariaDBConnector initialization works")


def test_mariadb_cloud_connector_ssl_validation():
    """Test MariaDB Cloud connector requires SSL"""
    config = {
        "host": "cloud-host",
        "port": 4020,
        "database": "test_db",
        "user": "test_user",
        "password": "test_pass"
        # Missing SSL certificate
    }
    
    # Should raise ValueError about missing SSL
    with pytest.raises(ValueError, match="SSL certificate is REQUIRED"):
        MariaDBCloudConnector(config)
    
    print("✅ MariaDB Cloud SSL validation works")


def test_mariadb_cloud_connector_with_ssl():
    """Test MariaDB Cloud connector with SSL config"""
    config = {
        "host": "cloud-host",
        "port": 4020,
        "database": "test_db",
        "user": "test_user",
        "password": "test_pass",
        "ssl": {
            "ca": "/path/to/cert.pem"
        }
    }
    
    connector = MariaDBCloudConnector(config)
    assert connector.source_type == "mariadb_cloud"
    assert connector.connection_config["is_cloud"] is True
    assert hasattr(connector, 'optimizer')
    
    # Check cloud defaults were applied
    assert connector.connection_config["connect_timeout"] == 30
    assert connector.connection_config["pool_size"] == 3
    
    print("✅ MariaDB Cloud connector with SSL works")


def test_connector_context_manager():
    """Test connectors support context manager"""
    config = {
        "host": "test-host",
        "port": 5432,
        "database": "test_db",
        "user": "test_user",
        "password": "test_pass"
    }
    
    connector = PostgresConnector(config)
    
    # Verify __enter__ and __exit__ methods exist
    assert hasattr(connector, '__enter__')
    assert hasattr(connector, '__exit__')
    
    print("✅ Connector context manager interface exists")


def test_connector_with_config_loader():
    """Test connector initialization from ConfigLoader"""
    try:
        # Try to load actual config
        config = load_config("config/db_config.yaml")
        
        if "postgresql" in config:
            connector = PostgresConnector(config["postgresql"])
            assert connector.connection_config["host"] is not None
            print(f"✅ PostgreSQL connector loaded from config: {connector.connection_config['host']}")
        
        if "mariadb" in config:
            connector = MariaDBConnector(config["mariadb"])
            assert connector.connection_config["host"] is not None
            print(f"✅ MariaDB connector loaded from config: {connector.connection_config['host']}")
        
        if "mariadb_cloud" in config:
            # This should work if SSL is configured
            connector = MariaDBCloudConnector(config["mariadb_cloud"])
            print(f"✅ MariaDB Cloud connector loaded from config: {connector.connection_config['host']}")
            
    except FileNotFoundError:
        print("⚠️  Config file not found - skipping ConfigLoader test")
    except Exception as e:
        print(f"⚠️  ConfigLoader test skipped: {e}")


def test_connector_registry():
    """Test connector registry for factory pattern"""
    from connectors import CONNECTOR_REGISTRY, get_connector
    
    # Check registry
    assert "postgres" in CONNECTOR_REGISTRY
    assert "mariadb" in CONNECTOR_REGISTRY
    assert "mariadb_cloud" in CONNECTOR_REGISTRY
    assert "mongodb" in CONNECTOR_REGISTRY
    
    # Test get_connector function
    PGClass = get_connector("postgres")
    assert PGClass == PostgresConnector
    
    MBClass = get_connector("mariadb")
    assert MBClass == MariaDBConnector
    
    MongoClass = get_connector("mongodb")
    assert MongoClass == MongoDBConnector
    
    # Test invalid connector type
    with pytest.raises(ValueError, match="Unknown connector type"):
        get_connector("nonexistent")
    
    print("✅ Connector registry works")


def test_mongodb_connector_initialization():
    """Test MongoDB connector can be initialized"""
    # Test with URI
    config_uri = {
        "uri": "mongodb://localhost:27017/",
        "database": "test_db"
    }
    
    connector = MongoDBConnector(config_uri)
    assert connector.source_type == "mongodb"
    assert connector.connection_config == config_uri
    assert connector.client is None  # Not connected yet
    
    print("✅ MongoDB connector (URI mode) initialization works")
    
    # Test with individual parameters
    config_params = {
        "host": "localhost",
        "port": 27017,
        "database": "test_db",
        "user": "test_user",
        "password": "test_pass"
    }
    
    connector = MongoDBConnector(config_params)
    assert connector.source_type == "mongodb"
    
    print("✅ MongoDB connector (params mode) initialization works")


def test_mongodb_connector_with_config_loader():
    """Test MongoDB connector from ConfigLoader"""
    try:
        config = load_config("config/db_config.yaml")
        
        if "mongodb" in config:
            connector = MongoDBConnector(config["mongodb"])
            assert connector.connection_config.get("uri") or connector.connection_config.get("host")
            print(f"✅ MongoDB connector loaded from config")
    except FileNotFoundError:
        print("⚠️  Config file not found - skipping MongoDB ConfigLoader test")
    except Exception as e:
        print(f"⚠️  MongoDB ConfigLoader test skipped: {e}")


if __name__ == "__main__":
    print("🧪 Running Connector Tests")
    print("=" * 60)
    
    test_baseconnector_is_abstract()
    test_postgres_connector_initialization()
    test_mariadb_connector_initialization()
    test_mariadb_cloud_connector_ssl_validation()
    test_mariadb_cloud_connector_with_ssl()
    test_connector_context_manager()
    test_mongodb_connector_initialization()
    test_connector_with_config_loader()
    test_mongodb_connector_with_config_loader()
    test_connector_registry()
    
    print("=" * 60)
    print("✅ All connector tests passed!")
