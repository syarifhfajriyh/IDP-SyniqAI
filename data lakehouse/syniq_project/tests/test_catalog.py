"""
Tests for Catalog Manager

Tests metadata catalog functionality including table registration,
schema versioning, and discovery.
"""

import pytest
import pandas as pd
from datetime import datetime
from minio import Minio
from catalog.catalog_manager import CatalogManager


@pytest.fixture
def minio_client():
    """Create test MinIO client."""
    return Minio(
        "localhost:9000",
        access_key="admin",
        secret_key="password123",
        secure=False
    )


@pytest.fixture
def catalog_manager(minio_client):
    """Create catalog manager instance."""
    return CatalogManager(minio_client, bucket="test-catalog")


def test_register_table_first_version(catalog_manager):
    """Test registering a new table."""
    schema = {
        "id": "int64",
        "name": "object",
        "email": "object",
        "created_at": "datetime64[ns]"
    }
    
    metadata = catalog_manager.register_table(
        source="test_source",
        entity="test_table",
        schema=schema,
        location="bronze/test_source/test_table",
        row_count=100,
        partition_keys=["year", "month", "day"],
        tags={"layer": "bronze", "test": "true"}
    )
    
    assert metadata["version"] == 1
    assert metadata["source"] == "test_source"
    assert metadata["entity"] == "test_table"
    assert metadata["row_count"] == 100
    assert len(metadata["schema"]) == 4
    assert metadata["partition_keys"] == ["year", "month", "day"]
    assert metadata["tags"]["layer"] == "bronze"


def test_register_table_schema_change(catalog_manager):
    """Test schema versioning on schema change."""
    # First version
    schema_v1 = {
        "id": "int64",
        "name": "object"
    }
    
    catalog_manager.register_table(
        "test_source",
        "versioned_table",
        schema_v1,
        "bronze/test_source/versioned_table",
        100
    )
    
    # Second version with schema change
    schema_v2 = {
        "id": "int64",
        "name": "object",
        "email": "object",  # New column
        "phone": "object"   # New column
    }
    
    metadata = catalog_manager.register_table(
        "test_source",
        "versioned_table",
        schema_v2,
        "bronze/test_source/versioned_table",
        150
    )
    
    assert metadata["version"] == 2
    assert len(metadata["schema"]) == 4


def test_register_table_no_schema_change(catalog_manager):
    """Test that version doesn't increment without schema change."""
    schema = {"id": "int64", "name": "object"}
    
    # First registration
    metadata1 = catalog_manager.register_table(
        "test_source",
        "stable_table",
        schema,
        "bronze/test_source/stable_table",
        100
    )
    
    # Second registration with same schema
    metadata2 = catalog_manager.register_table(
        "test_source",
        "stable_table",
        schema,
        "bronze/test_source/stable_table",
        200
    )
    
    assert metadata1["version"] == metadata2["version"]
    assert metadata2["row_count"] == 200  # Updated row count


def test_get_table(catalog_manager):
    """Test retrieving table metadata."""
    schema = {"id": "int64", "name": "object"}
    
    catalog_manager.register_table(
        "test_source",
        "retrieve_test",
        schema,
        "bronze/test_source/retrieve_test",
        100
    )
    
    # Retrieve current version
    metadata = catalog_manager.get_table("test_source", "retrieve_test")
    
    assert metadata is not None
    assert metadata["source"] == "test_source"
    assert metadata["entity"] == "retrieve_test"


def test_list_tables(catalog_manager):
    """Test listing all tables."""
    # Register multiple tables
    for i in range(3):
        catalog_manager.register_table(
            "test_source",
            f"table_{i}",
            {"id": "int64"},
            f"bronze/test_source/table_{i}",
            100 * (i + 1)
        )
    
    tables = catalog_manager.list_tables()
    
    assert len(tables) >= 3
    
    # Test filtering by source
    filtered = catalog_manager.list_tables(source="test_source")
    assert all(t["source"] == "test_source" for t in filtered)


def test_list_tables_with_tags(catalog_manager):
    """Test listing tables filtered by tags."""
    catalog_manager.register_table(
        "test_source",
        "tagged_table",
        {"id": "int64"},
        "bronze/test_source/tagged_table",
        100,
        tags={"environment": "test", "priority": "high"}
    )
    
    tables = catalog_manager.list_tables(tags={"environment": "test"})
    
    assert len(tables) >= 1
    assert all(t["tags"].get("environment") == "test" for t in tables)


def test_get_schema_history(catalog_manager):
    """Test retrieving schema change history."""
    # V1
    schema_v1 = {"id": "int64", "name": "object"}
    catalog_manager.register_table(
        "test_source",
        "history_test",
        schema_v1,
        "bronze/test_source/history_test",
        100
    )
    
    # V2
    schema_v2 = {"id": "int64", "name": "object", "email": "object"}
    catalog_manager.register_table(
        "test_source",
        "history_test",
        schema_v2,
        "bronze/test_source/history_test",
        150
    )
    
    # V3
    schema_v3 = {"id": "int64", "name": "object", "email": "object", "phone": "object"}
    catalog_manager.register_table(
        "test_source",
        "history_test",
        schema_v3,
        "bronze/test_source/history_test",
        200
    )
    
    history = catalog_manager.get_schema_history("test_source", "history_test")
    
    assert len(history) == 3
    assert history[0]["version"] == 1
    assert history[1]["version"] == 2
    assert history[2]["version"] == 3
    assert history[2]["row_count"] == 200


def test_search_tables(catalog_manager):
    """Test searching tables by name."""
    catalog_manager.register_table(
        "postgres_prod",
        "customers",
        {"id": "int64"},
        "bronze/postgres_prod/customers",
        100
    )
    
    catalog_manager.register_table(
        "postgres_prod",
        "orders",
        {"id": "int64"},
        "bronze/postgres_prod/orders",
        200
    )
    
    # Search by entity name
    results = catalog_manager.search_tables("customer")
    assert len(results) >= 1
    assert any("customer" in t["entity"].lower() for t in results)
    
    # Search by source name
    results = catalog_manager.search_tables("postgres")
    assert len(results) >= 2


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
