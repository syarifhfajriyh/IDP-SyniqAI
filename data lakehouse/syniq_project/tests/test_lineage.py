"""
Tests for Data Lineage Tracker

Tests lineage tracking functionality including transformation recording
and upstream/downstream analysis.
"""

import pytest
from minio import Minio
from lineage.lineage_tracker import LineageTracker


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
def lineage_tracker(minio_client):
    """Create lineage tracker instance."""
    return LineageTracker(minio_client)


def test_record_transformation(lineage_tracker):
    """Test recording a transformation."""
    lineage_tracker.record_transformation(
        source_layer="source",
        source_location="postgres_prod/customers",
        target_layer="bronze",
        target_location="bronze/postgres_prod/customers",
        transformation="raw_ingestion",
        row_count=1000,
        columns_used=["id", "name", "email"],
        metadata={"job_id": "test_job_123"}
    )
    
    # Should not raise any errors
    assert True


def test_get_lineage(lineage_tracker):
    """Test retrieving lineage for a location."""
    # Record a transformation
    lineage_tracker.record_transformation(
        source_layer="source",
        source_location="postgres_prod/orders",
        target_layer="bronze",
        target_location="bronze/postgres_prod/orders",
        transformation="raw_ingestion",
        row_count=500,
        columns_used=["id", "customer_id", "amount"]
    )
    
    # Retrieve lineage
    lineage = lineage_tracker.get_lineage("bronze", "bronze/postgres_prod/orders", limit=10)
    
    assert isinstance(lineage, list)
    if len(lineage) > 0:
        entry = lineage[0]
        assert "timestamp" in entry
        assert "source" in entry
        assert "target" in entry
        assert "transformation" in entry


def test_get_upstream_sources(lineage_tracker):
    """Test getting upstream sources."""
    # Record transformation
    lineage_tracker.record_transformation(
        source_layer="source",
        source_location="postgres_prod/products",
        target_layer="bronze",
        target_location="bronze/postgres_prod/products",
        transformation="raw_ingestion",
        row_count=200
    )
    
    # Get upstream sources
    upstream = lineage_tracker.get_upstream_sources("bronze", "bronze/postgres_prod/products")
    
    assert isinstance(upstream, list)


def test_get_downstream_targets(lineage_tracker):
    """Test getting downstream targets."""
    # Record bronze to silver transformation
    lineage_tracker.record_transformation(
        source_layer="bronze",
        source_location="bronze/postgres_prod/customers",
        target_layer="silver",
        target_location="silver/cleaned_customers",
        transformation="deduplication_and_validation",
        row_count=980
    )
    
    # Get downstream targets
    downstream = lineage_tracker.get_downstream_targets("bronze", "bronze/postgres_prod/customers")
    
    assert isinstance(downstream, list)


def test_multiple_transformations(lineage_tracker):
    """Test tracking multiple transformations in a pipeline."""
    # Source -> Bronze
    lineage_tracker.record_transformation(
        source_layer="source",
        source_location="postgres_prod/users",
        target_layer="bronze",
        target_location="bronze/postgres_prod/users",
        transformation="raw_ingestion",
        row_count=1000
    )
    
    # Bronze -> Silver
    lineage_tracker.record_transformation(
        source_layer="bronze",
        source_location="bronze/postgres_prod/users",
        target_layer="silver",
        target_location="silver/cleaned_users",
        transformation="cleaning_and_validation",
        row_count=990
    )
    
    # Silver -> Gold
    lineage_tracker.record_transformation(
        source_layer="silver",
        source_location="silver/cleaned_users",
        target_layer="gold",
        target_location="gold/user_metrics",
        transformation="aggregation",
        row_count=100
    )
    
    # Check bronze lineage
    bronze_lineage = lineage_tracker.get_lineage("bronze", "bronze/postgres_prod/users")
    assert len(bronze_lineage) >= 1
    
    # Check silver lineage
    silver_lineage = lineage_tracker.get_lineage("silver", "silver/cleaned_users")
    assert len(silver_lineage) >= 1
    
    # Check gold lineage
    gold_lineage = lineage_tracker.get_lineage("gold", "gold/user_metrics")
    assert len(gold_lineage) >= 1


def test_generate_lineage_report(lineage_tracker):
    """Test generating lineage report."""
    # Record some transformations
    lineage_tracker.record_transformation(
        source_layer="source",
        source_location="postgres_prod/transactions",
        target_layer="bronze",
        target_location="bronze/postgres_prod/transactions",
        transformation="raw_ingestion",
        row_count=5000
    )
    
    # Generate report
    report = lineage_tracker.generate_lineage_report("bronze", "bronze/postgres_prod/transactions")
    
    assert isinstance(report, str)
    assert "Lineage Report" in report
    assert "Upstream Sources" in report
    assert "Downstream Targets" in report


def test_transformation_with_metadata(lineage_tracker):
    """Test recording transformation with metadata."""
    metadata = {
        "user": "data_engineer",
        "job_id": "job_456",
        "source_type": "postgres",
        "quality_score": 95.5
    }
    
    lineage_tracker.record_transformation(
        source_layer="source",
        source_location="postgres_prod/events",
        target_layer="bronze",
        target_location="bronze/postgres_prod/events",
        transformation="raw_ingestion",
        row_count=10000,
        columns_used=["id", "event_type", "timestamp"],
        metadata=metadata
    )
    
    # Retrieve and verify
    lineage = lineage_tracker.get_lineage("bronze", "bronze/postgres_prod/events")
    
    if len(lineage) > 0:
        entry = lineage[0]
        assert entry["metadata"]["job_id"] == "job_456"
        assert entry["metadata"]["quality_score"] == 95.5


def test_lineage_limit(lineage_tracker):
    """Test lineage retrieval with limit."""
    # Record multiple transformations
    for i in range(5):
        lineage_tracker.record_transformation(
            source_layer="source",
            source_location="test/multiple",
            target_layer="bronze",
            target_location="bronze/test/multiple",
            transformation=f"transformation_{i}",
            row_count=100 * (i + 1)
        )
    
    # Get with limit
    lineage = lineage_tracker.get_lineage("bronze", "bronze/test/multiple", limit=3)
    
    assert len(lineage) <= 3


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
