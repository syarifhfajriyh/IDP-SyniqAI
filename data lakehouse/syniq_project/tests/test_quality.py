"""
Tests for Data Quality Profiler

Tests data profiling functionality including statistics computation,
quality scoring, and issue detection.
"""

import pytest
import pandas as pd
import numpy as np
from minio import Minio
from quality.profiler import DataProfiler


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
def profiler(minio_client):
    """Create data profiler instance."""
    return DataProfiler(minio_client)


@pytest.fixture
def sample_dataframe():
    """Create sample DataFrame for testing."""
    np.random.seed(42)
    
    return pd.DataFrame({
        "id": range(1, 101),
        "name": [f"Person_{i}" for i in range(1, 101)],
        "age": np.random.randint(18, 80, 100),
        "salary": np.random.normal(50000, 15000, 100),
        "email": [f"person{i}@example.com" if i % 10 != 0 else None for i in range(1, 101)],
        "city": np.random.choice(["NYC", "LA", "Chicago", "Houston"], 100),
        "created_at": pd.date_range("2024-01-01", periods=100, freq="D")
    })


def test_profile_dataframe_basic(profiler, sample_dataframe):
    """Test basic profiling functionality."""
    profile = profiler.profile_dataframe(
        sample_dataframe,
        "test_source",
        "test_entity"
    )
    
    assert "metadata" in profile
    assert "overview" in profile
    assert "columns" in profile
    assert "quality_score" in profile
    
    assert profile["metadata"]["total_rows"] == 100
    assert profile["metadata"]["total_columns"] == 7


def test_profile_overview(profiler, sample_dataframe):
    """Test overview statistics."""
    profile = profiler.profile_dataframe(
        sample_dataframe,
        "test_source",
        "test_entity"
    )
    
    overview = profile["overview"]
    
    assert overview["row_count"] == 100
    assert overview["column_count"] == 7
    assert overview["duplicate_rows"] == 0
    assert overview["missing_percentage"] > 0  # email has nulls


def test_profile_numeric_columns(profiler, sample_dataframe):
    """Test numeric column profiling."""
    profile = profiler.profile_dataframe(
        sample_dataframe,
        "test_source",
        "test_entity"
    )
    
    # Check age column
    age_profile = profile["columns"]["age"]
    assert "statistics" in age_profile
    assert "min" in age_profile["statistics"]
    assert "max" in age_profile["statistics"]
    assert "mean" in age_profile["statistics"]
    assert "outliers" in age_profile
    
    # Check salary column
    salary_profile = profile["columns"]["salary"]
    assert "statistics" in salary_profile
    assert salary_profile["statistics"]["std"] > 0


def test_profile_categorical_columns(profiler, sample_dataframe):
    """Test categorical column profiling."""
    profile = profiler.profile_dataframe(
        sample_dataframe,
        "test_source",
        "test_entity"
    )
    
    # Check city column
    city_profile = profile["columns"]["city"]
    assert "top_values" in city_profile
    assert len(city_profile["top_values"]) <= 10
    assert "avg_length" in city_profile
    
    # Check name column
    name_profile = profile["columns"]["name"]
    assert "top_values" in name_profile


def test_profile_datetime_columns(profiler, sample_dataframe):
    """Test datetime column profiling."""
    profile = profiler.profile_dataframe(
        sample_dataframe,
        "test_source",
        "test_entity"
    )
    
    created_at_profile = profile["columns"]["created_at"]
    assert "min_date" in created_at_profile
    assert "max_date" in created_at_profile
    assert "range_days" in created_at_profile
    assert created_at_profile["range_days"] > 0


def test_missing_values_detection(profiler):
    """Test missing value detection."""
    df = pd.DataFrame({
        "col1": [1, 2, None, 4, None],
        "col2": ["a", "b", "c", None, "e"],
        "col3": [1.0, 2.0, 3.0, 4.0, 5.0]
    })
    
    profile = profiler.profile_dataframe(df, "test", "missing_test")
    
    col1_profile = profile["columns"]["col1"]
    assert col1_profile["missing_count"] == 2
    assert col1_profile["missing_percentage"] == 40.0
    
    col2_profile = profile["columns"]["col2"]
    assert col2_profile["missing_count"] == 1
    
    col3_profile = profile["columns"]["col3"]
    assert col3_profile["missing_count"] == 0


def test_duplicates_detection(profiler):
    """Test duplicate row detection."""
    df = pd.DataFrame({
        "id": [1, 2, 2, 3, 3, 3],
        "value": ["a", "b", "b", "c", "c", "c"]
    })
    
    profile = profiler.profile_dataframe(df, "test", "duplicate_test")
    
    overview = profile["overview"]
    assert overview["duplicate_rows"] == 4  # 2+2 duplicates
    assert overview["duplicate_percentage"] > 0


def test_outliers_detection(profiler):
    """Test outlier detection."""
    # Create data with clear outliers
    data = list(range(1, 100)) + [1000, 2000]  # Add outliers
    df = pd.DataFrame({"value": data})
    
    profile = profiler.profile_dataframe(df, "test", "outlier_test")
    
    value_profile = profile["columns"]["value"]
    outliers = value_profile["outliers"]
    
    assert outliers["count"] >= 2  # Should detect the outliers
    assert outliers["percentage"] > 0


def test_quality_score_perfect_data(profiler):
    """Test quality score calculation for perfect data."""
    df = pd.DataFrame({
        "id": range(1, 101),
        "value": range(1, 101)
    })
    
    profile = profiler.profile_dataframe(df, "test", "perfect_test")
    
    # Quality score should be high (no missing, no duplicates, minimal outliers)
    assert profile["quality_score"] >= 90.0


def test_quality_score_poor_data(profiler):
    """Test quality score calculation for poor data."""
    df = pd.DataFrame({
        "id": [1, 1, 2, 2, None, None],
        "value": [None, None, None, "a", "b", "c"]
    })
    
    profile = profiler.profile_dataframe(df, "test", "poor_test")
    
    # Quality score should be low (many missing, duplicates)
    assert profile["quality_score"] < 70.0


def test_unique_values(profiler):
    """Test unique value counting."""
    df = pd.DataFrame({
        "all_unique": range(100),
        "half_unique": list(range(50)) * 2,
        "low_cardinality": ["A", "B", "C"] * 33 + ["A"]
    })
    
    profile = profiler.profile_dataframe(df, "test", "unique_test")
    
    assert profile["columns"]["all_unique"]["unique_percentage"] == 100.0
    assert profile["columns"]["half_unique"]["unique_percentage"] == 50.0
    assert profile["columns"]["low_cardinality"]["unique_count"] == 3


def test_get_latest_profile(profiler, sample_dataframe):
    """Test retrieving latest profile."""
    # Create profile
    profiler.profile_dataframe(sample_dataframe, "test_source", "profile_retrieve")
    
    # Retrieve it
    profile = profiler.get_latest_profile("test_source", "profile_retrieve")
    
    assert profile is not None
    assert profile["metadata"]["source"] == "test_source"
    assert profile["metadata"]["entity"] == "profile_retrieve"


def test_large_dataset_sampling(profiler):
    """Test that large datasets are sampled correctly."""
    # Create large dataset
    large_df = pd.DataFrame({
        "id": range(50000),
        "value": range(50000)
    })
    
    profile = profiler.profile_dataframe(
        large_df,
        "test",
        "large_test",
        sample_size=10000
    )
    
    assert profile["metadata"]["total_rows"] == 50000
    assert profile["metadata"]["sampled_rows"] == 10000


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
