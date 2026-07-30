"""
Tests for Schema Evolution

Tests schema evolution detection and compatibility checking.
"""

import pytest
from catalog.schema_evolution import SchemaEvolution


def test_detect_no_changes():
    """Test when schemas are identical."""
    old_schema = {
        "id": "int64",
        "name": "object",
        "email": "object"
    }
    new_schema = old_schema.copy()
    
    changes = SchemaEvolution.detect_changes(old_schema, new_schema)
    
    assert changes["is_compatible"] == True
    assert len(changes["added_columns"]) == 0
    assert len(changes["removed_columns"]) == 0
    assert len(changes["type_changes"]) == 0


def test_detect_added_columns():
    """Test detection of added columns."""
    old_schema = {
        "id": "int64",
        "name": "object"
    }
    new_schema = {
        "id": "int64",
        "name": "object",
        "email": "object",
        "phone": "object"
    }
    
    changes = SchemaEvolution.detect_changes(old_schema, new_schema)
    
    assert changes["is_compatible"] == True
    assert "email" in changes["added_columns"]
    assert "phone" in changes["added_columns"]
    assert len(changes["added_columns"]) == 2


def test_detect_removed_columns():
    """Test detection of removed columns (breaking change)."""
    old_schema = {
        "id": "int64",
        "name": "object",
        "email": "object"
    }
    new_schema = {
        "id": "int64",
        "name": "object"
    }
    
    changes = SchemaEvolution.detect_changes(old_schema, new_schema)
    
    assert changes["is_compatible"] == False
    assert "email" in changes["removed_columns"]


def test_detect_type_changes():
    """Test detection of type changes (breaking change)."""
    old_schema = {
        "id": "int64",
        "amount": "float64"
    }
    new_schema = {
        "id": "object",  # int -> string
        "amount": "int64"  # float -> int
    }
    
    changes = SchemaEvolution.detect_changes(old_schema, new_schema)
    
    assert changes["is_compatible"] == False
    assert len(changes["type_changes"]) == 2
    
    # Check type change details
    type_changes = {tc["column"]: tc for tc in changes["type_changes"]}
    assert type_changes["id"]["old_type"] == "int64"
    assert type_changes["id"]["new_type"] == "object"
    assert type_changes["amount"]["old_type"] == "float64"
    assert type_changes["amount"]["new_type"] == "int64"


def test_detect_mixed_changes():
    """Test detection of multiple change types."""
    old_schema = {
        "id": "int64",
        "name": "object",
        "old_field": "object"
    }
    new_schema = {
        "id": "object",  # type change
        "name": "object",
        "new_field": "object"  # added
        # old_field removed
    }
    
    changes = SchemaEvolution.detect_changes(old_schema, new_schema)
    
    assert changes["is_compatible"] == False
    assert "new_field" in changes["added_columns"]
    assert "old_field" in changes["removed_columns"]
    assert len(changes["type_changes"]) == 1


def test_is_compatible_change():
    """Test compatibility checking."""
    # Compatible change (only additions)
    compatible = {
        "is_compatible": True,
        "added_columns": ["new_col"],
        "removed_columns": [],
        "type_changes": []
    }
    assert SchemaEvolution.is_compatible_change(compatible) == True
    
    # Incompatible change (has removals)
    incompatible = {
        "is_compatible": False,
        "added_columns": [],
        "removed_columns": ["old_col"],
        "type_changes": []
    }
    assert SchemaEvolution.is_compatible_change(incompatible) == False


def test_get_change_summary_no_changes():
    """Test change summary for no changes."""
    changes = {
        "is_compatible": True,
        "added_columns": [],
        "removed_columns": [],
        "type_changes": []
    }
    
    summary = SchemaEvolution.get_change_summary(changes)
    assert summary == "No schema changes"


def test_get_change_summary_additions():
    """Test change summary for additions."""
    changes = {
        "is_compatible": True,
        "added_columns": ["email", "phone"],
        "removed_columns": [],
        "type_changes": []
    }
    
    summary = SchemaEvolution.get_change_summary(changes)
    assert "email" in summary
    assert "phone" in summary
    assert "✅ Compatible" in summary


def test_get_change_summary_breaking():
    """Test change summary for breaking changes."""
    changes = {
        "is_compatible": False,
        "added_columns": [],
        "removed_columns": ["old_field"],
        "type_changes": [
            {"column": "id", "old_type": "int64", "new_type": "object"}
        ]
    }
    
    summary = SchemaEvolution.get_change_summary(changes)
    assert "old_field" in summary
    assert "id" in summary
    assert "⚠️  Breaking change" in summary


def test_validate_evolution_compatible():
    """Test validation of compatible evolution."""
    old_schema = {"id": "int64", "name": "object"}
    new_schema = {"id": "int64", "name": "object", "email": "object"}
    
    is_valid, message = SchemaEvolution.validate_evolution(old_schema, new_schema)
    
    assert is_valid == True
    assert "Compatible" in message


def test_validate_evolution_incompatible_not_allowed():
    """Test validation of incompatible evolution when not allowed."""
    old_schema = {"id": "int64", "name": "object", "email": "object"}
    new_schema = {"id": "int64", "name": "object"}
    
    is_valid, message = SchemaEvolution.validate_evolution(
        old_schema,
        new_schema,
        allow_breaking=False
    )
    
    assert is_valid == False
    assert "Breaking change not allowed" in message


def test_validate_evolution_incompatible_allowed():
    """Test validation of incompatible evolution when allowed."""
    old_schema = {"id": "int64", "name": "object", "email": "object"}
    new_schema = {"id": "int64", "name": "object"}
    
    is_valid, message = SchemaEvolution.validate_evolution(
        old_schema,
        new_schema,
        allow_breaking=True
    )
    
    assert is_valid == True
    assert "Breaking change allowed" in message


def test_column_order_not_considered_change():
    """Test that column order changes don't trigger type changes."""
    old_schema = {
        "id": "int64",
        "name": "object",
        "email": "object"
    }
    new_schema = {
        "email": "object",
        "id": "int64",
        "name": "object"
    }
    
    changes = SchemaEvolution.detect_changes(old_schema, new_schema)
    
    assert changes["is_compatible"] == True
    assert len(changes["type_changes"]) == 0


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
