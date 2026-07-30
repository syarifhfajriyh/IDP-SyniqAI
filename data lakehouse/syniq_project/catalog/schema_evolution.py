"""
Schema Evolution Handler

Handles schema evolution strategies and change detection for data lakehouse tables.
Prepares infrastructure for Apache Iceberg integration.
"""

from typing import Dict, List, Set
from pathlib import Path
import sys
sys.path.append(str(Path(__file__).parent.parent))
from utils.logger import get_logger

logger = get_logger(__name__)


class SchemaEvolution:
    """
    Handles schema evolution strategies.
    
    Strategies:
    1. ADD_COLUMN: New columns allowed (backward compatible)
    2. DROP_COLUMN: Columns can be removed (breaking change)
    3. RENAME_COLUMN: Column renaming (breaking change)
    4. CHANGE_TYPE: Data type changes (breaking change)
    """
    
    @staticmethod
    def detect_changes(old_schema: Dict, new_schema: Dict) -> Dict:
        """
        Detect schema changes between versions.
        
        Args:
            old_schema: Previous schema {column: type}
            new_schema: New schema {column: type}
        
        Returns:
            {
                "added_columns": [...],
                "removed_columns": [...],
                "type_changes": [{"column": ..., "old_type": ..., "new_type": ...}],
                "is_compatible": bool
            }
        """
        changes = {
            "added_columns": [],
            "removed_columns": [],
            "type_changes": [],
            "is_compatible": True
        }
        
        old_cols = set(old_schema.keys())
        new_cols = set(new_schema.keys())
        
        # Detect added columns
        added = new_cols - old_cols
        if added:
            changes["added_columns"] = sorted(list(added))
            logger.info(f"✅ Schema evolution: Added columns {changes['added_columns']}")
        
        # Detect removed columns (breaking!)
        removed = old_cols - new_cols
        if removed:
            changes["removed_columns"] = sorted(list(removed))
            changes["is_compatible"] = False
            logger.warning(f"⚠️  Schema evolution: Removed columns {changes['removed_columns']}")
        
        # Detect type changes (breaking!)
        for col in old_cols & new_cols:
            if old_schema[col] != new_schema[col]:
                change = {
                    "column": col,
                    "old_type": old_schema[col],
                    "new_type": new_schema[col]
                }
                changes["type_changes"].append(change)
                changes["is_compatible"] = False
                logger.warning(f"⚠️  Schema evolution: Type changed for '{col}': {old_schema[col]} → {new_schema[col]}")
        
        return changes
    
    @staticmethod
    def is_compatible_change(changes: Dict) -> bool:
        """Check if schema change is backward compatible"""
        return changes.get("is_compatible", True)
    
    @staticmethod
    def get_change_summary(changes: Dict) -> str:
        """Get human-readable summary of changes"""
        if not any([changes["added_columns"], changes["removed_columns"], changes["type_changes"]]):
            return "No schema changes"
        
        summary_parts = []
        
        if changes["added_columns"]:
            summary_parts.append(f"Added: {', '.join(changes['added_columns'])}")
        
        if changes["removed_columns"]:
            summary_parts.append(f"Removed: {', '.join(changes['removed_columns'])}")
        
        if changes["type_changes"]:
            type_changes_str = ", ".join([
                f"{tc['column']} ({tc['old_type']}→{tc['new_type']})"
                for tc in changes["type_changes"]
            ])
            summary_parts.append(f"Type changes: {type_changes_str}")
        
        compatibility = "✅ Compatible" if changes["is_compatible"] else "⚠️  Breaking change"
        
        return f"{compatibility} | {' | '.join(summary_parts)}"
    
    @staticmethod
    def validate_evolution(old_schema: Dict, new_schema: Dict, allow_breaking: bool = False) -> tuple[bool, str]:
        """
        Validate if schema evolution is allowed.
        
        Args:
            old_schema: Previous schema
            new_schema: New schema
            allow_breaking: Whether to allow breaking changes
        
        Returns:
            (is_valid, message)
        """
        changes = SchemaEvolution.detect_changes(old_schema, new_schema)
        
        if changes["is_compatible"]:
            return True, SchemaEvolution.get_change_summary(changes)
        
        if allow_breaking:
            return True, f"Breaking change allowed: {SchemaEvolution.get_change_summary(changes)}"
        
        return False, f"Breaking change not allowed: {SchemaEvolution.get_change_summary(changes)}"
