"""
Metadata Catalog Module

Provides centralized table metadata management, schema versioning,
and discovery capabilities for the Syniq Data Lakehouse.
"""

from .catalog_manager import CatalogManager
from .schema_evolution import SchemaEvolution

__all__ = ['CatalogManager', 'SchemaEvolution']
