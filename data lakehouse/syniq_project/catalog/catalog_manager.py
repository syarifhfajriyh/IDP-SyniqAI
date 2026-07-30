"""
Catalog Manager - Centralized metadata management for data lakehouse
"""

from datetime import datetime
from pathlib import Path
import json
import hashlib
from typing import Dict, List, Optional
from minio import Minio
from io import BytesIO
import sys
sys.path.append(str(Path(__file__).parent.parent))
from utils.logger import get_logger

logger = get_logger(__name__)


class CatalogManager:
    """
    Manages table metadata catalog in MinIO.
    
    Structure:
        catalog/
        ├── tables/
        │   └── {source}/{entity}/
        │       ├── current.json          # Current version metadata
        │       └── history/
        │           ├── v1.json
        │           ├── v2.json
        │           └── v3.json
        └── index/
            └── all_tables.json           # Quick lookup index
    """
    
    def __init__(self, minio_client: Minio, bucket: str = "catalog"):
        self.minio = minio_client
        self.bucket = bucket
        self._ensure_bucket()
    
    def _ensure_bucket(self):
        """Create catalog bucket if not exists"""
        try:
            if not self.minio.bucket_exists(self.bucket):
                self.minio.make_bucket(self.bucket)
                logger.info(f"✅ Created catalog bucket: {self.bucket}")
        except Exception as e:
            logger.error(f"Failed to create catalog bucket: {e}")
    
    def register_table(
        self,
        source: str,
        entity: str,
        schema: Dict,
        location: str,
        row_count: int,
        partition_keys: Optional[List[str]] = None,
        tags: Optional[Dict[str, str]] = None
    ) -> Dict:
        """
        Register a new table or update existing.
        
        Args:
            source: Source system (e.g., 'postgres_prod')
            entity: Table/collection name (e.g., 'customers')
            schema: Column definitions with types
            location: MinIO path (e.g., 'bronze/postgres_prod/customers')
            row_count: Number of rows
            partition_keys: Partition columns (e.g., ['year', 'month', 'day'])
            tags: Custom metadata tags
        
        Returns:
            Metadata dictionary with version info
        """
        # Generate schema hash for change detection
        schema_hash = self._hash_schema(schema)
        
        # Load existing metadata if any
        existing = self._load_current_metadata(source, entity)
        
        # Determine version
        if existing and existing.get("schema_hash") != schema_hash:
            version = existing.get("version", 0) + 1
            logger.info(f"Schema changed for {source}.{entity}, creating version {version}")
        else:
            version = existing.get("version", 1) if existing else 1
        
        # Create metadata
        metadata = {
            "source": source,
            "entity": entity,
            "version": version,
            "schema": schema,
            "schema_hash": schema_hash,
            "location": location,
            "partition_keys": partition_keys or [],
            "row_count": row_count,
            "tags": tags or {},
            "created_at": existing.get("created_at", datetime.utcnow().isoformat()) if existing else datetime.utcnow().isoformat(),
            "updated_at": datetime.utcnow().isoformat(),
            "status": "active"
        }
        
        # Save current version
        self._save_metadata(source, entity, metadata, "current.json")
        
        # Save to history
        self._save_metadata(source, entity, metadata, f"history/v{version}.json")
        
        # Update index
        self._update_index(source, entity, metadata)
        
        logger.info(f"✅ Registered {source}.{entity} (v{version}) - {row_count:,} rows")
        return metadata
    
    def get_table(self, source: str, entity: str, version: Optional[int] = None) -> Optional[Dict]:
        """Get table metadata by source and entity"""
        if version:
            path = f"tables/{source}/{entity}/history/v{version}.json"
        else:
            path = f"tables/{source}/{entity}/current.json"
        
        return self._load_from_minio(path)
    
    def list_tables(
        self,
        source: Optional[str] = None,
        tags: Optional[Dict[str, str]] = None
    ) -> List[Dict]:
        """List all tables, optionally filtered by source or tags"""
        index = self._load_from_minio("index/all_tables.json") or {"tables": []}
        tables = index.get("tables", [])
        
        # Filter by source
        if source:
            tables = [t for t in tables if t.get("source") == source]
        
        # Filter by tags
        if tags:
            tables = [
                t for t in tables 
                if all(t.get("tags", {}).get(k) == v for k, v in tags.items())
            ]
        
        return tables
    
    def get_schema_history(self, source: str, entity: str) -> List[Dict]:
        """Get all schema versions for a table"""
        current = self.get_table(source, entity)
        if not current:
            return []
        
        versions = []
        for v in range(1, current.get("version", 0) + 1):
            meta = self.get_table(source, entity, version=v)
            if meta:
                versions.append({
                    "version": v,
                    "schema_hash": meta.get("schema_hash"),
                    "updated_at": meta.get("updated_at"),
                    "row_count": meta.get("row_count")
                })
        
        return versions
    
    def search_tables(self, query: str) -> List[Dict]:
        """Search tables by name"""
        tables = self.list_tables()
        query_lower = query.lower()
        
        return [
            t for t in tables 
            if query_lower in t.get("source", "").lower() 
            or query_lower in t.get("entity", "").lower()
        ]
    
    # Helper methods
    
    def _hash_schema(self, schema: Dict) -> str:
        """Generate hash of schema for change detection"""
        schema_str = json.dumps(schema, sort_keys=True)
        return hashlib.sha256(schema_str.encode()).hexdigest()[:16]
    
    def _load_current_metadata(self, source: str, entity: str) -> Optional[Dict]:
        """Load current metadata for a table"""
        return self._load_from_minio(f"tables/{source}/{entity}/current.json")
    
    def _save_metadata(self, source: str, entity: str, metadata: Dict, filename: str):
        """Save metadata to MinIO"""
        path = f"tables/{source}/{entity}/{filename}"
        data = json.dumps(metadata, indent=2).encode('utf-8')
        
        try:
            self.minio.put_object(
                self.bucket,
                path,
                BytesIO(data),
                len(data),
                content_type="application/json"
            )
        except Exception as e:
            logger.error(f"Failed to save metadata to {path}: {e}")
    
    def _load_from_minio(self, path: str) -> Optional[Dict]:
        """Load JSON from MinIO"""
        try:
            response = self.minio.get_object(self.bucket, path)
            data = response.read()
            return json.loads(data.decode('utf-8'))
        except Exception:
            return None
    
    def _update_index(self, source: str, entity: str, metadata: Dict):
        """Update the global index of all tables"""
        index = self._load_from_minio("index/all_tables.json") or {"tables": [], "updated_at": None}
        
        # Remove old entry if exists
        tables = [t for t in index.get("tables", []) if not (t.get("source") == source and t.get("entity") == entity)]
        
        # Add new entry
        tables.append({
            "source": source,
            "entity": entity,
            "version": metadata.get("version"),
            "location": metadata.get("location"),
            "row_count": metadata.get("row_count"),
            "tags": metadata.get("tags", {}),
            "updated_at": metadata.get("updated_at")
        })
        
        index["tables"] = tables
        index["updated_at"] = datetime.utcnow().isoformat()
        
        # Save index
        data = json.dumps(index, indent=2).encode('utf-8')
        try:
            self.minio.put_object(
                self.bucket,
                "index/all_tables.json",
                BytesIO(data),
                len(data),
                content_type="application/json"
            )
        except Exception as e:
            logger.error(f"Failed to update index: {e}")
