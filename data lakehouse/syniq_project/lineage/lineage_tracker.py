"""
Data Lineage Tracker

Tracks data transformations across layers (source → bronze → silver → gold)
and provides lineage information for impact analysis.
"""

from datetime import datetime
from typing import List, Dict, Optional
import json
from minio import Minio
from io import BytesIO
from pathlib import Path
import sys
sys.path.append(str(Path(__file__).parent.parent))
from utils.logger import get_logger

logger = get_logger(__name__)


class LineageTracker:
    """
    Track data lineage across layers.
    
    Lineage structure:
        {
            "entity": "customers",
            "lineage": [
                {
                    "layer": "source",
                    "location": "postgres://prod/customers",
                    "timestamp": "2026-02-18T10:00:00"
                },
                {
                    "layer": "bronze",
                    "location": "bronze/postgres_prod/customers",
                    "timestamp": "2026-02-18T10:05:00",
                    "transformation": "ingestion",
                    "row_count": 10000
                },
                {
                    "layer": "silver",
                    "location": "silver/cleaned_customers",
                    "timestamp": "2026-02-18T10:10:00",
                    "transformation": "deduplication + validation",
                    "row_count": 9980
                }
            ]
        }
    """
    
    def __init__(self, minio_client: Minio):
        self.minio = minio_client
        self.bucket = "lineage"
        self._ensure_bucket()
    
    def _ensure_bucket(self):
        """Create lineage bucket if not exists"""
        try:
            if not self.minio.bucket_exists(self.bucket):
                self.minio.make_bucket(self.bucket)
                logger.info(f"✅ Created lineage bucket: {self.bucket}")
        except Exception as e:
            logger.error(f"Failed to create lineage bucket: {e}")
    
    def record_transformation(
        self,
        source_layer: str,
        source_location: str,
        target_layer: str,
        target_location: str,
        transformation: str,
        row_count: int,
        columns_used: Optional[List[str]] = None,
        metadata: Optional[Dict] = None
    ):
        """
        Record a data transformation step.
        
        Args:
            source_layer: Source layer (source, bronze, silver, gold)
            source_location: Source path/table
            target_layer: Target layer
            target_location: Target path/table
            transformation: Description of transformation
            row_count: Output row count
            columns_used: List of columns used in transformation
            metadata: Additional metadata
        """
        lineage_entry = {
            "timestamp": datetime.utcnow().isoformat(),
            "source": {
                "layer": source_layer,
                "location": source_location
            },
            "target": {
                "layer": target_layer,
                "location": target_location
            },
            "transformation": transformation,
            "metrics": {
                "row_count": row_count,
                "columns_used": columns_used or []
            },
            "metadata": metadata or {}
        }
        
        # Save to MinIO
        today = datetime.utcnow()
        safe_location = target_location.replace('/', '_').replace('\\', '_')
        path = f"transformations/{target_layer}/{today.year}/{today.month:02d}/{today.day:02d}/{safe_location}_{today.strftime('%H%M%S')}.json"
        
        data = json.dumps(lineage_entry, indent=2).encode('utf-8')
        
        try:
            self.minio.put_object(
                self.bucket,
                path,
                BytesIO(data),
                len(data),
                content_type="application/json"
            )
            logger.info(f"📊 Lineage recorded: {source_location} → {target_location}")
        except Exception as e:
            logger.error(f"Failed to record lineage: {e}")
    
    def get_lineage(self, layer: str, location: str, limit: int = 10) -> List[Dict]:
        """
        Get lineage entries for a specific location.
        
        Args:
            layer: Layer name (bronze, silver, gold)
            location: Location path
            limit: Maximum number of entries to return
        
        Returns:
            List of lineage entries
        """
        prefix = f"transformations/{layer}/"
        
        try:
            objects = self.minio.list_objects(self.bucket, prefix=prefix, recursive=True)
            
            # Filter by location
            safe_location = location.replace('/', '_').replace('\\', '_')
            matching = []
            
            for obj in objects:
                if safe_location in obj.object_name:
                    try:
                        response = self.minio.get_object(self.bucket, obj.object_name)
                        entry = json.loads(response.read().decode('utf-8'))
                        matching.append(entry)
                    except Exception as e:
                        logger.warning(f"Failed to load lineage entry {obj.object_name}: {e}")
            
            # Sort by timestamp descending
            matching.sort(key=lambda x: x.get("timestamp", ""), reverse=True)
            
            return matching[:limit]
        
        except Exception as e:
            logger.error(f"Failed to get lineage: {e}")
            return []
    
    def get_upstream_sources(self, layer: str, location: str) -> List[Dict]:
        """Get all upstream sources for a given location"""
        lineage = self.get_lineage(layer, location)
        
        sources = []
        for entry in lineage:
            source_info = entry.get("source", {})
            if source_info not in sources:
                sources.append(source_info)
        
        return sources
    
    def get_downstream_targets(self, layer: str, location: str) -> List[Dict]:
        """Get all downstream targets that depend on this location"""
        # Search for entries where this location is the source
        all_layers = ["bronze", "silver", "gold"]
        targets = []
        
        for target_layer in all_layers:
            prefix = f"transformations/{target_layer}/"
            
            try:
                objects = self.minio.list_objects(self.bucket, prefix=prefix, recursive=True)
                
                for obj in objects:
                    try:
                        response = self.minio.get_object(self.bucket, obj.object_name)
                        entry = json.loads(response.read().decode('utf-8'))
                        
                        # Check if this entry has our location as source
                        if entry.get("source", {}).get("location") == location:
                            target_info = entry.get("target", {})
                            if target_info not in targets:
                                targets.append(target_info)
                    except Exception:
                        pass
            except Exception:
                pass
        
        return targets
    
    def generate_lineage_report(self, layer: str, location: str) -> str:
        """Generate human-readable lineage report"""
        upstream = self.get_upstream_sources(layer, location)
        downstream = self.get_downstream_targets(layer, location)
        recent = self.get_lineage(layer, location, limit=5)
        
        report = []
        report.append(f"\n📊 Lineage Report: {layer}/{location}\n")
        report.append("=" * 60)
        
        # Upstream
        report.append("\n🔼 Upstream Sources:")
        if upstream:
            for src in upstream:
                report.append(f"  • {src.get('layer')}: {src.get('location')}")
        else:
            report.append("  (none)")
        
        # Downstream
        report.append("\n🔽 Downstream Targets:")
        if downstream:
            for tgt in downstream:
                report.append(f"  • {tgt.get('layer')}: {tgt.get('location')}")
        else:
            report.append("  (none)")
        
        # Recent transformations
        report.append("\n📝 Recent Transformations:")
        if recent:
            for entry in recent:
                timestamp = entry.get("timestamp", "")[:19]
                transformation = entry.get("transformation", "")
                row_count = entry.get("metrics", {}).get("row_count", 0)
                report.append(f"  • {timestamp} - {transformation} ({row_count:,} rows)")
        else:
            report.append("  (none)")
        
        report.append("\n" + "=" * 60)
        
        return "\n".join(report)
