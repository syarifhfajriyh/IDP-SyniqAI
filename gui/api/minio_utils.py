"""
MinIO Utilities for Data Lakehouse Operations
Handles MinIO connections and data operations
"""

from minio import Minio
from minio.error import S3Error
from pathlib import Path
from typing import List, Dict, Optional
import io
import os
import pandas as pd
import json
from datetime import datetime
import logging

logger = logging.getLogger(__name__)

class MinIOClient:
    def __init__(self, endpoint: str = None,
                 access_key: str = None,
                 secret_key: str = None,
                 secure: bool = False):
        """Initialize MinIO client"""
        endpoint = endpoint or os.getenv("MINIO_ENDPOINT", "localhost:9000").replace("http://", "").replace("https://", "")
        access_key = access_key or os.getenv("MINIO_ACCESS_KEY", "admin")
        secret_key = secret_key or os.getenv("MINIO_SECRET_KEY", "password123")
        self.client = Minio(
            endpoint,
            access_key=access_key,
            secret_key=secret_key,
            secure=secure
        )
        self.endpoint = endpoint
        logger.info(f"MinIO client initialized: {endpoint}")
    
    def _get_bucket_name(self, layer: str) -> str:
        """Map layer name to actual bucket name"""
        bucket_mapping = {
            "bronze": "syniqai-bronze",
            "silver": "syniqai-silver",
            "gold": "syniqai-gold",
            "quarantine": "syniqai-quarantine"
        }
        # If layer is already prefixed, return as-is
        if layer.startswith("syniqai-"):
            return layer
        return bucket_mapping.get(layer, layer)
    
    def check_connection(self) -> Dict:
        """Test MinIO connection"""
        try:
            # Try to list buckets
            buckets = self.client.list_buckets()
            return {
                "success": True,
                "endpoint": self.endpoint,
                "buckets": [bucket.name for bucket in buckets],
                "message": "Connected to MinIO"
            }
        except Exception as e:
            return {
                "success": False,
                "error": str(e),
                "message": "Failed to connect to MinIO"
            }
    
    def ensure_buckets(self, buckets: List[str] = None):
        """Ensure required buckets exist"""
        if buckets is None:
            buckets = ["syniqai-bronze", "syniqai-silver", "syniqai-gold"]
        
        for bucket in buckets:
            try:
                if not self.client.bucket_exists(bucket):
                    self.client.make_bucket(bucket)
                    logger.info(f"Created bucket: {bucket}")
            except S3Error as e:
                logger.error(f"Error creating bucket {bucket}: {e}")
    
    def list_tables(self, layer: str = "bronze") -> List[Dict]:
        """List all tables in a layer"""
        tables = []
        bucket = self._get_bucket_name(layer)
        
        try:
            # List objects in bucket
            objects = self.client.list_objects(bucket, recursive=True)
            
            # Parse folder structure: layer/source/entity/...
            seen = set()
            for obj in objects:
                parts = obj.object_name.split('/')
                if len(parts) >= 2:
                    source = parts[0]
                    entity = parts[1]
                    key = f"{source}.{entity}"
                    
                    if key not in seen:
                        seen.add(key)
                        tables.append({
                            "source": source,
                            "entity": entity,
                            "layer": layer,
                            "minio_path": f"{layer}/{source}/{entity}/",
                            "last_modified": obj.last_modified.isoformat() if hasattr(obj, 'last_modified') else None
                        })
            
            logger.info(f"Found {len(tables)} tables in {layer} layer")
            return tables
        
        except Exception as e:
            logger.error(f"Error listing tables in {layer}: {e}")
            return []
    
    def get_table_info(self, layer: str, source: str, entity: str) -> Optional[Dict]:
        """Get information about a specific table"""
        bucket = self._get_bucket_name(layer)
        try:
            prefix = f"{source}/{entity}/"
            objects = list(self.client.list_objects(bucket, prefix=prefix, recursive=True))
            
            if not objects:
                return None
            
            total_size = sum(obj.size for obj in objects)
            file_count = len(objects)
            
            # Get row count from metadata if available
            metadata_path = f"{source}/{entity}/_metadata.json"
            row_count = None
            
            try:
                data = self.client.get_object(bucket, metadata_path)
                metadata = json.loads(data.read().decode('utf-8'))
                row_count = metadata.get('row_count')
            except:
                pass
            
            return {
                "source": source,
                "entity": entity,
                "layer": layer,
                "file_count": file_count,
                "total_size_bytes": total_size,
                "row_count": row_count,
                "last_modified": max(obj.last_modified for obj in objects).isoformat()
            }
        
        except Exception as e:
            logger.error(f"Error getting table info for {layer}/{source}/{entity}: {e}")
            return None
    
    def read_parquet(self, layer: str, source: str, entity: str, 
                     limit: int = None) -> Optional[pd.DataFrame]:
        """Read Parquet data from MinIO
        
        Handles multiple storage formats:
        - Standard 2-level: source/entity/file.parquet
        - Standard 3-level: source/source_type/entity/file.parquet
        - Iceberg format: iceberg/source/entity/data/*.parquet
        """
        bucket = self._get_bucket_name(layer)
        try:
            # First, check if this is an Iceberg table
            iceberg_prefix = f"iceberg/{source}/{entity}/data/"
            iceberg_objects = list(self.client.list_objects(bucket, prefix=iceberg_prefix, recursive=True))
            iceberg_files = [obj.object_name for obj in iceberg_objects if obj.object_name.endswith('.parquet')]
            
            if iceberg_files:
                logger.info(f"Reading Iceberg table: {source}.{entity} ({len(iceberg_files)} files)")
                
                # Read all Iceberg data files
                dfs = []
                files_to_read = iceberg_files[:5] if limit else iceberg_files  # Read up to 5 files if limited
                for file_path in files_to_read:
                    data = self.client.get_object(bucket, file_path)
                    df = pd.read_parquet(io.BytesIO(data.read()))
                    dfs.append(df)
                
                result = pd.concat(dfs, ignore_index=True)
                
                if limit:
                    result = result.head(limit)
                
                logger.info(f"Read {len(result)} rows from Iceberg table {source}/{entity}")
                return result
            
            # Try standard parquet format
            # Try exact path first (2-level structure)
            prefix = f"{source}/{entity}/"
            objects = list(self.client.list_objects(bucket, prefix=prefix, recursive=True))
            parquet_files = [obj.object_name for obj in objects if obj.object_name.endswith('.parquet')]
            
            # If not found, try searching under source with entity in path (3-level structure)
            if not parquet_files:
                logger.info(f"No files at {prefix}, searching {source}/* for {entity}")
                prefix = f"{source}/"
                objects = list(self.client.list_objects(bucket, prefix=prefix, recursive=True))
                # Filter for paths containing entity name
                parquet_files = [obj.object_name for obj in objects 
                               if obj.object_name.endswith('.parquet') and f"/{entity}/" in obj.object_name]
            
            if not parquet_files:
                logger.warning(f"No Parquet files found for {layer}/{source}/{entity}")
                return None
            
            logger.info(f"Found {len(parquet_files)} parquet files for {source}/{entity}")
            
            # Read all files (or just first if limit specified)
            dfs = []
            files_to_read = parquet_files[:1] if limit else parquet_files
            for file_path in files_to_read:
                data = self.client.get_object(bucket, file_path)
                df = pd.read_parquet(io.BytesIO(data.read()))
                dfs.append(df)
            
            result = pd.concat(dfs, ignore_index=True)
            
            if limit:
                result = result.head(limit)
            
            logger.info(f"Read {len(result)} rows from {layer}/{source}/{entity}")
            return result
        
        except Exception as e:
            logger.error(f"Error reading Parquet from {layer}/{source}/{entity}: {e}")
            return None
    
    def save_metadata(self, layer: str, source: str, entity: str, metadata: Dict):
        """Save metadata for a table"""
        bucket = self._get_bucket_name(layer)
        try:
            metadata_path = f"{source}/{entity}/_metadata.json"
            metadata_json = json.dumps(metadata, indent=2)
            
            self.client.put_object(
                bucket,
                metadata_path,
                io.BytesIO(metadata_json.encode('utf-8')),
                len(metadata_json),
                content_type='application/json'
            )
            
            logger.info(f"Saved metadata for {layer}/{source}/{entity}")
        
        except Exception as e:
            logger.error(f"Error saving metadata: {e}")
    
    def get_eda_report(self, source: str, entity: str) -> Optional[Dict]:
        """Get the latest EDA report from Gold layer"""
        bucket = self._get_bucket_name("gold")
        try:
            # Try to find EDA report
            prefix = f"eda_reports/{source}_{entity}_eda_"
            objects = list(self.client.list_objects(bucket, prefix=prefix, recursive=False))
            
            if not objects:
                return None
            
            # Get latest report
            latest = max(objects, key=lambda obj: obj.last_modified)
            
            data = self.client.get_object(bucket, latest.object_name)
            report = json.loads(data.read().decode('utf-8'))
            
            logger.info(f"Retrieved EDA report for {source}.{entity}")
            return report
        
        except Exception as e:
            logger.error(f"Error getting EDA report for {source}.{entity}: {e}")
            return None
    
    def save_eda_report(self, source: str, entity: str, report: Dict):
        """Save EDA report to Gold layer"""
        bucket = self._get_bucket_name("gold")
        try:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            report_path = f"eda_reports/{source}_{entity}_eda_{timestamp}.json"
            
            report_json = json.dumps(report, indent=2)
            
            self.client.put_object(
                bucket,
                report_path,
                io.BytesIO(report_json.encode('utf-8')),
                len(report_json),
                content_type='application/json'
            )
            
            logger.info(f"Saved EDA report to gold/{report_path}")
            return report_path
        
        except Exception as e:
            logger.error(f"Error saving EDA report: {e}")
            return None
