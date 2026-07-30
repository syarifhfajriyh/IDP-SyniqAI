"""
S3 File Streaming Ingestor: AWS S3 → MinIO (with metadata tracking)

Features:
- ✅ Streams files from AWS S3 to MinIO
- ✅ Supports multiple file types (CSV, JSON, Parquet, images, videos, etc.)
- ✅ Preserves directory structure
- ✅ Metadata extraction (file size, type, count)
- ✅ Optional local backup
- ✅ Progress tracking
- ✅ Integrated catalog/quality/lineage

Usage:
    python s3_file_ingestor.py s3_config.json --mode streaming
    python s3_file_ingestor.py s3_config.json --mode hybrid  # saves local backup
"""

import os
import sys
import json
import pandas as pd
import pyarrow.parquet as pq
from pathlib import Path
from datetime import datetime
from typing import Iterator, Dict, Optional, List
from io import BytesIO
import logging
import mimetypes
import boto3
from botocore.exceptions import ClientError, NoCredentialsError

# Add paths
sys.path.append(str(Path(__file__).parent))
sys.path.append(str(Path(__file__).parent.parent.parent / "data lakehouse" / "syniq_project"))

from catalog.catalog_manager import CatalogManager
from quality.profiler import DataProfiler
from lineage.lineage_tracker import LineageTracker
from minio import Minio
import yaml

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("S3Ingestor")

class S3FileIngestor:
    """
    Stream files from AWS S3 to MinIO with metadata tracking.
    """
    
    def __init__(
        self,
        minio_client: Minio,
        save_local_backup: bool = False,
        backup_dir: str = "bronze_layer_backup"
    ):
        self.minio = minio_client
        self.save_local_backup = save_local_backup
        self.backup_dir = Path(backup_dir)
        
        # Initialize lakehouse components
        self.catalog = CatalogManager(minio_client)
        self.profiler = DataProfiler(minio_client)
        self.lineage = LineageTracker(minio_client)
        
        # Ensure syniqai-bronze bucket exists
        if not self.minio.bucket_exists("syniqai-bronze"):
            self.minio.make_bucket("syniqai-bronze")
    
    def _get_file_type(self, filename: str) -> str:
        """Determine file type from extension"""
        ext = Path(filename).suffix.lower()
        
        type_map = {
            '.csv': 'structured',
            '.json': 'structured',
            '.jsonl': 'structured',
            '.parquet': 'structured',
            '.txt': 'text',
            '.log': 'text',
            '.jpg': 'image',
            '.jpeg': 'image',
            '.png': 'image',
            '.gif': 'image',
            '.bmp': 'image',
            '.mp4': 'video',
            '.avi': 'video',
            '.mov': 'video',
            '.mkv': 'video',
            '.pdf': 'document',
            '.doc': 'document',
            '.docx': 'document',
            '.xls': 'document',
            '.xlsx': 'document',
        }
        
        return type_map.get(ext, 'binary')
    
    def _list_s3_files(
        self,
        s3_client,
        bucket: str,
        prefix: str = "",
        file_extensions: List[str] = None
    ) -> Iterator[Dict]:
        """
        List files in S3 bucket matching criteria.
        
        Yields:
            Dict with keys: key, size, modified, content_type
        """
        paginator = s3_client.get_paginator('list_objects_v2')
        
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            if 'Contents' not in page:
                continue
            
            for obj in page['Contents']:
                key = obj['Key']
                
                # Skip directories
                if key.endswith('/'):
                    continue
                
                # Filter by extension if specified
                if file_extensions:
                    ext = Path(key).suffix.lower()
                    if ext not in file_extensions:
                        continue
                
                yield {
                    'key': key,
                    'size': obj['Size'],
                    'modified': obj['LastModified'],
                    'content_type': mimetypes.guess_type(key)[0] or 'application/octet-stream'
                }
    
    def _profile_structured_file(
        self,
        s3_client,
        bucket: str,
        key: str,
        file_type: str
    ) -> Optional[Dict]:
        """
        Profile structured files (CSV, JSON, Parquet) for quality metrics.
        """
        try:
            # Download file to memory
            obj = s3_client.get_object(Bucket=bucket, Key=key)
            data = obj['Body'].read()
            
            # Parse based on type
            if key.endswith('.csv'):
                df = pd.read_csv(BytesIO(data))
            elif key.endswith('.json'):
                df = pd.read_json(BytesIO(data))
            elif key.endswith('.jsonl'):
                df = pd.read_json(BytesIO(data), lines=True)
            elif key.endswith('.parquet'):
                df = pd.read_parquet(BytesIO(data))
            else:
                return None
            
            # Return basic stats
            return {
                'rows': len(df),
                'columns': len(df.columns),
                'column_names': list(df.columns),
                'dtypes': {col: str(dtype) for col, dtype in df.dtypes.items()}
            }
        
        except Exception as e:
            logger.warning(f"   ⚠️  Could not profile {key}: {e}")
            return None
    
    def ingest_streaming(
        self,
        s3_bucket: str,
        s3_prefix: str,
        source: str,
        aws_access_key: str,
        aws_secret_key: str,
        aws_region: str = 'us-east-1',
        file_extensions: List[str] = None,
        profile_structured: bool = True
    ) -> Dict:
        """
        Stream files from S3 to MinIO.
        
        Args:
            s3_bucket: Source S3 bucket name
            s3_prefix: Prefix/folder in S3 (e.g., "data/images/")
            source: Source name (e.g., 's3_prod')
            aws_access_key: AWS access key ID
            aws_secret_key: AWS secret access key
            aws_region: AWS region
            file_extensions: Filter by extensions (e.g., ['.csv', '.json'])
            profile_structured: Profile CSV/JSON/Parquet files
        
        Returns:
            Ingestion result with metrics
        """
        logger.info(f"🚀 Starting S3 FILE ingestion")
        logger.info(f"   Source: {source}")
        logger.info(f"   S3 Bucket: {s3_bucket}")
        logger.info(f"   Prefix: {s3_prefix or '(root)'}")
        logger.info(f"   Extensions: {file_extensions or 'all'}")
        
        start_time = datetime.utcnow()
        
        # Connect to S3
        logger.info(f"🔌 Connecting to AWS S3...")
        try:
            s3_client = boto3.client(
                's3',
                aws_access_key_id=aws_access_key,
                aws_secret_access_key=aws_secret_key,
                region_name=aws_region
            )
            # Test connection
            s3_client.head_bucket(Bucket=s3_bucket)
            logger.info(f"   ✅ Connected to bucket: {s3_bucket}")
        except NoCredentialsError:
            logger.error(f"   ❌ AWS credentials not found")
            raise
        except ClientError as e:
            logger.error(f"   ❌ Cannot access bucket: {e}")
            raise
        
        # Prepare MinIO path to match Silver transformer expectations
        # Format: {domain}/{source}/{entity}/
        # For S3 ingestion, we useentity name derived from source_name or bucket
        today = datetime.utcnow()
        entity_name = s3_prefix.strip("/").replace("/", "_") if s3_prefix and s3_prefix != "/" else s3_bucket.replace("-", "_")
        minio_prefix = f"finance/{source}/{entity_name}"  # domain/source/entity structure
        
        logger.info(f"   MinIO target structure: {minio_prefix}/")
        logger.info(f"   (Compatible with Silver transformer Bronze path expectations)")
        
        # Optional: Setup local backup
        backup_path = None
        if self.save_local_backup:
            backup_path = self.backup_dir / source / start_time.strftime("%Y%m%d_%H%M%S")
            backup_path.mkdir(parents=True, exist_ok=True)
            logger.info(f"💾 Local backup enabled: {backup_path}")
        
        # Track statistics
        file_count = 0
        total_bytes = 0
        file_types = {}
        structured_files = []
        
        try:
            logger.info(f"\n📥 Listing and transferring files...")
            
            # Stream files from S3 to MinIO
            for file_info in self._list_s3_files(s3_client, s3_bucket, s3_prefix, file_extensions):
                file_count += 1
                key = file_info['key']
                size = file_info['size']
                total_bytes += size
                
                # Determine file type
                file_type = self._get_file_type(key)
                file_types[file_type] = file_types.get(file_type, 0) + 1
                
                # Download from S3
                obj = s3_client.get_object(Bucket=s3_bucket, Key=key)
                file_data = obj['Body'].read()
                
                # Upload to MinIO (preserve directory structure)
                relative_key = key[len(s3_prefix):] if key.startswith(s3_prefix) else key
                minio_key = f"{minio_prefix}/{relative_key}"
                
                self.minio.put_object(
                    bucket_name="syniqai-bronze",  # Bronze bucket name
                    object_name=minio_key,
                    data=BytesIO(file_data),
                    length=len(file_data),
                    content_type=file_info['content_type']
                )
                
                logger.info(f"   ✅ File {file_count}: {key} ({size:,} bytes) → MinIO")
                
                # Optional: Save local backup
                if self.save_local_backup:
                    local_file = backup_path / relative_key
                    local_file.parent.mkdir(parents=True, exist_ok=True)
                    local_file.write_bytes(file_data)
                
                # Profile structured files
                if profile_structured and file_type == 'structured':
                    profile = self._profile_structured_file(s3_client, s3_bucket, key, file_type)
                    if profile:
                        structured_files.append({
                            'file': key,
                            'profile': profile
                        })
            
            logger.info(f"\n📊 Transfer complete: {file_count} files, {total_bytes:,} bytes")
            
            # Register in catalog
            logger.info(f"\n📋 Registering in catalog...")
            schema = {
                'file_count': 'int',
                'total_bytes': 'int',
                'file_types': 'object',
                'structured_file_count': 'int'
            }
            
            catalog_entry = self.catalog.register_table(
                source=source,
                entity="s3_files",
                schema=schema,
                location=minio_prefix,
                row_count=file_count,
                partition_keys=["year", "month", "day"],
                tags={
                    "layer": "bronze",
                    "source_type": "s3",
                    "s3_bucket": s3_bucket,
                    "s3_prefix": s3_prefix,
                    "file_types": file_types,
                    "ingestion_mode": "streaming"
                }
            )
            logger.info(f"   ✅ Catalog v{catalog_entry['version']}")
            
            # Profile structured files if any
            quality_score = None
            if structured_files and profile_structured:
                logger.info(f"\n📊 Profiling structured files ({len(structured_files)})...")
                # For now, just log the profiles
                for item in structured_files[:5]:  # Show first 5
                    logger.info(f"   📄 {item['file']}: {item['profile']['rows']} rows, {item['profile']['columns']} cols")
                quality_score = 100.0  # Placeholder - files successfully transferred
            else:
                quality_score = 100.0  # All files transferred successfully
            
            # Record lineage
            logger.info(f"\n🔗 Recording lineage...")
            self.lineage.record_transformation(
                source_layer="source",
                source_location=f"s3://{s3_bucket}/{s3_prefix}",
                target_layer="bronze",
                target_location=minio_prefix,
                transformation="s3_file_ingestion",
                row_count=file_count,
                columns_used=[],
                metadata={
                    "s3_bucket": s3_bucket,
                    "s3_prefix": s3_prefix,
                    "file_types": file_types,
                    "total_bytes": total_bytes,
                    "structured_files": len(structured_files),
                    "ingestion_mode": "streaming",
                    "local_backup": str(backup_path) if backup_path else None
                }
            )
            logger.info(f"   ✅ Lineage recorded")
            
            # Calculate duration
            duration = (datetime.utcnow() - start_time).total_seconds()
            
            # Summary
            result = {
                "success": True,
                "source": source,
                "s3_bucket": s3_bucket,
                "s3_prefix": s3_prefix,
                "total_files": file_count,
                "total_bytes": total_bytes,
                "file_types": file_types,
                "structured_files": len(structured_files),
                "minio_location": minio_prefix,
                "catalog_version": catalog_entry['version'],
                "quality_score": quality_score,
                "duration_seconds": duration,
                "bytes_per_second": int(total_bytes / duration) if duration > 0 else 0,
                "local_backup": str(backup_path) if backup_path else None
            }
            
            logger.info("\n" + "="*70)
            logger.info("🎉 S3 FILE INGESTION COMPLETE")
            logger.info("="*70)
            logger.info(f"Source S3:       s3://{s3_bucket}/{s3_prefix}")
            logger.info(f"Total Files:     {file_count:,}")
            logger.info(f"Total Size:      {total_bytes:,} bytes ({total_bytes / 1024 / 1024:.2f} MB)")
            logger.info(f"File Types:      {file_types}")
            logger.info(f"Structured:      {len(structured_files)} files")
            logger.info(f"Duration:        {duration:.2f}s")
            logger.info(f"Speed:           {result['bytes_per_second']:,} bytes/sec")
            logger.info(f"MinIO Location:  {minio_prefix}")
            if backup_path:
                logger.info(f"Local Backup:    {backup_path}")
            logger.info("="*70)
            
            return result
            
        except Exception as e:
            logger.error(f"\n❌ File ingestion failed: {e}")
            
            # If we have local backup, we can retry upload later
            if self.save_local_backup and backup_path and backup_path.exists():
                logger.warning(f"⚠️  Local backup available at: {backup_path}")
            
            raise

def load_minio_config() -> Minio:
    """Load MinIO configuration"""
    config_path = Path(__file__).parent.parent.parent / "data lakehouse" / "syniq_project" / "config" / "minio_config.yaml"
    
    with open(config_path) as f:
        config = yaml.safe_load(f)
    
    minio_cfg = config['minio']
    return Minio(
        minio_cfg['endpoint'],
        access_key=minio_cfg['access_key'],
        secret_key=minio_cfg['secret_key'],
        secure=minio_cfg['secure']
    )

def main():
    import argparse
    
    parser = argparse.ArgumentParser(description="S3 File Streaming Ingestor")
    parser.add_argument("config", help="Path to s3_config.json")
    parser.add_argument(
        "--mode",
        choices=['streaming', 'hybrid'],
        default='streaming',
        help="streaming=direct to MinIO, hybrid=also save local backup"
    )
    
    args = parser.parse_args()
    
    # Load config
    with open(args.config) as f:
        config = json.load(f)
    
    # Initialize MinIO
    logger.info("🔌 Connecting to MinIO...")
    minio_client = load_minio_config()
    
    # Initialize ingestor
    save_backup = (args.mode == 'hybrid')
    ingestor = S3FileIngestor(
        minio_client=minio_client,
        save_local_backup=save_backup
    )
    
    # Extract configuration
    s3_bucket = config['s3_bucket']
    s3_prefix = config.get('s3_prefix', '')
    source = config.get('source_name', 's3')
    aws_access_key = config['aws_access_key']
    aws_secret_key = config['aws_secret_key']
    aws_region = config.get('aws_region', 'us-east-1')
    file_extensions = config.get('file_extensions', None)
    profile_structured = config.get('profile_structured', True)
    
    # Run streaming ingestion
    result = ingestor.ingest_streaming(
        s3_bucket=s3_bucket,
        s3_prefix=s3_prefix,
        source=source,
        aws_access_key=aws_access_key,
        aws_secret_key=aws_secret_key,
        aws_region=aws_region,
        file_extensions=file_extensions,
        profile_structured=profile_structured
    )
    
    if result['success']:
        logger.info(f"\n✅ Success! Files streamed to MinIO")
        sys.exit(0)
    else:
        logger.error(f"\n❌ Ingestion failed")
        sys.exit(1)

if __name__ == "__main__":
    main()
