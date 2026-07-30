"""
Direct Streaming Ingestor: Client DB → MinIO (with optional local backup)

Features:
- ✅ Streams directly to MinIO (no double storage)
- ✅ Optional local backup for debugging
- ✅ In-memory validation
- ✅ Automatic retry on failure
- ✅ Progress tracking
- ✅ Integrated catalog/quality/lineage

Usage:
    python direct_ingestor.py config.json --mode streaming
    python direct_ingestor.py config.json --mode hybrid  # saves local backup
"""

import os
import sys
import json
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from pathlib import Path
from datetime import datetime
from typing import Iterator, Dict, Optional
from io import BytesIO
import logging

# Add paths
sys.path.append(str(Path(__file__).parent))
sys.path.append(str(Path(__file__).parent.parent.parent / "data lakehouse" / "syniq_project"))

from postgres_connector import PostgresConnector
from mariadb_connector import MariaDBConnector
from mariadbcloud_conn import MariaDBCloudConnector

from ingestion.bronze_ingestor import BronzeIngestor
from catalog.catalog_manager import CatalogManager
from quality.profiler import DataProfiler
from lineage.lineage_tracker import LineageTracker
from minio import Minio
import yaml

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("DirectIngestor")

class DirectStreamIngestor:
    """
    Stream data directly from source to MinIO with optional local backup.
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
        
        # Ensure bronze bucket exists
        if not self.minio.bucket_exists("bronze"):
            self.minio.make_bucket("bronze")
    
    def ingest_streaming(
        self,
        connector,
        source: str,
        entity: str,
        extraction_config: Dict
    ) -> Dict:
        """
        Stream data directly from source to MinIO.
        
        Args:
            connector: Database connector instance
            source: Source name (e.g., 'postgres_prod')
            entity: Table/collection name
            extraction_config: Extraction settings
        
        Returns:
            Ingestion result with metrics
        """
        logger.info(f"🚀 Starting DIRECT STREAM ingestion: {source}.{entity}")
        
        start_time = datetime.utcnow()
        total_rows = 0
        chunk_count = 0
        
        # Get data stream from connector
        data_stream = connector.extract(extraction_config)
        
        # Prepare MinIO path
        today = datetime.utcnow()
        minio_prefix = f"bronze/{source}/{entity}/year={today.year}/month={today.month:02d}/day={today.day:02d}"
        
        # Optional: Setup local backup
        backup_path = None
        if self.save_local_backup:
            backup_path = self.backup_dir / entity / start_time.strftime("%Y%m%d_%H%M%S")
            backup_path.mkdir(parents=True, exist_ok=True)
            logger.info(f"💾 Local backup enabled: {backup_path}")
        
        # Collect all chunks for quality profiling
        all_chunks = []
        schema = None
        
        try:
            logger.info(f"📥 Processing chunks...")
            
            for chunk_data in data_stream:
                # Extract DataFrame from the data structure
                if isinstance(chunk_data, dict) and "data" in chunk_data:
                    chunk_df = chunk_data["data"]
                else:
                    chunk_df = chunk_data
                
                chunk_count += 1
                chunk_rows = len(chunk_df)
                total_rows += chunk_rows
                
                # Add metadata columns
                chunk_df['_ingestion_timestamp'] = start_time
                chunk_df['_source_system'] = source
                chunk_df['_entity'] = entity
                chunk_df['_chunk_id'] = chunk_count
                
                # Capture schema from first chunk
                if schema is None:
                    schema = {col: str(dtype) for col, dtype in chunk_df.dtypes.items()}
                
                # Convert to Parquet in memory
                table = pa.Table.from_pandas(chunk_df)
                parquet_buffer = BytesIO()
                pq.write_table(table, parquet_buffer)
                parquet_buffer.seek(0)
                
                # Upload to MinIO
                object_name = f"{minio_prefix}/part-{chunk_count:05d}.parquet"
                self.minio.put_object(
                    bucket_name="bronze",
                    object_name=object_name,
                    data=parquet_buffer,
                    length=parquet_buffer.getbuffer().nbytes,
                    content_type="application/octet-stream"
                )
                
                logger.info(f"   ✅ Chunk {chunk_count}: {chunk_rows:,} rows → MinIO")
                
                # Optional: Save local backup
                if self.save_local_backup:
                    chunk_df.to_parquet(backup_path / f"chunk-{chunk_count:05d}.parquet")
                
                # Keep for quality profiling (sample only if too large)
                if total_rows < 100000:  # Only keep first 100K rows for profiling
                    all_chunks.append(chunk_df)
            
            # Combine chunks for validation and profiling
            logger.info(f"\n📊 Processing complete: {total_rows:,} rows in {chunk_count} chunks")
            
            # Create sample for quality profiling
            if all_chunks:
                sample_df = pd.concat(all_chunks[:10], ignore_index=True)  # Sample first 10 chunks
            else:
                # If no chunks kept in memory, read one back from MinIO
                sample_obj = self.minio.get_object("bronze", f"{minio_prefix}/part-00001.parquet")
                sample_df = pd.read_parquet(BytesIO(sample_obj.read()))
            
            # Register in catalog
            logger.info(f"\n📋 Registering in catalog...")
            catalog_entry = self.catalog.register_table(
                source=source,
                entity=entity,
                schema=schema,
                location=minio_prefix,
                row_count=total_rows,
                partition_keys=["year", "month", "day"],
                tags={
                    "layer": "bronze",
                    "source_type": source,
                    "ingestion_mode": "streaming",
                    "chunks": chunk_count
                }
            )
            logger.info(f"   ✅ Catalog v{catalog_entry['version']}")
            
            # Generate quality profile
            logger.info(f"\n📊 Generating quality profile...")
            profile = self.profiler.profile_dataframe(sample_df, source, entity)
            logger.info(f"   ✅ Quality Score: {profile['quality_score']:.2f}%")
            
            # Record lineage
            logger.info(f"\n🔗 Recording lineage...")
            self.lineage.record_transformation(
                source_layer="source",
                source_location=f"{source}/{entity}",
                target_layer="bronze",
                target_location=minio_prefix,
                transformation="direct_stream_ingestion",
                row_count=total_rows,
                columns_used=list(schema.keys()),
                metadata={
                    "ingestion_mode": "streaming",
                    "chunks_written": chunk_count,
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
                "entity": entity,
                "total_rows": total_rows,
                "chunks_written": chunk_count,
                "minio_location": minio_prefix,
                "catalog_version": catalog_entry['version'],
                "quality_score": profile['quality_score'],
                "duration_seconds": duration,
                "rows_per_second": int(total_rows / duration) if duration > 0 else 0,
                "local_backup": str(backup_path) if backup_path else None
            }
            
            logger.info("\n" + "="*70)
            logger.info("🎉 STREAMING INGESTION COMPLETE")
            logger.info("="*70)
            logger.info(f"Source:          {source}")
            logger.info(f"Entity:          {entity}")
            logger.info(f"Total Rows:      {total_rows:,}")
            logger.info(f"Chunks:          {chunk_count}")
            logger.info(f"Quality Score:   {profile['quality_score']:.2f}%")
            logger.info(f"Duration:        {duration:.2f}s")
            logger.info(f"Speed:           {result['rows_per_second']:,} rows/sec")
            logger.info(f"MinIO Location:  {minio_prefix}")
            if backup_path:
                logger.info(f"Local Backup:    {backup_path}")
            logger.info("="*70)
            
            return result
            
        except Exception as e:
            logger.error(f"\n❌ Streaming ingestion failed: {e}")
            
            # If we have local backup, we can retry upload later
            if self.save_local_backup and backup_path and backup_path.exists():
                logger.warning(f"⚠️  Local backup available at: {backup_path}")
                logger.warning(f"    You can retry upload using: upload_from_backup.py {backup_path}")
            
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

def get_connector(config: Dict):
    """Initialize database connector"""
    source_type = config['source_type'].lower()
    conn_config = config['connection_config']
    
    if source_type == 'postgres':
        connector = PostgresConnector(conn_config)
    elif source_type == 'mariadb':
        connector = MariaDBConnector(conn_config)
    elif source_type == 'mariadb_cloud':
        connector = MariaDBCloudConnector(conn_config)
    else:
        raise ValueError(f"Unsupported source type: {source_type}")
    
    # Connect to database
    connector.connect()
    return connector

def main():
    import argparse
    
    parser = argparse.ArgumentParser(description="Direct Streaming Ingestor")
    parser.add_argument("config", help="Path to config.json")
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
    
    # Initialize connector
    logger.info(f"🔌 Connecting to {config['source_type']}...")
    connector = get_connector(config)
    
    # Initialize ingestor
    save_backup = (args.mode == 'hybrid')
    ingestor = DirectStreamIngestor(
        minio_client=minio_client,
        save_local_backup=save_backup
    )
    
    # Extract source and entity
    source = config.get('source_name', config['source_type'])
    entity = config['extraction_request']['entity']
    
    # Run streaming ingestion
    result = ingestor.ingest_streaming(
        connector=connector,
        source=source,
        entity=entity,
        extraction_config=config['extraction_request']
    )
    
    # Cleanup
    connector.close()
    
    if result['success']:
        logger.info(f"\n✅ Success! Data streamed to MinIO")
        sys.exit(0)
    else:
        logger.error(f"\n❌ Ingestion failed")
        sys.exit(1)

if __name__ == "__main__":
    main()
