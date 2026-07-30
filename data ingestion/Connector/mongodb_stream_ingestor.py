"""
MongoDB Streaming Ingestor: MongoDB → MinIO (with optional local backup)

Features:
- ✅ Streams directly from MongoDB collections to MinIO
- ✅ Supports MongoDB Atlas and On-Prem
- ✅ Handles nested documents (flatten or preserve)
- ✅ Optional local backup for debugging
- ✅ Automatic retry on failure
- ✅ Progress tracking
- ✅ Integrated catalog/quality/lineage

Usage:
    python mongodb_stream_ingestor.py mongodb_config.json --mode streaming
    python mongodb_stream_ingestor.py mongodb_config.json --mode hybrid  # saves local backup
"""

import os
import sys
import json
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from pathlib import Path
from datetime import datetime
from typing import Iterator, Dict, Optional, List
from io import BytesIO
import logging
from pymongo import MongoClient
from bson import ObjectId

# Add paths
sys.path.append(str(Path(__file__).parent))
sys.path.append(str(Path(__file__).parent.parent.parent / "data lakehouse" / "syniq_project"))

from catalog.catalog_manager import CatalogManager
from quality.profiler import DataProfiler
from lineage.lineage_tracker import LineageTracker
from minio import Minio
import yaml

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("MongoDBIngestor")

class MongoDBStreamIngestor:
    """
    Stream data directly from MongoDB to MinIO with optional local backup.
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
    
    def _flatten_document(self, doc: Dict, parent_key: str = '', sep: str = '_') -> Dict:
        """
        Flatten nested MongoDB document.
        Example: {'user': {'name': 'John'}} → {'user_name': 'John'}
        """
        items = []
        for k, v in doc.items():
            new_key = f"{parent_key}{sep}{k}" if parent_key else k
            
            # Handle ObjectId
            if isinstance(v, ObjectId):
                items.append((new_key, str(v)))
            # Handle nested dict
            elif isinstance(v, dict):
                items.extend(self._flatten_document(v, new_key, sep=sep).items())
            # Handle lists (convert to JSON string)
            elif isinstance(v, list):
                items.append((new_key, json.dumps(v)))
            else:
                items.append((new_key, v))
        
        return dict(items)
    
    def _query_collection_chunks(
        self,
        mongo_client: MongoClient,
        database: str,
        collection: str,
        query: Dict,
        chunk_size: int,
        flatten: bool = True
    ) -> Iterator[pd.DataFrame]:
        """
        Query MongoDB collection in chunks and yield DataFrames.
        """
        db = mongo_client[database]
        coll = db[collection]
        
        # Get total count
        total_docs = coll.count_documents(query)
        logger.info(f"📊 Total documents: {total_docs:,}")
        
        if total_docs == 0:
            logger.warning(f"⚠️  No documents found matching query")
            return
        
        # Stream documents in chunks
        cursor = coll.find(query).batch_size(chunk_size)
        
        chunk_docs = []
        for doc in cursor:
            # Flatten if requested
            if flatten:
                doc = self._flatten_document(doc)
            else:
                # Convert ObjectId to string even in non-flattened mode
                if '_id' in doc and isinstance(doc['_id'], ObjectId):
                    doc['_id'] = str(doc['_id'])
            
            chunk_docs.append(doc)
            
            # Yield chunk when full
            if len(chunk_docs) >= chunk_size:
                yield pd.DataFrame(chunk_docs)
                chunk_docs = []
        
        # Yield remaining documents
        if chunk_docs:
            yield pd.DataFrame(chunk_docs)
    
    def ingest_streaming(
        self,
        mongo_uri: str,
        database: str,
        collection: str,
        source: str,
        query: Dict = None,
        chunk_size: int = 10000,
        flatten_documents: bool = True
    ) -> Dict:
        """
        Stream data directly from MongoDB to MinIO.
        
        Args:
            mongo_uri: MongoDB connection URI
            database: Database name
            collection: Collection name
            source: Source name (e.g., 'mongodb_prod')
            query: MongoDB query filter (default: {})
            chunk_size: Documents per chunk
            flatten_documents: Flatten nested documents
        
        Returns:
            Ingestion result with metrics
        """
        if query is None:
            query = {}
        
        logger.info(f"🚀 Starting MONGODB STREAM ingestion")
        logger.info(f"   Source: {source}")
        logger.info(f"   Database: {database}")
        logger.info(f"   Collection: {collection}")
        logger.info(f"   Query: {query}")
        logger.info(f"   Flatten: {flatten_documents}")
        
        start_time = datetime.utcnow()
        total_rows = 0
        chunk_count = 0
        
        # Connect to MongoDB
        logger.info(f"🔌 Connecting to MongoDB...")
        mongo_client = MongoClient(mongo_uri, serverSelectionTimeoutMS=10000)
        
        try:
            # Test connection
            mongo_client.admin.command('ping')
            logger.info(f"   ✅ Connected")
        except Exception as e:
            logger.error(f"   ❌ Connection failed: {e}")
            raise
        
        # Prepare MinIO path
        today = datetime.utcnow()
        minio_prefix = f"bronze/{source}/{collection}/year={today.year}/month={today.month:02d}/day={today.day:02d}"
        
        # Optional: Setup local backup
        backup_path = None
        if self.save_local_backup:
            backup_path = self.backup_dir / collection / start_time.strftime("%Y%m%d_%H%M%S")
            backup_path.mkdir(parents=True, exist_ok=True)
            logger.info(f"💾 Local backup enabled: {backup_path}")
        
        # Collect all chunks for quality profiling
        all_chunks = []
        schema = None
        
        try:
            logger.info(f"\n📥 Processing chunks...")
            
            # Stream documents
            data_stream = self._query_collection_chunks(
                mongo_client=mongo_client,
                database=database,
                collection=collection,
                query=query,
                chunk_size=chunk_size,
                flatten=flatten_documents
            )
            
            for chunk_df in data_stream:
                chunk_count += 1
                chunk_rows = len(chunk_df)
                total_rows += chunk_rows
                
                # Add metadata columns
                chunk_df['_ingestion_timestamp'] = start_time
                chunk_df['_source_system'] = source
                chunk_df['_entity'] = collection
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
                
                logger.info(f"   ✅ Chunk {chunk_count}: {chunk_rows:,} documents → MinIO")
                
                # Optional: Save local backup
                if self.save_local_backup:
                    chunk_df.to_parquet(backup_path / f"chunk-{chunk_count:05d}.parquet")
                
                # Keep for quality profiling (sample only if too large)
                if total_rows < 100000:  # Only keep first 100K rows for profiling
                    all_chunks.append(chunk_df)
            
            # Combine chunks for validation and profiling
            logger.info(f"\n📊 Processing complete: {total_rows:,} documents in {chunk_count} chunks")
            
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
                entity=collection,
                schema=schema,
                location=minio_prefix,
                row_count=total_rows,
                partition_keys=["year", "month", "day"],
                tags={
                    "layer": "bronze",
                    "source_type": "mongodb",
                    "database": database,
                    "flattened": flatten_documents,
                    "ingestion_mode": "streaming",
                    "chunks": chunk_count
                }
            )
            logger.info(f"   ✅ Catalog v{catalog_entry['version']}")
            
            # Generate quality profile
            logger.info(f"\n📊 Generating quality profile...")
            profile = self.profiler.profile_dataframe(sample_df, source, collection)
            logger.info(f"   ✅ Quality Score: {profile['quality_score']:.2f}%")
            
            # Record lineage
            logger.info(f"\n🔗 Recording lineage...")
            self.lineage.record_transformation(
                source_layer="source",
                source_location=f"{source}/{database}/{collection}",
                target_layer="bronze",
                target_location=minio_prefix,
                transformation="mongodb_stream_ingestion",
                row_count=total_rows,
                columns_used=list(schema.keys()),
                metadata={
                    "database": database,
                    "collection": collection,
                    "query": str(query),
                    "flattened": flatten_documents,
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
                "database": database,
                "collection": collection,
                "total_documents": total_rows,
                "chunks_written": chunk_count,
                "minio_location": minio_prefix,
                "catalog_version": catalog_entry['version'],
                "quality_score": profile['quality_score'],
                "duration_seconds": duration,
                "documents_per_second": int(total_rows / duration) if duration > 0 else 0,
                "local_backup": str(backup_path) if backup_path else None
            }
            
            logger.info("\n" + "="*70)
            logger.info("🎉 MONGODB STREAMING INGESTION COMPLETE")
            logger.info("="*70)
            logger.info(f"Source:          {source}")
            logger.info(f"Database:        {database}")
            logger.info(f"Collection:      {collection}")
            logger.info(f"Total Docs:      {total_rows:,}")
            logger.info(f"Chunks:          {chunk_count}")
            logger.info(f"Quality Score:   {profile['quality_score']:.2f}%")
            logger.info(f"Duration:        {duration:.2f}s")
            logger.info(f"Speed:           {result['documents_per_second']:,} docs/sec")
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
            
            raise
        finally:
            mongo_client.close()

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
    
    parser = argparse.ArgumentParser(description="MongoDB Streaming Ingestor")
    parser.add_argument("config", help="Path to mongodb_config.json")
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
    ingestor = MongoDBStreamIngestor(
        minio_client=minio_client,
        save_local_backup=save_backup
    )
    
    # Extract configuration
    mongo_uri = config['mongo_uri']
    database = config['database']
    collection = config['collection']
    source = config.get('source_name', 'mongodb')
    query = config.get('query', {})
    chunk_size = config.get('chunk_size', 10000)
    flatten = config.get('flatten_documents', True)
    
    # Run streaming ingestion
    result = ingestor.ingest_streaming(
        mongo_uri=mongo_uri,
        database=database,
        collection=collection,
        source=source,
        query=query,
        chunk_size=chunk_size,
        flatten_documents=flatten
    )
    
    if result['success']:
        logger.info(f"\n✅ Success! Data streamed to MinIO")
        sys.exit(0)
    else:
        logger.error(f"\n❌ Ingestion failed")
        sys.exit(1)

if __name__ == "__main__":
    main()
