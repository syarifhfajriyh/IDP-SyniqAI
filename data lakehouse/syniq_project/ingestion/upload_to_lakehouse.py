"""
Bridge Script: Bronze Layer → MinIO Data Lakehouse

Usage:
    python upload_to_lakehouse.py bronze_layer/customers/20260219_143022
"""
import os
import sys
import json
import pandas as pd
import pyarrow.parquet as pq
from pathlib import Path
from datetime import datetime

# Add syniq_project to path
sys.path.append(str(Path(__file__).parent.parent.parent / "data lakehouse" / "syniq_project"))

from ingestion.bronze_ingestor import BronzeIngestor
from catalog.catalog_manager import CatalogManager
from quality.profiler import DataProfiler
from lineage.lineage_tracker import LineageTracker
from minio import Minio
import yaml

def load_minio_config():
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

def upload_bronze_to_lakehouse(bronze_folder: str):
    """
    Upload bronze layer data to MinIO lakehouse
    
    Args:
        bronze_folder: Path like "bronze_layer/customers/20260219_143022"
    """
    bronze_path = Path(bronze_folder)
    
    if not bronze_path.exists():
        raise FileNotFoundError(f"Bronze folder not found: {bronze_folder}")
    
    # Load metadata
    metadata_file = bronze_path / "metadata.json"
    if not metadata_file.exists():
        raise FileNotFoundError(f"metadata.json not found in {bronze_folder}")
    
    with open(metadata_file) as f:
        metadata = json.load(f)
    
    # Load parquet data
    parquet_file = bronze_path / "data.parquet"
    if not parquet_file.exists():
        raise FileNotFoundError(f"data.parquet not found in {bronze_folder}")
    
    print(f"📂 Loading data from: {parquet_file}")
    df = pd.read_parquet(parquet_file)
    print(f"   Rows: {len(df):,}")
    print(f"   Columns: {len(df.columns)}")
    
    # Extract source and entity from metadata
    source = metadata.get("source_type", "unknown")
    entity = metadata.get("entity", "unknown_table")
    
    # Initialize MinIO client
    print("\n🔌 Connecting to MinIO...")
    minio_client = load_minio_config()
    
    # Initialize lakehouse components
    print("🏗️  Initializing lakehouse components...")
    ingestor = BronzeIngestor(minio_client, {})
    catalog = CatalogManager(minio_client)
    profiler = DataProfiler(minio_client)
    lineage = LineageTracker(minio_client)
    
    # Ingest to MinIO
    print(f"\n📤 Uploading to MinIO bronze bucket...")
    result = ingestor.ingest(
        df=df,
        source=source,
        entity=entity,
        metadata=metadata
    )
    
    print(f"✅ Upload complete!")
    print(f"   Location: {result.get('location', 'N/A')}")
    print(f"   Partitions: {result.get('partitions_written', 0)}")
    
    # Register in catalog
    print(f"\n📋 Registering in catalog...")
    schema = {col: str(dtype) for col, dtype in df.dtypes.items()}
    
    catalog_entry = catalog.register_table(
        source=source,
        entity=entity,
        schema=schema,
        location=result.get('location', f"bronze/{source}/{entity}"),
        row_count=len(df),
        partition_keys=["year", "month", "day"],
        tags={
            "layer": "bronze",
            "source_type": source,
            "ingestion_run_id": metadata.get("run_id", "unknown")
        }
    )
    
    print(f"✅ Catalog registered (v{catalog_entry['version']})")
    
    # Generate quality profile
    print(f"\n📊 Generating quality profile...")
    profile = profiler.profile_dataframe(df, source, entity)
    print(f"✅ Quality Score: {profile['quality_score']:.2f}%")
    
    # Record lineage
    print(f"\n🔗 Recording lineage...")
    lineage.record_transformation(
        source_layer="source",
        source_location=f"client_postgres/{entity}",
        target_layer="bronze",
        target_location=result.get('location', f"bronze/{source}/{entity}"),
        transformation="network_ingestion",
        row_count=len(df),
        columns_used=list(df.columns),
        metadata={
            "extraction_timestamp": metadata.get("extracted_at"),
            "original_bronze_folder": str(bronze_folder)
        }
    )
    
    print(f"✅ Lineage recorded")
    
    # Summary
    print("\n" + "="*60)
    print("🎉 LAKEHOUSE UPLOAD COMPLETE")
    print("="*60)
    print(f"Source: {source}")
    print(f"Entity: {entity}")
    print(f"Rows: {len(df):,}")
    print(f"Quality: {profile['quality_score']:.2f}%")
    print(f"Catalog Version: {catalog_entry['version']}")
    print(f"MinIO Location: {result.get('location')}")
    print("="*60)
    
    return {
        "source": source,
        "entity": entity,
        "rows": len(df),
        "quality_score": profile['quality_score'],
        "minio_location": result.get('location'),
        "catalog_version": catalog_entry['version']
    }

def main():
    if len(sys.argv) < 2:
        print("Usage: python upload_to_lakehouse.py <bronze_folder>")
        print("\nExample:")
        print("  python upload_to_lakehouse.py bronze_layer/customers/20260219_143022")
        sys.exit(1)
    
    bronze_folder = sys.argv[1]
    
    try:
        result = upload_bronze_to_lakehouse(bronze_folder)
        print(f"\n✅ Success! Data uploaded to MinIO")
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()