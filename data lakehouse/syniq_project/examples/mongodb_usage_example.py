# filepath: examples/mongodb_usage_example.py
"""
MongoDB Connector Usage Examples
=================================
Demonstrates various ways to use the MongoDB connector.
"""

import sys
import os

# Add project root to path
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

from utils.config_loader import load_config
from connectors import MongoDBConnector


def example_1_basic_extraction():
    """Example 1: Basic document extraction"""
    print("\n" + "="*60)
    print("EXAMPLE 1: Basic Document Extraction")
    print("="*60)
    
    # Load config
    config = load_config("config/db_config.yaml")
    
    # Initialize connector
    connector = MongoDBConnector(config["mongodb"])
    connector.connect()
    connector.validate_credentials()
    
    # Extract documents
    extraction_plan = {
        "entity": "customers",          # Collection name
        "mode": "full",                 # Full load
        "batch_size": 1000,            # Documents per batch
        "flatten_nested": False         # Keep nested structure
    }
    
    total_docs = 0
    for batch in connector.extract(extraction_plan):
        df = batch["data"]
        meta = batch["metadata"]
        
        print(f"\n📦 Batch {meta['chunk_index']}:")
        print(f"   Documents: {len(df)}")
        print(f"   Progress: {meta['progress_percent']}%")
        print(f"   Columns: {list(df.columns)}")
        
        total_docs += len(df)
    
    print(f"\n✅ Total documents extracted: {total_docs:,}")
    connector.close()


def example_2_flattened_documents():
    """Example 2: Extract with document flattening"""
    print("\n" + "="*60)
    print("EXAMPLE 2: Flattened Document Extraction")
    print("="*60)
    
    config = load_config("config/db_config.yaml")
    connector = MongoDBConnector(config["mongodb"])
    connector.connect()
    
    # Extract with flattening
    extraction_plan = {
        "entity": "customer_interactions",
        "mode": "full",
        "flatten_nested": True,         # Flatten nested documents
        "max_depth": 3,                 # Maximum nesting depth
        "batch_size": 500
    }
    
    for batch in connector.extract(extraction_plan):
        df = batch["data"]
        meta = batch["metadata"]
        
        print(f"\n📦 Batch {meta['chunk_index']}:")
        print(f"   Documents: {len(df)}")
        print(f"   Flattened: {meta['flattened']}")
        print(f"   Sample columns: {list(df.columns)[:10]}")
        
        # Show sample data
        if len(df) > 0:
            print(f"\n   Sample record:")
            print(df.iloc[0].to_dict())
        
        break  # Just show first batch
    
    connector.close()


def example_3_incremental_extraction():
    """Example 3: Incremental extraction (watermark-based)"""
    print("\n" + "="*60)
    print("EXAMPLE 3: Incremental Extraction")
    print("="*60)
    
    config = load_config("config/db_config.yaml")
    connector = MongoDBConnector(config["mongodb"])
    connector.connect()
    
    from datetime import datetime, timedelta
    
    # Extract only recent documents
    watermark_date = datetime.now() - timedelta(days=7)
    
    extraction_plan = {
        "entity": "policy_events",
        "mode": "incremental",
        "watermark_field": "event_date",    # Field to filter on
        "watermark_value": watermark_date,  # Starting value
        "batch_size": 1000
    }
    
    for batch in connector.extract(extraction_plan):
        df = batch["data"]
        meta = batch["metadata"]
        
        print(f"\n📦 Incremental batch:")
        print(f"   New documents: {len(df)}")
        print(f"   Date range: {watermark_date} to now")
    
    connector.close()


def example_4_binary_data_extraction():
    """Example 4: Extract documents with binary data (images, files)"""
    print("\n" + "="*60)
    print("EXAMPLE 4: Binary Data Extraction")
    print("="*60)
    
    config = load_config("config/db_config.yaml")
    connector = MongoDBConnector(config["mongodb"])
    connector.connect()
    
    # Extract with binary fields
    extraction_plan = {
        "entity": "images_metadata",
        "mode": "full",
        "extract_binary": True,         # Extract binary data
        "batch_size": 100               # Smaller batches for binary
    }
    
    for batch in connector.extract(extraction_plan):
        df = batch["data"]
        meta = batch["metadata"]
        
        print(f"\n📦 Batch {meta['chunk_index']}:")
        print(f"   Documents: {len(df)}")
        print(f"   Binary fields: {meta.get('binary_fields', [])}")
        
        # Check if binary data is present
        for field in meta.get('binary_fields', []):
            if field in df.columns:
                sample_binary = df[field].iloc[0] if len(df) > 0 else None
                if sample_binary:
                    print(f"   Binary '{field}': {len(sample_binary)} bytes")
        
        break  # Just show first batch
    
    connector.close()


def example_5_separate_blobs():
    """Example 5: Extract with separate blob collections"""
    print("\n" + "="*60)
    print("EXAMPLE 5: Metadata + Separate Blobs")
    print("="*60)
    
    config = load_config("config/db_config.yaml")
    connector = MongoDBConnector(config["mongodb"])
    connector.connect()
    
    # Extract metadata + blobs from separate collections
    for batch in connector.extract_with_blobs(
        metadata_collection="images_metadata",
        blob_collection="images_blobs",
        link_field="checksum",
        blob_field="content",
        batch_size=50
    ):
        df = batch["data"]
        meta = batch["metadata"]
        
        print(f"\n📦 Batch {meta['chunk_index']}:")
        print(f"   Metadata records: {len(df)}")
        print(f"   Blobs attached: {meta.get('blobs_attached', 0)}")
        
        # Check blob data
        if "content_data" in df.columns:
            print(f"   Blob column present: content_data")
            blobs_with_data = df["content_data"].notna().sum()
            print(f"   Records with blob data: {blobs_with_data}")
        
        break
    
    connector.close()


def example_6_collection_exploration():
    """Example 6: Explore database collections"""
    print("\n" + "="*60)
    print("EXAMPLE 6: Database Exploration")
    print("="*60)
    
    config = load_config("config/db_config.yaml")
    connector = MongoDBConnector(config["mongodb"])
    connector.connect()
    
    # List all collections
    collections = connector.list_collections()
    print(f"\n📚 Collections in database:")
    for i, coll in enumerate(collections, 1):
        print(f"   {i}. {coll}")
    
    # Get schema for a collection
    if collections:
        sample_collection = collections[0]
        print(f"\n🔍 Schema analysis for '{sample_collection}':")
        
        schema = connector.get_collection_schema(sample_collection, sample_size=100)
        
        for field_name, field_info in schema.items():
            print(f"\n   Field: {field_name}")
            print(f"      Type: {field_info['type']}")
            print(f"      Presence: {field_info['presence_percent']}%")
            print(f"      Example: {field_info['example']}")
    
    connector.close()


def example_7_filtered_extraction():
    """Example 7: Extract with MongoDB query filter"""
    print("\n" + "="*60)
    print("EXAMPLE 7: Filtered Extraction")
    print("="*60)
    
    config = load_config("config/db_config.yaml")
    connector = MongoDBConnector(config["mongodb"])
    connector.connect()
    
    # Extract with custom filter
    extraction_plan = {
        "entity": "policies",
        "mode": "full",
        "query_filter": {
            "status": "active",
            "premium": {"$gt": 1000}
        },
        "projection": {
            "policy_number": 1,
            "customer_id": 1,
            "premium": 1,
            "status": 1,
            "_id": 0
        },
        "sort_by": [("premium", -1)],  # Sort by premium descending
        "batch_size": 500
    }
    
    for batch in connector.extract(extraction_plan):
        df = batch["data"]
        print(f"\n📦 Filtered extraction:")
        print(f"   Active policies with premium > 1000: {len(df)}")
        print(f"   Columns: {list(df.columns)}")
        
        if len(df) > 0:
            print(f"\n   Top 5 by premium:")
            print(df.head())
        
        break
    
    connector.close()


if __name__ == "__main__":
    print("\n" + "🍃" * 30)
    print("MongoDB Connector Usage Examples")
    print("🍃" * 30)
    
    # Run examples (comment out as needed)
    try:
        example_1_basic_extraction()
        # example_2_flattened_documents()
        # example_3_incremental_extraction()
        # example_4_binary_data_extraction()
        # example_5_separate_blobs()
        # example_6_collection_exploration()
        # example_7_filtered_extraction()
        
    except FileNotFoundError:
        print("\n❌ Config file not found. Please ensure:")
        print("   1. config/db_config.yaml exists")
        print("   2. MongoDB configuration is set up")
        print("   3. config/.env has MongoDB credentials")
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
    
    print("\n" + "🍃" * 30)
    print("Examples complete!")
    print("🍃" * 30)
