# filepath: connectors/mongodb_connector.py
"""
MongoDB Connector - Optimized for Document & Binary Data
=========================================================
Features:
- Supports MongoDB Atlas (mongodb+srv://) and On-Premise (mongodb://)
- Binary data handling (images, videos, files)
- Document flattening for nested structures
- Batch extraction with configurable batch size
- GridFS support for large files
- Collection-aware extraction strategies

Usage:
    from utils.config_loader import load_config
    from connectors import MongoDBConnector
    
    config = load_config("config/db_config.yaml")
    connector = MongoDBConnector(config["mongodb"])
    connector.connect()
    
    # Extract documents
    for batch in connector.extract({
        "entity": "customers", 
        "mode": "full",
        "flatten_nested": True
    }):
        df = batch["data"]
        print(f"Got {len(df)} documents")
"""

import pandas as pd
import json
import logging
from datetime import datetime, timezone
from typing import Dict, Any, Iterator, List, Optional
from bson import ObjectId
from bson.binary import Binary
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure, OperationFailure

from .base_connector import BaseConnector

logger = logging.getLogger(__name__)


def timed(func):
    """Decorator to measure function execution time"""
    import time
    def wrapper(*args, **kwargs):
        start = time.time()
        result = func(*args, **kwargs)
        duration = time.time() - start
        logger.info(f"⏱️  {func.__name__} took {duration:.4f}s")
        return result
    return wrapper


# ==========================================
# MONGODB CONNECTOR
# ==========================================
class MongoDBConnector(BaseConnector):
    """
    MongoDB Connector - Document Database with Binary Support
    
    Features:
    ✅ Atlas (mongodb+srv://) and On-Prem (mongodb://) support
    ✅ Binary data extraction (images, videos, files)
    ✅ Document flattening (configurable depth)
    ✅ Batch processing with configurable size
    ✅ GridFS support for large files
    ✅ BSON type handling (ObjectId, Binary, DateTime)
    """
    
    source_type = "mongodb"

    def __init__(self, connection_config: Dict[str, Any]):
        super().__init__(connection_config)
        self.client = None
        self.db = None

    @timed
    def connect(self) -> None:
        """
        Establish connection to MongoDB.
        
        Supports two connection modes:
        1. URI-based (Atlas or custom connection string)
        2. Individual parameters (On-Prem)
        """
        cfg = self.connection_config
        
        # Mode 1: URI-based connection (Atlas or custom)
        if cfg.get("uri"):
            connection_string = cfg["uri"]
            database_name = cfg.get("database")
            
            logger.info(f"🍃 Connecting to MongoDB using URI...")
            
            # Mask credentials for logging
            masked_uri = self._mask_uri(connection_string)
            logger.info(f"   URI: {masked_uri}")
            
        # Mode 2: Individual parameters (On-Prem)
        else:
            host = cfg.get("host", "localhost")
            port = cfg.get("port", 27017)
            user = cfg.get("user")
            password = cfg.get("password")
            auth_source = cfg.get("auth_source", "admin")
            database_name = cfg.get("database")
            
            logger.info(f"🍃 Connecting to MongoDB at {host}:{port}")
            
            # Build connection string
            if user and password:
                connection_string = (
                    f"mongodb://{user}:{password}@{host}:{port}/"
                    f"?authSource={auth_source}"
                )
                logger.info(f"   Using authentication (user: {user})")
            else:
                connection_string = f"mongodb://{host}:{port}/"
                logger.info("   Using unauthenticated connection")
        
        # Connection options
        connect_timeout_ms = cfg.get("connect_timeout_ms", 10000)
        server_selection_timeout_ms = cfg.get("server_selection_timeout_ms", 5000)
        max_pool_size = cfg.get("max_pool_size", 10)
        min_pool_size = cfg.get("min_pool_size", 1)
        
        try:
            # Create client
            self.client = MongoClient(
                connection_string,
                connectTimeoutMS=connect_timeout_ms,
                serverSelectionTimeoutMS=server_selection_timeout_ms,
                maxPoolSize=max_pool_size,
                minPoolSize=min_pool_size,
                retryWrites=cfg.get("retry_writes", True),
                w=cfg.get("w", "majority")
            )
            
            # Test connection
            self.client.admin.command('ping')
            
            # Get server info
            server_info = self.client.server_info()
            version = server_info.get('version', 'Unknown')
            
            logger.info(f"✅ Connected to MongoDB {version}")
            
            # Connect to database
            if not database_name:
                raise ValueError("Database name is required in configuration")
            
            self.db = self.client[database_name]
            logger.info(f"✅ Using database: {database_name}")
            
            # List collections
            collections = self.db.list_collection_names()
            logger.info(f"   Found {len(collections)} collections")
            if collections:
                logger.info(f"   Collections: {', '.join(collections[:5])}" + 
                          (" ..." if len(collections) > 5 else ""))
            
        except ConnectionFailure as e:
            logger.error(f"❌ Connection failed: {e}")
            raise ConnectionError(f"Failed to connect to MongoDB: {e}")
        except Exception as e:
            logger.error(f"❌ Unexpected error: {e}")
            raise

    def validate_credentials(self) -> None:
        """Validate MongoDB connection and permissions"""
        try:
            # Test database access
            db_name = self.db.name
            collections = self.db.list_collection_names()
            logger.info(f"✅ Database access verified: {db_name}")
            logger.info(f"   Can list {len(collections)} collections")
            
            # Test read permission (try to count documents in first collection)
            if collections:
                test_collection = collections[0]
                count = self.db[test_collection].count_documents({})
                logger.info(f"✅ Read permission verified ({test_collection}: {count:,} docs)")
            
            # Test write permission (try to write to test collection)
            try:
                test_collection = self.db['_connection_test']
                result = test_collection.insert_one({
                    'test': True, 
                    'timestamp': datetime.now(timezone.utc)
                })
                test_collection.delete_one({'_id': result.inserted_id})
                logger.info(f"✅ Write permission verified")
            except OperationFailure:
                logger.warning("⚠️  Write permission not available (read-only access)")
            
        except Exception as e:
            logger.error(f"❌ Credential validation failed: {e}")
            raise PermissionError(f"MongoDB validation failed: {e}")

    def close(self) -> None:
        """Close MongoDB connection"""
        if self.client:
            self.client.close()
            logger.info("🔌 MongoDB connection closed")

    # ==========================================
    # HELPER METHODS
    # ==========================================
    def _mask_uri(self, uri: str) -> str:
        """Mask credentials in URI for safe logging"""
        if '@' in uri:
            parts = uri.split('@')
            cred_part = parts[0].split('//')[-1]
            if ':' in cred_part:
                user = cred_part.split(':')[0]
                uri = uri.replace(cred_part, f"{user}:***")
        return uri

    def _safe_json_value(self, value: Any) -> Any:
        """
        Convert BSON types to JSON-serializable formats.
        
        Handles:
        - ObjectId → string
        - DateTime → ISO8601 string
        - Binary → base64 string or bytes
        - Others → as-is
        """
        if isinstance(value, ObjectId):
            return str(value)
        elif isinstance(value, datetime):
            return value.replace(tzinfo=timezone.utc).isoformat().replace("+00:00", "Z")
        elif isinstance(value, Binary):
            # Return as bytes for binary data
            return bytes(value)
        elif isinstance(value, (dict, list)):
            # Recursively handle nested structures
            if isinstance(value, dict):
                return {k: self._safe_json_value(v) for k, v in value.items()}
            else:
                return [self._safe_json_value(v) for v in value]
        return value

    def _flatten_document(
        self, 
        doc: Dict[str, Any], 
        parent_key: str = '', 
        sep: str = '_',
        max_depth: int = 3,
        current_depth: int = 0
    ) -> Dict[str, Any]:
        """
        Flatten nested MongoDB documents.
        
        Args:
            doc: Document to flatten
            parent_key: Parent key for nested fields
            sep: Separator for flattened keys
            max_depth: Maximum nesting depth to flatten
            current_depth: Current recursion depth
        
        Returns:
            Flattened dictionary
        
        Example:
            Input:  {"user": {"name": "John", "age": 30}}
            Output: {"user_name": "John", "user_age": 30}
        """
        items = []
        
        for k, v in doc.items():
            new_key = f"{parent_key}{sep}{k}" if parent_key else k
            
            # Stop flattening at max depth
            if current_depth >= max_depth:
                items.append((new_key, self._safe_json_value(v)))
                continue
            
            # Flatten nested dicts
            if isinstance(v, dict) and not isinstance(v, Binary):
                items.extend(
                    self._flatten_document(
                        v, new_key, sep, max_depth, current_depth + 1
                    ).items()
                )
            # Convert lists to JSON strings (avoid DataFrame issues)
            elif isinstance(v, list):
                items.append((new_key, json.dumps(v, default=str)))
            else:
                items.append((new_key, self._safe_json_value(v)))
        
        return dict(items)

    def _get_collection_stats(self, collection_name: str) -> Dict[str, Any]:
        """Get collection statistics"""
        collection = self.db[collection_name]
        
        try:
            stats = self.db.command("collStats", collection_name)
            doc_count = stats.get("count", 0)
            avg_obj_size = stats.get("avgObjSize", 0)
            storage_size = stats.get("storageSize", 0)
            
            return {
                "document_count": doc_count,
                "avg_document_size_bytes": avg_obj_size,
                "storage_size_bytes": storage_size,
                "storage_size_mb": round(storage_size / (1024**2), 2)
            }
        except Exception as e:
            logger.warning(f"⚠️  Could not get collection stats: {e}")
            # Fallback to simple count
            return {
                "document_count": collection.count_documents({}),
                "avg_document_size_bytes": 1024,
                "storage_size_bytes": 0,
                "storage_size_mb": 0
            }

    def _detect_binary_fields(self, collection_name: str) -> List[str]:
        """
        Detect fields containing binary data by sampling documents.
        
        Returns:
            List of field names containing Binary data
        """
        collection = self.db[collection_name]
        
        # Sample first document
        sample_doc = collection.find_one()
        if not sample_doc:
            return []
        
        binary_fields = []
        
        def check_binary(obj, prefix=''):
            if isinstance(obj, dict):
                for key, value in obj.items():
                    field_name = f"{prefix}.{key}" if prefix else key
                    if isinstance(value, Binary):
                        binary_fields.append(field_name)
                    elif isinstance(value, dict):
                        check_binary(value, field_name)
        
        check_binary(sample_doc)
        
        if binary_fields:
            logger.info(f"   📎 Detected binary fields: {', '.join(binary_fields)}")
        
        return binary_fields

    # ==========================================
    # MAIN EXTRACT METHOD
    # ==========================================
    @timed
    def extract(self, extraction_plan: Dict[str, Any]) -> Iterator[Dict[str, Any]]:
        """
        Extract documents from MongoDB collection.
        
        Args:
            extraction_plan:
                Required:
                - entity: Collection name
                - mode: "full" or "incremental"
                
                Optional:
                - batch_size: Documents per batch (default: 1000)
                - flatten_nested: Flatten nested documents (default: False)
                - max_depth: Maximum flattening depth (default: 3)
                - query_filter: MongoDB query filter dict (default: {})
                - projection: Fields to include/exclude (default: None)
                - sort_by: Sort field (default: None)
                - watermark_field: Field for incremental extraction
                - watermark_value: Starting value for incremental
                - extract_binary: Whether to extract binary data (default: True)
        
        Yields:
            Dict with "data" (DataFrame) and "metadata"
        """
        entity = extraction_plan["entity"]
        batch_size = extraction_plan.get("batch_size", 
                                        self.connection_config.get("batch_size", 1000))
        flatten_nested = extraction_plan.get("flatten_nested", False)
        max_depth = extraction_plan.get("max_depth", 3)
        extract_binary = extraction_plan.get("extract_binary", True)
        
        # Get collection
        collection = self.db[entity]
        
        # Get collection stats
        stats = self._get_collection_stats(entity)
        logger.info(f"📊 Collection '{entity}' stats:")
        logger.info(f"   Documents: {stats['document_count']:,}")
        logger.info(f"   Avg size: {stats['avg_document_size_bytes']:,} bytes")
        logger.info(f"   Storage: {stats['storage_size_mb']} MB")
        
        # Detect binary fields
        binary_fields = self._detect_binary_fields(entity) if extract_binary else []
        
        # Build query filter
        query_filter = extraction_plan.get("query_filter", {})
        
        # Handle incremental mode
        if extraction_plan.get("mode") == "incremental":
            watermark_field = extraction_plan.get("watermark_field")
            watermark_value = extraction_plan.get("watermark_value")
            
            if not watermark_field:
                raise ValueError("Incremental mode requires 'watermark_field'")
            
            if watermark_value:
                query_filter[watermark_field] = {"$gt": watermark_value}
                logger.info(f"🔄 Incremental mode: {watermark_field} > {watermark_value}")
        
        # Projection (field selection)
        projection = extraction_plan.get("projection")
        
        # Sort
        sort_by = extraction_plan.get("sort_by")
        
        # Get total count for this query
        total_docs = collection.count_documents(query_filter)
        logger.info(f"📦 Extracting {total_docs:,} documents (batch size: {batch_size:,})")
        
        if total_docs == 0:
            logger.warning(f"⚠️  No documents found matching filter")
            return
        
        # Optimize batch size for binary data
        if binary_fields and stats['avg_document_size_bytes'] > 100_000:  # >100KB avg
            optimized_batch = max(10, min(batch_size, 100))
            if optimized_batch != batch_size:
                logger.info(f"⚡ Optimized batch size for binary data: {batch_size} → {optimized_batch}")
                batch_size = optimized_batch
        
        # Build cursor
        cursor = collection.find(query_filter, projection)
        
        if sort_by:
            cursor = cursor.sort(sort_by)
        
        # Extract in batches
        chunk_index = 0
        documents_processed = 0
        
        while True:
            # Fetch batch
            batch_docs = list(cursor.skip(chunk_index * batch_size).limit(batch_size))
            
            if not batch_docs:
                break
            
            # Process batch
            if flatten_nested:
                processed_docs = [
                    self._flatten_document(doc, max_depth=max_depth) 
                    for doc in batch_docs
                ]
            else:
                processed_docs = [
                    {k: self._safe_json_value(v) for k, v in doc.items()}
                    for doc in batch_docs
                ]
            
            # Convert to DataFrame
            try:
                chunk_df = pd.DataFrame(processed_docs)
            except Exception as e:
                logger.error(f"❌ Error converting to DataFrame: {e}")
                # Try with orient='records' as fallback
                chunk_df = pd.DataFrame.from_records(processed_docs)
            
            # Handle binary columns
            if extract_binary and binary_fields:
                for field in binary_fields:
                    if field in chunk_df.columns:
                        # Keep as bytes for now (will be handled by Bronze writer)
                        chunk_df[field] = chunk_df[field].apply(
                            lambda x: bytes(x) if isinstance(x, (Binary, bytes, bytearray)) else x
                        )
            
            documents_processed += len(chunk_df)
            
            # Yield batch
            yield {
                "data": chunk_df,
                "metadata": {
                    "source_type": self.source_type,
                    "entity": entity,
                    "chunk_index": chunk_index,
                    "row_count": len(chunk_df),
                    "documents_processed": documents_processed,
                    "total_documents": total_docs,
                    "progress_percent": round((documents_processed / total_docs) * 100, 2),
                    "flattened": flatten_nested,
                    "binary_fields": binary_fields,
                    "collection_stats": stats,
                    "extracted_at": datetime.now(timezone.utc).isoformat()
                }
            }
            
            chunk_index += 1
            
            # Log progress
            if chunk_index % 10 == 0:
                logger.info(f"   Progress: {documents_processed:,} / {total_docs:,} documents")
        
        logger.info(f"✅ Extraction complete: {documents_processed:,} documents in {chunk_index} batches")

    # ==========================================
    # SPECIALIZED EXTRACTION METHODS
    # ==========================================
    def extract_with_blobs(
        self, 
        metadata_collection: str,
        blob_collection: str,
        link_field: str = "checksum",
        blob_field: str = "content",
        **kwargs
    ) -> Iterator[Dict[str, Any]]:
        """
        Extract documents with separate blob storage.
        
        Common pattern: metadata collection + blob collection linked by checksum.
        
        Args:
            metadata_collection: Collection with document metadata
            blob_collection: Collection with binary blobs
            link_field: Field linking metadata to blobs (e.g., "checksum")
            blob_field: Field containing binary data in blob collection
            **kwargs: Additional extraction_plan parameters
        
        Yields:
            Documents with blobs embedded
        
        Example:
            # MongoDB structure:
            # images_metadata: {_id, file_name, checksum, size_bytes}
            # images_blobs: {checksum, content: Binary(...)}
            
            for batch in connector.extract_with_blobs(
                metadata_collection="images_metadata",
                blob_collection="images_blobs",
                link_field="checksum",
                blob_field="content"
            ):
                df = batch["data"]
                # df contains metadata + embedded blob data
        """
        logger.info(f"📦 Extracting with separate blobs:")
        logger.info(f"   Metadata: {metadata_collection}")
        logger.info(f"   Blobs: {blob_collection}")
        logger.info(f"   Link field: {link_field}")
        
        # Extract metadata
        extraction_plan = {
            "entity": metadata_collection,
            "mode": kwargs.get("mode", "full"),
            **kwargs
        }
        
        blob_col = self.db[blob_collection]
        
        for batch in self.extract(extraction_plan):
            df = batch["data"]
            meta = batch["metadata"]
            
            # Fetch blobs for this batch
            if link_field in df.columns:
                # Get unique link values
                link_values = df[link_field].dropna().unique().tolist()
                
                # Fetch blobs
                blob_docs = list(blob_col.find({link_field: {"$in": link_values}}))
                blob_map = {
                    doc[link_field]: self._safe_json_value(doc.get(blob_field))
                    for doc in blob_docs if link_field in doc
                }
                
                # Add blob column
                df[f"{blob_field}_data"] = df[link_field].map(blob_map)
                
                logger.info(f"   📎 Attached {len(blob_map)} blobs to batch")
            
            yield {
                "data": df,
                "metadata": {
                    **meta,
                    "blob_collection": blob_collection,
                    "blob_field": blob_field,
                    "blobs_attached": len(blob_map) if link_field in df.columns else 0
                }
            }

    def list_collections(self) -> List[str]:
        """List all collections in database"""
        return self.db.list_collection_names()

    def get_collection_schema(self, collection_name: str, sample_size: int = 100) -> Dict[str, Any]:
        """
        Infer schema from collection by sampling documents.
        
        Args:
            collection_name: Collection to analyze
            sample_size: Number of documents to sample
        
        Returns:
            Dict with field names and types
        """
        collection = self.db[collection_name]
        
        # Sample documents
        sample_docs = list(collection.aggregate([
            {"$sample": {"size": sample_size}}
        ]))
        
        if not sample_docs:
            return {}
        
        # Infer schema
        schema = {}
        
        for doc in sample_docs:
            for key, value in doc.items():
                if key not in schema:
                    schema[key] = {
                        "type": type(value).__name__,
                        "count": 1,
                        "example": str(value)[:100]
                    }
                else:
                    schema[key]["count"] += 1
        
        # Add presence percentage
        for field_info in schema.values():
            field_info["presence_percent"] = round(
                (field_info["count"] / len(sample_docs)) * 100, 2
            )
        
        return schema
