"""
MinIO/S3 Storage Manager for Bronze, Silver, Gold, and Quarantine layers
"""
from minio import Minio
from minio.error import S3Error
from typing import List, Dict, Any, Optional, BinaryIO
import io
import pandas as pd
import pyarrow.parquet as pq
import json
import logging
from datetime import datetime, timedelta
from pathlib import Path
from dotenv import load_dotenv
import os

# Load environment variables
env_path = Path(__file__).parent.parent.parent / ".env"
load_dotenv(env_path)

# Import config from same directory
from app_config import MinIOConfig

# Get config
minio_config = MinIOConfig.from_env()

logger = logging.getLogger(__name__)


class StorageManager:
    """MinIO/S3 object storage manager"""
    
    def __init__(self):
        self.client: Optional[Minio] = None
        self._initialized = False
    
    def initialize(self):
        """Initialize MinIO client"""
        if self._initialized:
            return
        
        try:
            self.client = Minio(
                minio_config.endpoint,
                access_key=minio_config.access_key,
                secret_key=minio_config.secret_key,
                secure=minio_config.secure
            )
            self._initialized = True
            logger.info(f"MinIO client initialized: {minio_config.endpoint}")
            
            # Ensure buckets exist
            self._ensure_buckets()
        except Exception as e:
            logger.error(f"Failed to initialize MinIO client: {e}")
            raise
    
    def _ensure_buckets(self):
        """Create buckets if they don't exist"""
        buckets = [
            minio_config.bronze_bucket,
            minio_config.silver_bucket,
            minio_config.gold_bucket,
            minio_config.quarantine_bucket
        ]
        
        for bucket in buckets:
            try:
                if not self.client.bucket_exists(bucket):
                    self.client.make_bucket(bucket)
                    logger.info(f"Created bucket: {bucket}")
            except S3Error as e:
                logger.error(f"Error creating bucket {bucket}: {e}")
    
    def list_objects(self, layer: str, domain: Optional[str] = None, prefix: str = "") -> List[Dict[str, Any]]:
        """List objects in a specific layer/bucket"""
        if not self._initialized:
            self.initialize()
        
        bucket = minio_config.get_bucket_for_layer(layer)
        
        # Build prefix with domain if specified
        full_prefix = f"{domain}/{prefix}" if domain else prefix
        
        objects = []
        try:
            for obj in self.client.list_objects(bucket, prefix=full_prefix, recursive=True):
                objects.append({
                    "object_name": obj.object_name,
                    "size": obj.size,
                    "last_modified": obj.last_modified,
                    "etag": obj.etag,
                    "content_type": obj.content_type
                })
        except S3Error as e:
            logger.error(f"Error listing objects in {bucket}/{full_prefix}: {e}")
        
        return objects
    
    def get_object(self, layer: str, object_name: str) -> Optional[bytes]:
        """Get object data as bytes"""
        if not self._initialized:
            self.initialize()
        
        bucket = minio_config.get_bucket_for_layer(layer)
        
        try:
            response = self.client.get_object(bucket, object_name)
            data = response.read()
            response.close()
            response.release_conn()
            return data
        except S3Error as e:
            logger.error(f"Error getting object {bucket}/{object_name}: {e}")
            return None
    
    def put_object(self, layer: str, object_name: str, data: BinaryIO, length: int, content_type: str = "application/octet-stream") -> bool:
        """Upload object to storage"""
        if not self._initialized:
            self.initialize()
        
        bucket = minio_config.get_bucket_for_layer(layer)
        
        try:
            self.client.put_object(bucket, object_name, data, length, content_type=content_type)
            logger.info(f"Uploaded object: {bucket}/{object_name}")
            return True
        except S3Error as e:
            logger.error(f"Error uploading object {bucket}/{object_name}: {e}")
            return False
    
    def delete_object(self, layer: str, object_name: str) -> bool:
        """Delete object from storage"""
        if not self._initialized:
            self.initialize()
        
        bucket = minio_config.get_bucket_for_layer(layer)
        
        try:
            self.client.remove_object(bucket, object_name)
            logger.info(f"Deleted object: {bucket}/{object_name}")
            return True
        except S3Error as e:
            logger.error(f"Error deleting object {bucket}/{object_name}: {e}")
            return False
    
    def read_parquet(self, layer: str, object_name: str) -> Optional[pd.DataFrame]:
        """Read Parquet file into DataFrame"""
        data = self.get_object(layer, object_name)
        if data is None:
            return None
        
        try:
            buffer = io.BytesIO(data)
            df = pd.read_parquet(buffer)
            return df
        except Exception as e:
            logger.error(f"Error reading Parquet file {object_name}: {e}")
            return None
    
    def write_parquet(self, layer: str, object_name: str, df: pd.DataFrame) -> bool:
        """Write DataFrame to Parquet file"""
        try:
            buffer = io.BytesIO()
            df.to_parquet(buffer, index=False, engine='pyarrow')
            buffer.seek(0)
            
            return self.put_object(layer, object_name, buffer, len(buffer.getvalue()), content_type="application/parquet")
        except Exception as e:
            logger.error(f"Error writing Parquet file {object_name}: {e}")
            return False
    
    def read_json(self, layer: str, object_name: str) -> Optional[dict]:
        """Read JSON file"""
        data = self.get_object(layer, object_name)
        if data is None:
            return None
        
        try:
            return json.loads(data.decode('utf-8'))
        except Exception as e:
            logger.error(f"Error reading JSON file {object_name}: {e}")
            return None
    
    def write_json(self, layer: str, object_name: str, data: dict) -> bool:
        """Write JSON file"""
        try:
            json_str = json.dumps(data, ensure_ascii=False, indent=2)
            json_bytes = json_str.encode('utf-8')
            buffer = io.BytesIO(json_bytes)
            
            return self.put_object(layer, object_name, buffer, len(json_bytes), content_type="application/json")
        except Exception as e:
            logger.error(f"Error writing JSON file {object_name}: {e}")
            return False
    
    def get_object_metadata(self, layer: str, object_name: str) -> Optional[Dict[str, Any]]:
        """Get object metadata without downloading content"""
        if not self._initialized:
            self.initialize()
        
        bucket = minio_config.get_bucket_for_layer(layer)
        
        try:
            stat = self.client.stat_object(bucket, object_name)
            return {
                "object_name": stat.object_name,
                "size": stat.size,
                "last_modified": stat.last_modified,
                "etag": stat.etag,
                "content_type": stat.content_type,
                "metadata": stat.metadata
            }
        except S3Error as e:
            logger.error(f"Error getting metadata for {bucket}/{object_name}: {e}")
            return None
    
    def copy_object(self, source_layer: str, source_name: str, dest_layer: str, dest_name: str) -> bool:
        """Copy object between layers/buckets"""
        if not self._initialized:
            self.initialize()
        
        source_bucket = minio_config.get_bucket_for_layer(source_layer)
        dest_bucket = minio_config.get_bucket_for_layer(dest_layer)
        
        try:
            from minio import CopySource
            self.client.copy_object(
                dest_bucket,
                dest_name,
                CopySource(source_bucket, source_name)
            )
            logger.info(f"Copied {source_bucket}/{source_name} to {dest_bucket}/{dest_name}")
            return True
        except S3Error as e:
            logger.error(f"Error copying object: {e}")
            return False
    
    def get_presigned_url(self, layer: str, object_name: str, expires: timedelta = timedelta(hours=1)) -> Optional[str]:
        """Generate presigned URL for temporary access"""
        if not self._initialized:
            self.initialize()
        
        bucket = minio_config.get_bucket_for_layer(layer)
        
        try:
            url = self.client.presigned_get_object(bucket, object_name, expires=expires)
            return url
        except S3Error as e:
            logger.error(f"Error generating presigned URL: {e}")
            return None
    
    def save_metadata(self, layer: str, source: str, entity: str, metadata: Dict[str, Any]) -> bool:
        """Save metadata for a table (used by ingestion service)"""
        if not self._initialized:
            self.initialize()
        
        bucket = minio_config.get_bucket_for_layer(layer)
        metadata_path = f"{source}/{entity}/_metadata.json"
        
        try:
            metadata_json = json.dumps(metadata, indent=2, default=str)
            data = io.BytesIO(metadata_json.encode('utf-8'))
            
            self.client.put_object(
                bucket,
                metadata_path,
                data,
                len(metadata_json),
                content_type='application/json'
            )
            
            logger.info(f"Saved metadata for {layer}/{source}/{entity}")
            return True
        except S3Error as e:
            logger.error(f"Error saving metadata: {e}")
            return False


class BronzeLayerManager:
    """Specialized manager for Bronze layer operations"""
    
    def __init__(self, storage: StorageManager):
        self.storage = storage
    
    def list_tables(self, domain: str) -> List[Dict[str, Any]]:
        """List all tables with enhanced metadata for enterprise catalog"""
        objects = self.storage.list_objects("bronze", domain)
        
        # Group by table - handle both folder-based and direct files
        tables = {}
        table_files = {}  # Track files per table for metadata extraction
        
        for obj in objects:
            # Skip metadata files
            if obj["object_name"].endswith("_metadata.json"):
                continue
                
            # Parse: domain/source_type/table_name/file or domain/source_type/file
            parts = obj["object_name"].split("/")
            
            if len(parts) == 3:
                # Direct file: finance/s3/file.mp4
                source_type = parts[1]
                filename = parts[2]
                table_key = f"{source_type}/{filename}"
                
                if table_key not in tables:
                    tables[table_key] = {
                        "table_name": filename,
                        "source_type": source_type,
                        "file_count": 0,
                        "total_size": 0,
                        "last_modified": obj["last_modified"],
                        "format": self._detect_format(filename),
                        "is_partitioned": False
                    }
                    table_files[table_key] = []
                
                tables[table_key]["file_count"] += 1
                tables[table_key]["total_size"] += obj["size"]
                if obj["last_modified"] > tables[table_key]["last_modified"]:
                    tables[table_key]["last_modified"] = obj["last_modified"]
                table_files[table_key].append(obj)
                    
            elif len(parts) >= 4:
                # Folder-based: finance/postgres/table_name/file.parquet
                source_type = parts[1]
                table_name = parts[2]
                table_key = f"{source_type}/{table_name}"
                
                # Detect partitioning (e.g., run_date=2026-02-10)
                is_partitioned = any("=" in p for p in parts[3:])
                
                if table_key not in tables:
                    tables[table_key] = {
                        "table_name": table_name,
                        "source_type": source_type,
                        "file_count": 0,
                        "total_size": 0,
                        "last_modified": obj["last_modified"],
                        "format": self._detect_format(obj["object_name"]),
                        "is_partitioned": is_partitioned
                    }
                    table_files[table_key] = []
                
                tables[table_key]["file_count"] += 1
                tables[table_key]["total_size"] += obj["size"]
                tables[table_key]["is_partitioned"] = tables[table_key]["is_partitioned"] or is_partitioned
                if obj["last_modified"] > tables[table_key]["last_modified"]:
                    tables[table_key]["last_modified"] = obj["last_modified"]
                table_files[table_key].append(obj)
        
        # Enhance with row counts and quality indicators (sample first file)
        result = []
        for table_key, table_info in tables.items():
            # Get row count for structured data
            if table_info["format"] == "parquet" and table_files[table_key]:
                try:
                    first_file = table_files[table_key][0]["object_name"]
                    df = self.storage.read_parquet("bronze", first_file)
                    if df is not None:
                        table_info["row_count"] = len(df)
                        table_info["column_count"] = len(df.columns)
                        # Simple quality indicator
                        null_pct = (df.isnull().sum().sum() / (len(df) * len(df.columns))) * 100
                        table_info["quality_score"] = max(0, 100 - int(null_pct))
                    else:
                        table_info["row_count"] = None
                        table_info["column_count"] = None
                        table_info["quality_score"] = None
                except Exception as e:
                    logger.debug(f"Could not get row count for {table_key}: {e}")
                    table_info["row_count"] = None
                    table_info["column_count"] = None
                    table_info["quality_score"] = None
            else:
                table_info["row_count"] = None
                table_info["column_count"] = None
                table_info["quality_score"] = None
            
            # Extract partition columns if partitioned
            if table_info["is_partitioned"] and table_files[table_key]:
                partition_cols = self._extract_partition_columns(table_files[table_key][0]["object_name"])
                table_info["partition_columns"] = partition_cols
            else:
                table_info["partition_columns"] = []
            
            result.append(table_info)
        
        return result
    
    def _detect_format(self, filename: str) -> str:
        """Detect file format from filename"""
        ext = filename.split('.')[-1].lower()
        format_map = {
            "parquet": "parquet",
            "json": "json",
            "csv": "csv", "tsv": "csv",
            "jpg": "image", "jpeg": "image", "png": "image", "gif": "image",
            "mp4": "video", "avi": "video", "mov": "video",
            "pdf": "pdf",
            "txt": "text", "log": "text", "md": "text"
        }
        return format_map.get(ext, "unknown")
    
    def _extract_partition_columns(self, object_name: str) -> List[str]:
        """Extract partition column names from path like run_date=2026-02-10"""
        parts = object_name.split("/")
        partition_cols = []
        for part in parts:
            if "=" in part:
                col_name = part.split("=")[0]
                if col_name not in partition_cols:
                    partition_cols.append(col_name)
        return partition_cols
    
    def get_table_files(self, domain: str, table_name: str) -> List[Dict[str, Any]]:
        """Get all files for a specific table (handles both direct files and nested structures)"""
        # List all objects in the domain and filter by table_name
        objects = self.storage.list_objects("bronze", domain)
        
        files = []
        for obj in objects:
            # Skip metadata files
            if obj["object_name"].endswith("_metadata.json"):
                continue
            
            parts = obj["object_name"].split("/")
            
            # Check for direct file match: domain/source/filename
            if len(parts) == 3 and parts[2] == table_name:
                # Direct file like finance/s3/file.mp4
                if self._is_supported_file(obj["object_name"]):
                    files.append(obj)
            # Check for folder-based match: domain/source/table_name/...
            elif len(parts) >= 4 and parts[2] == table_name:
                # Folder-based like finance/postgres/table_name/file.parquet
                if self._is_supported_file(obj["object_name"]):
                    files.append(obj)
        
        return files
    
    def _is_supported_file(self, filename: str) -> bool:
        """Check if file extension is supported"""
        return filename.endswith((".parquet", ".jpg", ".jpeg", ".png", ".gif", 
                                 ".bmp", ".tiff", ".webp", ".pdf", ".json", 
                                 ".csv", ".tsv", ".mp4", ".avi", ".mov", 
                                 ".mkv", ".webm", ".txt", ".log", ".md"))
    
    def read_table_sample(self, domain: str, table_name: str, max_rows: Optional[int] = 1000) -> Optional[pd.DataFrame]:
        """Read sample data from ALL files of a table (consolidated view)"""
        files = self.get_table_files(domain, table_name)
        if not files:
            return None
        
        # Read ALL parquet files and concatenate them
        dfs = []
        for file_obj in files:
            file_path = file_obj["object_name"]
            df = self.storage.read_parquet("bronze", file_path)
            if df is not None:
                dfs.append(df)
        
        if not dfs:
            return None
        
        # Concatenate all dataframes
        combined_df = pd.concat(dfs, ignore_index=True)
        
        # Sort by primary key or timestamp to maintain order
        sort_column = self._find_sort_column(combined_df)
        if sort_column:
            combined_df = combined_df.sort_values(by=sort_column).reset_index(drop=True)
        
        # Apply row limit if specified
        if max_rows is not None and len(combined_df) > max_rows:
            return combined_df.head(max_rows)
        
        return combined_df
    
    def _find_sort_column(self, df: pd.DataFrame) -> Optional[str]:
        """Intelligently find the best column to sort by"""
        if df.empty:
            return None
        
        columns = df.columns.tolist()
        
        # Priority 1: Look for ID columns (transaction_id, user_id, id, etc.)
        id_candidates = [col for col in columns if col.lower().endswith('_id') or col.lower() == 'id']
        for col in id_candidates:
            # Check if it's numeric and has sequential-like values
            if pd.api.types.is_numeric_dtype(df[col]):
                return col
        
        # Priority 2: Look for timestamp/date columns (created_at, timestamp, date, etc.)
        timestamp_candidates = [col for col in columns if any(kw in col.lower() for kw in ['timestamp', 'created_at', 'updated_at', 'date', 'time'])]
        if timestamp_candidates:
            return timestamp_candidates[0]
        
        # Priority 3: First numeric column that looks like an index
        for col in columns:
            if pd.api.types.is_numeric_dtype(df[col]):
                # Check if values are mostly unique (likely an ID/index)
                if df[col].nunique() > len(df) * 0.8:
                    return col
        
        return None
    
    def _detect_file_type(self, table_name: str) -> str:
        """Detect if this is structured (parquet) or unstructured file"""
        extension = table_name.split('.')[-1].lower()
        
        # Image formats
        if extension in ['jpg', 'jpeg', 'png', 'gif', 'bmp', 'tiff', 'webp']:
            return 'image'
        # Video formats
        elif extension in ['mp4', 'avi', 'mov', 'mkv', 'webm']:
            return 'video'
        # Document formats
        elif extension == 'pdf':
            return 'pdf'
        # Text formats
        elif extension in ['txt', 'log', 'md']:
            return 'text'
        # Data formats
        elif extension == 'json':
            return 'json'
        elif extension in ['csv', 'tsv']:
            return 'csv'
        # Default to structured
        else:
            return 'structured'
    
    def get_table_schema(self, domain: str, table_name: str) -> Optional[Dict[str, Any]]:
        """Get schema information for a table"""
        files = self.get_table_files(domain, table_name)
        if not files:
            return None
        
        # Detect file type from the first actual file (not from table name)
        first_file = files[0]["object_name"]
        file_type = self._detect_file_type(first_file)
        
        # For unstructured data, return file metadata instead of schema
        if file_type != 'structured':
            return self._get_file_metadata(domain, first_file, file_type, files[0])
        
        # Read schema from first file (structured data)
        df = self.storage.read_parquet("bronze", first_file)
        
        if df is None:
            return None
        
        schema = {
            "columns": [],
            "row_count": len(df),
            "memory_usage": df.memory_usage(deep=True).sum()
        }
        
        for col in df.columns:
            col_info = {
                "name": col,
                "dtype": str(df[col].dtype),
                "null_count": int(df[col].isnull().sum()),
                "null_percentage": float(df[col].isnull().sum() / len(df) * 100),
                "unique_count": int(df[col].nunique())
            }
            
            # Add type-specific statistics
            if pd.api.types.is_numeric_dtype(df[col]):
                col_info["min"] = float(df[col].min()) if not df[col].isnull().all() else None
                col_info["max"] = float(df[col].max()) if not df[col].isnull().all() else None
                col_info["mean"] = float(df[col].mean()) if not df[col].isnull().all() else None
            
            schema["columns"].append(col_info)
        
        return schema
    
    def _get_file_metadata(self, domain: str, file_path: str, file_type: str, file_obj: Dict) -> Dict[str, Any]:
        """Get metadata for unstructured files"""
        import mimetypes
        
        # Extract just the filename from the path
        filename = file_path.split("/")[-1]
        
        # Base metadata
        metadata = {
            "file_type": file_type,
            "mime_type": mimetypes.guess_type(filename)[0] or 'application/octet-stream',
            "filename": filename,
            "size_bytes": file_obj["size"],
            "last_modified": file_obj["last_modified"],
            "object_path": file_obj["object_name"]
        }
        
        try:
            # Try to get the actual file content for detailed metadata
            bucket = minio_config.bronze_bucket
            file_data = self.storage.client.get_object(bucket, file_obj["object_name"])
            
            if file_type == 'image':
                # Extract image metadata
                try:
                    from PIL import Image
                    import io
                    img = Image.open(io.BytesIO(file_data.read()))
                    metadata.update({
                        "width": img.width,
                        "height": img.height,
                        "format": img.format,
                        "mode": img.mode,
                        "has_exif": bool(img.getexif()) if hasattr(img, 'getexif') else False
                    })
                except Exception as e:
                    logger.warning(f"Failed to extract image metadata: {e}")
            
            elif file_type == 'pdf':
                # Extract PDF metadata
                try:
                    import PyPDF2
                    import io
                    pdf = PyPDF2.PdfReader(io.BytesIO(file_data.read()))
                    metadata.update({
                        "page_count": len(pdf.pages),
                        "title": pdf.metadata.get('/Title', '') if pdf.metadata else '',
                        "author": pdf.metadata.get('/Author', '') if pdf.metadata else ''
                    })
                except Exception as e:
                    logger.warning(f"Failed to extract PDF metadata: {e}")
            
            elif file_type == 'json':
                # Validate and get JSON info
                try:
                    import json
                    content = file_data.read().decode('utf-8')
                    data = json.loads(content)
                    metadata.update({
                        "valid_json": True,
                        "content_type": type(data).__name__,
                        "content_preview": json.dumps(data, indent=2)[:500] if len(json.dumps(data)) > 500 else json.dumps(data, indent=2)
                    })
                except Exception as e:
                    metadata["valid_json"] = False
                    logger.warning(f"Failed to parse JSON: {e}")
            
            elif file_type == 'video':
                # Extract video metadata
                metadata.update({
                    "duration": None,  # Would need ffmpeg/moviepy for actual duration
                    "video_format": filename.split('.')[-1].upper(),
                    "note": "Video metadata extraction requires additional libraries"
                })
            
            elif file_type == 'text':
                # Get text file preview
                try:
                    content = file_data.read().decode('utf-8')
                    metadata.update({
                        "line_count": len(content.split('\n')),
                        "char_count": len(content),
                        "content_preview": content[:1000] if len(content) > 1000 else content
                    })
                except Exception as e:
                    logger.warning(f"Failed to read text file: {e}")
        
        except Exception as e:
            logger.warning(f"Failed to fetch file content for metadata extraction: {e}")
        
        return metadata


# Global storage instances
storage_manager = StorageManager()
bronze_manager = None  # Will be initialized after storage_manager


def get_bronze_manager() -> BronzeLayerManager:
    """Get the bronze layer manager instance"""
    if bronze_manager is None:
        raise RuntimeError("Storage not initialized. Call initialize_storage() first.")
    return bronze_manager


def initialize_storage():
    """Initialize storage on application startup"""
    global bronze_manager
    try:
        storage_manager.initialize()
        bronze_manager = BronzeLayerManager(storage_manager)
        logger.info("Storage initialized successfully")
        return True
    except Exception as e:
        logger.error(f"Storage initialization failed: {e}")
        return False


if __name__ == "__main__":
    # Test storage connection
    logging.basicConfig(level=logging.INFO)
    if initialize_storage():
        print("✓ MinIO connection successful")
        
        # List buckets
        for bucket in storage_manager.client.list_buckets():
            print(f"  - {bucket.name}")
