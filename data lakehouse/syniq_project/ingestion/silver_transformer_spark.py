"""
Silver Transformer - Spark + Iceberg Implementation
Replaces Pandas-based transformation for large datasets (>1GB)

Features:
- Distributed processing with Apache Spark
- ACID transactions with Iceberg tables
- Schema evolution and time-travel
- Incremental processing with watermarks
- Data quality validation
- Quarantine management
"""
import os
import json
import logging
import io
from pathlib import Path
from datetime import datetime
from typing import Dict, Any, List, Optional, Tuple

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col, trim, upper, lower, when, lit, isnan, isnull, 
    count, sum as spark_sum, coalesce, regexp_replace, to_timestamp,
    current_timestamp, md5, concat_ws, year, month, dayofmonth
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, IntegerType, 
    TimestampType, BooleanType, LongType, ArrayType
)

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class SilverTransformerSpark:
    """
    Spark-based Silver layer transformation engine
    Processes Bronze data → Silver with quality checks using distributed computing
    """
    
    def __init__(self, spark: SparkSession = None, minio_config: Dict[str, Any] = None):
        """
        Initialize Spark transformer
        
        Args:
            spark: Existing Spark session (will create if None)
            minio_config: MinIO connection configuration
        """
        self.spark = spark
        self.minio_config = minio_config or self._load_minio_config()
        
        # Initialize Spark session if not provided
        if self.spark is None:
            self.spark = self._create_spark_session()
        
        # Configuration
        self.catalog_name = "syniq_iceberg"
        self.silver_bucket = "syniqai-bronze"  # Actual MinIO bucket name
        self.quarantine_bucket = "syniqai-bronze"
        
        logger.info(f"✅ SilverTransformerSpark initialized with Spark {self.spark.version}")
    
    def _load_minio_config(self) -> Dict[str, Any]:
        """Load MinIO configuration from config file"""
        try:
            from utils.config_loader import load_config
            from pathlib import Path
            config_path = Path(__file__).parent.parent / "config" / "minio_config.yaml"
            config = load_config(str(config_path))
            return config.get('minio', {})
        except Exception as e:
            logger.warning(f"Could not load MinIO config: {e}")
            # Fallback to actual MinIO credentials
            return {
                'endpoint': 'http://localhost:9000',
                'access_key': 'admin',
                'secret_key': 'password123'
            }
    
    def _create_spark_session(self) -> SparkSession:
        """Create Spark session with Iceberg configuration"""
        try:
            from utils.spark_iceberg_config import create_spark_session_with_iceberg
            logger.info("Creating Spark session with Iceberg...")
            spark = create_spark_session_with_iceberg(
                app_name="SilverTransformer",
                minio_config=self.minio_config
            )
            logger.info(f"✅ Spark session ready: {spark.version}")
            return spark
        except Exception as e:
            logger.error(f"Failed to create Spark session: {e}")
            raise
    
    def cleanup(self):
        """Cleanup Spark resources (call after transformation completes)"""
        try:
            if self.spark is not None:
                # Don't stop the session, just uncache any cached DataFrames
                logger.info("Cleaning up Spark resources...")
                self.spark.catalog.clearCache()
                logger.info("✅ Spark resources cleaned")
        except Exception as e:
            logger.warning(f"Error during cleanup: {e}")
    
    def transform(
        self,
        source: str,
        entity: str,
        domain: str = "general",
        execution_mode: str = "full",
        watermark_column: str = None,
        watermark_value: str = None,
        rules: List[Dict[str, Any]] = None,
        custom_sql: str = None,
        data_type: str = "structured",
        unstructured_type: str = None,
        image_transforms: Dict[str, Any] = None,
        video_transforms: Dict[str, Any] = None,
        audio_transforms: Dict[str, Any] = None,
        text_transforms: Dict[str, Any] = None,
        pdf_transforms: Dict[str, Any] = None
    ) -> Dict[str, Any]:
        """
        Transform Bronze data to Silver using Spark
        
        Args:
            source: Source system (e.g., 'postgres', 'mysql')
            entity: Entity name (e.g., 'customers', 'transactions')
            domain: Domain for rule filtering
            execution_mode: 'full' or 'incremental'
            watermark_column: Column for incremental processing
            watermark_value: Last processed watermark value
            rules: List of data quality rules to apply
            custom_sql: Optional custom SQL transformation
            data_type: 'structured' or 'unstructured'
            unstructured_type: 'image', 'video', 'audio', 'text', 'pdf'
            image_transforms: Image transformation options
            video_transforms: Video transformation options
            audio_transforms: Audio transformation options
            text_transforms: Text transformation options
            pdf_transforms: PDF transformation options
        
        Returns:
            Transformation result with metrics
        """
        start_time = datetime.now()
        job_id = f"silver_{source}_{entity}_{start_time.strftime('%Y%m%d_%H%M%S')}"
        
        logger.info(f"🚀 Starting Silver transformation: {source}.{entity}")
        logger.info(f"   Mode: {execution_mode}, Domain: {domain}, Data Type: {data_type}")
        
        try:
            # Handle unstructured data differently
            if data_type == "unstructured":
                return self._transform_unstructured(
                    source, entity, domain, unstructured_type,
                    image_transforms, video_transforms, audio_transforms,
                    text_transforms, pdf_transforms,
                    job_id, start_time
                )
            
            # Step 1: Read from Bronze (structured data)
            bronze_df = self._read_bronze(source, entity, domain, execution_mode, watermark_column, watermark_value)
            initial_count = bronze_df.count()
            logger.info(f"✅ Read {initial_count:,} rows from Bronze")
            
            # Step 2: Apply data quality rules
            clean_df, quarantine_df, quality_metrics = self._apply_quality_rules(
                bronze_df, source, entity, rules or []
            )
            
            # Step 3: Apply custom SQL transformation if provided
            if custom_sql:
                clean_df = self._apply_custom_sql(clean_df, custom_sql, source, entity)
            
            # Step 4: Remove duplicates
            clean_df, duplicates_removed = self._remove_duplicates(clean_df)
            
            # Step 5: Standardize data types and formats
            clean_df = self._standardize_data(clean_df)
            
            # Step 6: Add metadata columns
            clean_df = self._add_metadata_columns(clean_df, source, entity, job_id)
            
            final_count = clean_df.count()
            quarantine_count = quarantine_df.count() if quarantine_df else 0
            
            logger.info(f"📊 Transformation complete:")
            logger.info(f"   Processed: {final_count:,} rows")
            logger.info(f"   Quarantined: {quarantine_count:,} rows")
            logger.info(f"   Duplicates removed: {duplicates_removed:,}")
            
            # Step 7: Write to Silver as Iceberg table
            silver_table = self._write_to_silver(
                clean_df, source, entity, execution_mode, watermark_column
            )
            
            # Step 8: Write quarantine data
            if quarantine_count > 0:
                self._write_quarantine(quarantine_df, source, entity, job_id)
            
            # Step 9: Calculate final metrics
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            
            result = {
                "job_id": job_id,
                "status": "completed",
                "source": source,
                "entity": entity,
                "domain": domain,
                "execution_mode": execution_mode,
                "silver_table": silver_table,
                "metrics": {
                    "initial_rows": initial_count,
                    "final_rows": final_count,
                    "rows_quarantined": quarantine_count,
                    "duplicates_removed": duplicates_removed,
                    "processing_time_seconds": duration,
                    "throughput_rows_per_second": int(initial_count / duration) if duration > 0 else 0
                },
                "quality_metrics": quality_metrics,
                "watermark": {
                    "column": watermark_column,
                    "last_value": self._get_max_watermark(clean_df, watermark_column) if watermark_column else None
                },
                "start_time": start_time.isoformat(),
                "end_time": end_time.isoformat()
            }
            
            logger.info(f"✅ Silver transformation completed in {duration:.2f}s")
            return result
            
        except Exception as e:
            logger.error(f"❌ Silver transformation failed: {str(e)}")
            import traceback
            logger.error(traceback.format_exc())
            
            return {
                "job_id": job_id,
                "status": "failed",
                "source": source,
                "entity": entity,
                "error": str(e),
                "start_time": start_time.isoformat(),
                "end_time": datetime.now().isoformat()
            }
    
    def _transform_unstructured(
        self,
        source: str,
        entity: str,
        domain: str,
        unstructured_type: str,
        image_transforms: Dict[str, Any],
        video_transforms: Dict[str, Any],
        audio_transforms: Dict[str, Any],
        text_transforms: Dict[str, Any],
        pdf_transforms: Dict[str, Any],
        job_id: str,
        start_time: datetime
    ) -> Dict[str, Any]:
        """Transform unstructured data (images, videos, audio, text, pdf)"""
        logger.info(f"🖼️ Transforming unstructured data: {unstructured_type}")
        
        try:
            # Step 1: Read unstructured files from Bronze
            files_df = self._read_unstructured_bronze(source, entity, domain, unstructured_type)
            initial_count = files_df.count()
            logger.info(f"✅ Found {initial_count:,} {unstructured_type} files")
            
            # Step 2: Apply transformations based on type
            if unstructured_type == "image" and image_transforms:
                files_df = self._apply_image_transforms(files_df, image_transforms)
            elif unstructured_type == "video" and video_transforms:
                files_df = self._apply_video_transforms(files_df, video_transforms)
            elif unstructured_type == "audio" and audio_transforms:
                files_df = self._apply_audio_transforms(files_df, audio_transforms)
            elif unstructured_type == "text" and text_transforms:
                files_df = self._apply_text_transforms(files_df, text_transforms)
            elif unstructured_type == "pdf" and pdf_transforms:
                files_df = self._apply_pdf_transforms(files_df, pdf_transforms)
            
            # Step 3: Add metadata
            files_df = files_df.withColumn("_processing_timestamp", current_timestamp())
            files_df = files_df.withColumn("_job_id", lit(job_id))
            files_df = files_df.withColumn("_data_type", lit(unstructured_type))
            
            # Drop binary content to avoid storing large blobs in metadata table
            files_df = files_df.drop("content") if "content" in files_df.columns else files_df
            
            final_count = files_df.count()
            
            logger.info(f"✅ Transformed {final_count:,} files with finalized schema")
            logger.info(f"   Schema: {files_df.schema.simpleString()}")
            
            # Step 4: Write to Silver (as metadata catalog)
            silver_table = self._write_unstructured_to_silver(files_df, source, entity)
            
            # Calculate metrics
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            
            result = {
                "job_id": job_id,
                "status": "completed",
                "source": source,
                "entity": entity,
                "domain": domain,
                "data_type": "unstructured",
                "unstructured_type": unstructured_type,
                "silver_table": silver_table,
                "row_count": final_count,
                "duration": duration,
                "metrics": {
                    "initial_files": initial_count,
                    "processed_files": final_count,
                    "processing_time_seconds": duration
                },
                "start_time": start_time.isoformat(),
                "end_time": end_time.isoformat()
            }
            
            logger.info(f"✅ Unstructured transformation completed in {duration:.2f}s")
            return result
            
        except Exception as e:
            logger.error(f"❌ Unstructured transformation failed: {str(e)}")
            import traceback
            logger.error(traceback.format_exc())
            
            return {
                "job_id": job_id,
                "status": "failed",
                "source": source,
                "entity": entity,
                "error": str(e),
                "start_time": start_time.isoformat(),
                "end_time": datetime.now().isoformat()
            }
    
    def _read_unstructured_bronze(
        self,
        source: str,
        entity: str,
        domain: str,
        unstructured_type: str
    ) -> DataFrame:
        """Read unstructured files from Bronze as metadata DataFrame"""
        # MinIO path structure: syniqai-bronze/{domain}/{source}/{entity}/
        bronze_path = f"s3a://syniqai-bronze/{domain}/{source}/{entity}/"
        
        logger.info(f"📖 Reading unstructured files from: {bronze_path}")
        
        try:
            # Read files as binary format to get file metadata
            files_df = self.spark.read.format("binaryFile") \
                .option("recursiveFileLookup", "true") \
                .option("pathGlobFilter", self._get_file_filter(unstructured_type)) \
                .load(bronze_path)
            
            # Add file metadata columns
            from pyspark.sql.functions import regexp_extract, element_at, split, lower
            
            files_df = files_df \
                .withColumn("file_name", element_at(split(col("path"), "/"), -1)) \
                .withColumn("file_extension", lower(regexp_extract(col("file_name"), r"\.(\w+)$", 1))) \
                .withColumn("file_size_bytes", col("length"))
            
            # Filter by file extension based on unstructured type
            extension_filters = {
                "image": ["jpg", "jpeg", "png", "gif", "bmp", "tiff", "webp"],
                "video": ["mp4", "avi", "mov", "wmv", "flv", "mkv", "webm"],
                "audio": ["mp3", "wav", "flac", "aac", "ogg", "m4a"],
                "text": ["txt", "csv", "json", "log"],
                "pdf": ["pdf"]
            }
            
            valid_extensions = extension_filters.get(unstructured_type, [])
            if valid_extensions:
                files_df = files_df.filter(col("file_extension").isin(valid_extensions))
            
            return files_df
            
        except Exception as e:
            logger.error(f"Failed to read unstructured Bronze data: {e}")
            raise
    
    def _get_file_filter(self, unstructured_type: str) -> str:
        """Get file filter pattern based on unstructured type"""
        # Note: pathGlobFilter doesn't support brace expansion, so we use wildcard
        # and filter by extension in the dataframe
        return "*.*"
    
    def _apply_image_transforms(self, df: DataFrame, transforms: Dict[str, Any]) -> DataFrame:
        """Apply image transformations with actual feature extraction"""
        logger.info("🖼️ Applying image transformations with feature extraction...")
        logger.info("   Collecting files to driver for processing (avoids serialization issues)...")
        
        # Collect data to driver - extract ONLY what we need to avoid any Spark object references
        files_data = []
        for row in df.collect():
            files_data.append({
                'path': str(row.path),  # Ensure pure Python string
                'file_name': str(row.file_name),
                'file_extension': str(row.file_extension),
                'file_size_bytes': int(row.file_size_bytes),
                'content': bytes(row.content)  # Ensure pure Python bytes
            })
        
        logger.info(f"   Processing {len(files_data)} image files locally...")
        
        # Process locally to avoid serialization
        from PIL import Image
        import cv2
        import numpy as np
        
        results = []
        for row_data in files_data:
            try:
                # Load image from binary content
                image = Image.open(io.BytesIO(row_data['content']))
                
                # Basic properties
                width, height = image.size
                color_mode = str(image.mode)
                img_format = str(image.format) if image.format else "UNKNOWN"
                
                # Convert to numpy for analysis
                img_array = np.array(image.convert('RGB'))
                
                # Calculate brightness
                brightness = float(np.mean(img_array))
                
                # Calculate contrast (standard deviation)
                contrast = float(np.std(img_array))
                
                # Calculate blur score using Laplacian variance
                gray = cv2.cvtColor(img_array, cv2.COLOR_RGB2GRAY)
                blur_score = float(cv2.Laplacian(gray, cv2.CV_64F).var())
                
                # Explicitly close image to free memory
                image.close()
                del img_array
                del gray
                
                # Store ONLY primitive types - no objects, no numpy types
                results.append((
                    str(row_data['path']),
                    str(row_data['file_name']),
                    str(row_data['file_extension']),
                    int(row_data['file_size_bytes']),
                    int(width),
                    int(height),
                    str(color_mode),
                    str(img_format),
                    False,  # is_corrupted
                    float(brightness),
                    float(contrast),
                    float(blur_score),
                    None  # error_message
                ))
                
            except Exception as e:
                logger.warning(f"   Failed to process {row_data.get('file_name', 'unknown')}: {e}")
                results.append((
                    str(row_data['path']),
                    str(row_data['file_name']),
                    str(row_data['file_extension']),
                    int(row_data['file_size_bytes']),
                    None,
                    None,
                    None,
                    None,
                    True,  # is_corrupted
                    None,
                    None,
                    None,
                    str(e)
                ))
        
        # Get active Spark session
        active_spark = SparkSession.getActiveSession()
        if active_spark is None:
            raise RuntimeError("No active Spark session found")
        
        # Define explicit Spark schema to avoid serialization issues
        from pyspark.sql.types import StructType, StructField, StringType, LongType, IntegerType, BooleanType, DoubleType
        
        schema = StructType([
            StructField("path", StringType(), False),
            StructField("file_name", StringType(), False),
            StructField("file_extension", StringType(), False),
            StructField("file_size_bytes", LongType(), False),
            StructField("width", IntegerType(), True),
            StructField("height", IntegerType(), True),
            StructField("color_mode", StringType(), True),
            StructField("format", StringType(), True),
            StructField("is_corrupted", BooleanType(), False),
            StructField("brightness_avg", DoubleType(), True),
            StructField("contrast_score", DoubleType(), True),
            StructField("blur_score", DoubleType(), True),
            StructField("error_message", StringType(), True)
        ])
        
        # Create Spark DataFrame DIRECTLY from Python list with explicit schema
        # This bypasses pandas and avoids all serialization issues
        df = active_spark.createDataFrame(results, schema=schema)
        
        # Add original dimensions before transformations
        df = df.withColumn("original_dimensions", 
                          concat_ws("x", col("width"), col("height")))
        
        # Add transformation flags
        df = df.withColumn("is_resized", lit(transforms.get("resize", False)))
        if transforms.get("resize"):
            df = df.withColumn("target_width", lit(transforms.get("resizeWidth", 224)))
            df = df.withColumn("target_height", lit(transforms.get("resizeHeight", 224)))
        
        df = df.withColumn("is_format_converted", lit(bool(transforms.get("formatConversion"))))
        if transforms.get("formatConversion"):
            df = df.withColumn("target_format", lit(transforms.get("formatConversion")))
        
        df = df.withColumn("is_normalized", lit(transforms.get("normalizePixels", False)))
        df = df.withColumn("is_grayscale", lit(transforms.get("grayscale", False)))
        df = df.withColumn("edge_detected", lit(transforms.get("edgeDetection", False)))
        df = df.withColumn("object_detected", lit(transforms.get("objectDetection", False)))
        
        # Add processing metadata
        df = df.withColumn("processed_at", current_timestamp())
        df = df.withColumn("processing_status", 
                          when(col("is_corrupted"), lit("failed")).otherwise(lit("success")))
        
        # Add storage references
        df = df.withColumn("bronze_path", col("path"))
        df = df.withColumn("silver_path", lit(None).cast(StringType()))
        
        logger.info("✅ Image feature extraction completed")
        return df
    
    def _apply_video_transforms(self, df: DataFrame, transforms: Dict[str, Any]) -> DataFrame:
        """Apply video transformations with metadata extraction"""
        logger.info("🎥 Applying video transformations with metadata extraction...")
        logger.info("   Collecting files to driver for processing...")
        
        # Extract pure Python primitives to avoid Spark object references
        files_data = []
        for row in df.collect():
            files_data.append({
                'path': str(row.path),
                'file_name': str(row.file_name),
                'file_extension': str(row.file_extension),
                'file_size_bytes': int(row.file_size_bytes),
                'content': bytes(row.content)
            })
        logger.info(f"   Processing {len(files_data)} video files locally...")
        
        results = []
        for row in files_data:
            try:
                import cv2
                import tempfile
                
                # Save to temp file for OpenCV processing
                with tempfile.NamedTemporaryFile(suffix='.mp4', delete=False) as tmp:
                    tmp.write(row['content'])
                    tmp_path = tmp.name
                
                try:
                    cap = cv2.VideoCapture(tmp_path)
                    
                    # Extract video properties
                    width = int(cap.get(cv2.CAP_PROP_FRAME_WIDTH))
                    height = int(cap.get(cv2.CAP_PROP_FRAME_HEIGHT))
                    fps = cap.get(cv2.CAP_PROP_FPS)
                    frame_count = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))
                    duration = frame_count / fps if fps > 0 else 0
                    resolution = f"{width}x{height}"
                    
                    cap.release()
                    os.unlink(tmp_path)
                    
                    results.append((
                        str(row['path']),
                        str(row['file_name']),
                        str(row['file_extension']),
                        int(row['file_size_bytes']),
                        str("UNKNOWN"),
                        float(duration),
                        float(fps),
                        int(width),
                        int(height),
                        str(resolution),
                        None,
                        None,
                        None,
                        None,
                        int(frame_count),
                        False,
                        None
                    ))
                finally:
                    if os.path.exists(tmp_path):
                        os.unlink(tmp_path)
                
            except Exception as e:
                logger.warning(f"   Failed to process {row.get('file_name', 'unknown')}: {e}")
                results.append((
                    str(row['path']),
                    str(row['file_name']),
                    str(row['file_extension']),
                    int(row['file_size_bytes']),
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    True,
                    str(e)
                ))
        
        # Define explicit Spark schema to avoid serialization issues
        from pyspark.sql.types import StructType, StructField, StringType, LongType, IntegerType, BooleanType, DoubleType
        
        schema = StructType([
            StructField("path", StringType(), False),
            StructField("file_name", StringType(), False),
            StructField("file_extension", StringType(), False),
            StructField("file_size_bytes", LongType(), False),
            StructField("codec", StringType(), True),
            StructField("duration_seconds", DoubleType(), True),
            StructField("fps", DoubleType(), True),
            StructField("width", IntegerType(), True),
            StructField("height", IntegerType(), True),
            StructField("resolution", StringType(), True),
            StructField("bitrate_kbps", DoubleType(), True),
            StructField("has_audio", BooleanType(), True),
            StructField("audio_codec", StringType(), True),
            StructField("audio_channels", IntegerType(), True),
            StructField("frame_count", IntegerType(), True),
            StructField("is_corrupted", BooleanType(), False),
            StructField("error_message", StringType(), True)
        ])
        
        active_spark = SparkSession.getActiveSession()
        if active_spark is None:
            raise RuntimeError("No active Spark session found")
        
        # Create Spark DataFrame DIRECTLY from Python list with explicit schema
        df = active_spark.createDataFrame(results, schema=schema)
        
        # Add transformation flags
        df = df.withColumn("is_compressed", lit(transforms.get("compression", False)))
        df = df.withColumn("compression_ratio", lit(None).cast(DoubleType()))
        df = df.withColumn("is_fps_normalized", lit(transforms.get("normalizeFPS", False)))
        
        if transforms.get("normalizeFPS"):
            df = df.withColumn("target_fps", lit(transforms.get("targetFPS", 30)))
        
        df = df.withColumn("is_format_converted", lit(bool(transforms.get("formatConversion"))))
        if transforms.get("formatConversion"):
            df = df.withColumn("target_format", lit(transforms.get("formatConversion")))
        
        # Add processing metadata
        df = df.withColumn("processed_at", current_timestamp())
        df = df.withColumn("processing_status", 
                          when(col("is_corrupted"), lit("failed")).otherwise(lit("success")))
        
        # Add storage references
        df = df.withColumn("bronze_path", col("path"))
        df = df.withColumn("silver_path", lit(None).cast(StringType()))
        
        logger.info("✅ Video metadata extraction completed")
        return df
    
    def _apply_audio_transforms(self, df: DataFrame, transforms: Dict[str, Any]) -> DataFrame:
        """Apply audio transformations with metadata extraction"""
        logger.info("🎧 Applying audio transformations with metadata extraction...")
        logger.info("   Collecting files to driver for processing...")
        
        # Extract pure Python primitives to avoid Spark object references
        files_data = []
        for row in df.collect():
            files_data.append({
                'path': str(row.path),
                'file_name': str(row.file_name),
                'file_extension': str(row.file_extension),
                'file_size_bytes': int(row.file_size_bytes),
                'content': bytes(row.content)
            })
        logger.info(f"   Processing {len(files_data)} audio files locally...")
        
        results = []
        for row in files_data:
            try:
                import librosa
                import numpy as np
                import tempfile
                
                # Save to temp file for librosa processing
                with tempfile.NamedTemporaryFile(suffix='.mp3', delete=False) as tmp:
                    tmp.write(row['content'])
                    tmp_path = tmp.name
                
                try:
                    # Load audio with librosa
                    y, sr = librosa.load(tmp_path, sr=None, mono=False)
                    
                    # Handle stereo vs mono
                    if y.ndim > 1:
                        channels = y.shape[0]
                        y_mono = np.mean(y, axis=0)
                    else:
                        channels = 1
                        y_mono = y
                    
                    # Calculate duration
                    duration = len(y_mono) / sr
                    
                    # Calculate volume metrics
                    rms = librosa.feature.rms(y=y_mono)[0]
                    avg_volume_db = 20 * np.log10(np.mean(rms) + 1e-10)
                    peak_volume_db = 20 * np.log10(np.max(np.abs(y_mono)) + 1e-10)
                    
                    # Detect silence
                    silence_threshold = 0.01
                    is_silent = np.max(np.abs(y_mono)) < silence_threshold
                    silence_frames = np.sum(np.abs(y_mono) < silence_threshold)
                    silence_percentage = (silence_frames / len(y_mono)) * 100
                    
                    os.unlink(tmp_path)
                    
                    results.append((
                        str(row['path']),
                        str(row['file_name']),
                        str(row['file_extension']),
                        int(row['file_size_bytes']),
                        str("UNKNOWN"),
                        float(duration),
                        int(sr),
                        None,
                        int(channels),
                        None,
                        float(avg_volume_db),
                        float(peak_volume_db),
                        bool(is_silent),
                        float(silence_percentage),
                        False,
                        None
                    ))
                finally:
                    if os.path.exists(tmp_path):
                        os.unlink(tmp_path)
                
            except Exception as e:
                logger.warning(f"   Failed to process {row.get('file_name', 'unknown')}: {e}")
                results.append((
                    str(row['path']),
                    str(row['file_name']),
                    str(row['file_extension']),
                    int(row['file_size_bytes']),
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    True,
                    str(e)
                ))
        
        # Define explicit Spark schema to avoid serialization issues
        from pyspark.sql.types import StructType, StructField, StringType, LongType, IntegerType, BooleanType, DoubleType
        
        schema = StructType([
            StructField("path", StringType(), False),
            StructField("file_name", StringType(), False),
            StructField("file_extension", StringType(), False),
            StructField("file_size_bytes", LongType(), False),
            StructField("codec", StringType(), True),
            StructField("duration_seconds", DoubleType(), True),
            StructField("sample_rate_hz", IntegerType(), True),
            StructField("bit_rate_kbps", DoubleType(), True),
            StructField("channels", IntegerType(), True),
            StructField("bits_per_sample", IntegerType(), True),
            StructField("average_volume_db", DoubleType(), True),
            StructField("peak_volume_db", DoubleType(), True),
            StructField("is_silent", BooleanType(), True),
            StructField("silence_percentage", DoubleType(), True),
            StructField("is_corrupted", BooleanType(), False),
            StructField("error_message", StringType(), True)
        ])
        
        active_spark = SparkSession.getActiveSession()
        if active_spark is None:
            raise RuntimeError("No active Spark session found")
        
        # Create Spark DataFrame DIRECTLY from Python list with explicit schema
        df = active_spark.createDataFrame(results, schema=schema)
        
        # Add transformation flags
        df = df.withColumn("is_format_converted", lit(bool(transforms.get("formatConversion"))))
        if transforms.get("formatConversion"):
            df = df.withColumn("target_format", lit(transforms.get("formatConversion")))
        
        df = df.withColumn("is_normalized", lit(transforms.get("normalizeVolume", False)))
        df = df.withColumn("is_channel_converted", lit(bool(transforms.get("channelConfig"))))
        
        if transforms.get("channelConfig"):
            target_channels = 1 if transforms.get("channelConfig") == "mono" else 2
            df = df.withColumn("target_channels", lit(target_channels))
        
        # Add processing metadata
        df = df.withColumn("processed_at", current_timestamp())
        df = df.withColumn("processing_status", 
                          when(col("is_corrupted"), lit("failed")).otherwise(lit("success")))
        
        # Add storage references
        df = df.withColumn("bronze_path", col("path"))
        df = df.withColumn("silver_path", lit(None).cast(StringType()))
        
        logger.info("✅ Audio metadata extraction completed")
        return df
    
    def _apply_text_transforms(self, df: DataFrame, transforms: Dict[str, Any]) -> DataFrame:
        """Apply text file transformations with NLP analysis"""
        logger.info("📄 Applying text transformations with NLP analysis...")
        logger.info("   Collecting files to driver for processing...")
        
        # Extract pure Python primitives to avoid Spark object references
        files_data = []
        for row in df.collect():
            files_data.append({
                'path': str(row.path),
                'file_name': str(row.file_name),
                'file_extension': str(row.file_extension),
                'file_size_bytes': int(row.file_size_bytes),
                'content': bytes(row.content)
            })
        logger.info(f"   Processing {len(files_data)} text files locally...")
        
        results = []
        for row in files_data:
            try:
                from langdetect import detect
                from textblob import TextBlob
                from collections import Counter
                import re
                
                # Decode text content
                try:
                    text = row['content'].decode('utf-8')
                    encoding = 'UTF-8'
                except:
                    try:
                        text = row['content'].decode('latin-1')
                        encoding = 'latin-1'
                    except:
                        text = str(row['content'])
                        encoding = 'unknown'
                
                # Basic text metrics
                line_count = len(text.split('\n'))
                words = re.findall(r'\w+', text)
                word_count = len(words)
                char_count = len(text)
                
                # Store full content if small
                max_size = 1024 * 1024  # 1MB
                text_content = text if len(text) < max_size else None
                text_preview = text[:1000] if len(text) > 1000 else text
                
                # Language detection
                try:
                    detected_language = detect(text[:1000]) if text else None
                except:
                    detected_language = None
                
                # Sentiment analysis
                try:
                    blob = TextBlob(text[:5000])
                    sentiment_score = blob.sentiment.polarity
                    
                    if sentiment_score > 0.1:
                        sentiment_label = "positive"
                    elif sentiment_score < -0.1:
                        sentiment_label = "negative"
                    else:
                        sentiment_label = "neutral"
                except:
                    sentiment_score = None
                    sentiment_label = None
                
                # Extract keywords
                try:
                    stopwords = {'the', 'a', 'an', 'and', 'or', 'but', 'in', 'on', 'at', 'to', 'for'}
                    filtered_words = [w.lower() for w in words if w.lower() not in stopwords and len(w) > 3]
                    word_freq = Counter(filtered_words)
                    keywords = [word for word, _ in word_freq.most_common(10)]
                except:
                    keywords = []
                
                results.append((
                    str(row['path']),
                    str(row['file_name']),
                    str(row['file_extension']),
                    int(row['file_size_bytes']),
                    str(encoding),
                    int(line_count),
                    int(word_count),
                    int(char_count),
                    str(text_content) if text_content else None,
                    str(text_preview),
                    str(detected_language) if detected_language else None,
                    float(sentiment_score) if sentiment_score is not None else None,
                    str(sentiment_label) if sentiment_label else None,
                    [str(k) for k in keywords],
                    False,
                    None
                ))
                
            except Exception as e:
                logger.warning(f"   Failed to process {row.get('file_name', 'unknown')}: {e}")
                results.append((
                    str(row['path']),
                    str(row['file_name']),
                    str(row['file_extension']),
                    int(row['file_size_bytes']),
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    [],
                    True,
                    str(e)
                ))
        
        # Use pandas as intermediary to avoid Spark serialization issues
        import pandas as pd
        
        columns = [
            'path', 'file_name', 'file_extension', 'file_size_bytes',
            'encoding', 'line_count', 'word_count', 'char_count',
            'text_content', 'text_preview', 'detected_language',
            'sentiment_score', 'sentiment_label', 'keywords',
            'is_corrupted', 'error_message'
        ]
        
        pdf = pd.DataFrame(results, columns=columns)
        
        # Explicitly set dtypes to help Spark infer schema correctly
        # Note: keywords is a list column, keep as object
        pdf = pdf.astype({
            'path': 'string',
            'file_name': 'string',
            'file_extension': 'string',
            'file_size_bytes': 'Int64',
            'encoding': 'string',
            'line_count': 'Int32',
            'word_count': 'Int32',
            'char_count': 'Int32',
            'text_content': 'string',
            'text_preview': 'string',
            'detected_language': 'string',
            'sentiment_score': 'float64',
            'sentiment_label': 'string',
            'is_corrupted': 'bool',
            'error_message': 'string'
        })
        # keywords column stays as object (list of strings)
        
        active_spark = SparkSession.getActiveSession()
        if active_spark is None:
            raise RuntimeError("No active Spark session found")
        
        df = active_spark.createDataFrame(pdf)
        
        # Add processing metadata
        df = df.withColumn("processed_at", current_timestamp())
        df = df.withColumn("processing_status", 
                          when(col("is_corrupted"), lit("failed")).otherwise(lit("success")))
        
        # Add storage reference
        df = df.withColumn("bronze_path", col("path"))
        
        logger.info("✅ Text analysis completed")
        return df
    
    def _apply_pdf_transforms(self, df: DataFrame, transforms: Dict[str, Any]) -> DataFrame:
        """Apply PDF transformations with text extraction"""
        logger.info("📑 Applying PDF transformations with text extraction...")
        logger.info("   Collecting files to driver for processing...")
        
        # Extract pure Python primitives to avoid Spark object references
        files_data = []
        for row in df.collect():
            files_data.append({
                'path': str(row.path),
                'file_name': str(row.file_name),
                'file_extension': str(row.file_extension),
                'file_size_bytes': int(row.file_size_bytes),
                'content': bytes(row.content)
            })
        logger.info(f"   Processing {len(files_data)} PDF files locally...")
        
        results = []
        for row in files_data:
            try:
                import pdfplumber
                from langdetect import detect
                from collections import Counter
                import re
                
                # Load PDF from bytes
                pdf_file = io.BytesIO(row['content'])
                
                with pdfplumber.open(pdf_file) as pdf:
                    # Extract metadata
                    metadata = pdf.metadata or {}
                    page_count = len(pdf.pages)
                    author = metadata.get('/Author', None)
                    creator = metadata.get('/Creator', None)
                    pdf_version = f"PDF {metadata.get('/Version', 'Unknown')}"
                    
                    # Extract text from all pages
                    full_text = ""
                    for page in pdf.pages:
                        page_text = page.extract_text() or ""
                        full_text += page_text + "\n"
                    
                    # Check for images
                    has_images = any(len(page.images) > 0 for page in pdf.pages)
                    
                    # Check for tables
                    has_tables = any(page.extract_tables() for page in pdf.pages)
                
                # Text analysis
                text_preview = full_text[:1000] if len(full_text) > 1000 else full_text
                words = re.findall(r'\w+', full_text)
                word_count = len(words)
                
                # Language detection
                try:
                    detected_language = detect(full_text[:1000]) if full_text else None
                except:
                    detected_language = None
                
                # Extract keywords
                try:
                    stopwords = {'the', 'a', 'an', 'and', 'or', 'but', 'in', 'on', 'at', 'to', 'for'}
                    filtered_words = [w.lower() for w in words if w.lower() not in stopwords and len(w) > 3]
                    word_freq = Counter(filtered_words)
                    keywords = [word for word, _ in word_freq.most_common(10)]
                except:
                    keywords = []
                
                results.append((
                    str(row['path']),
                    str(row['file_name']),
                    str(row['file_extension']),
                    int(row['file_size_bytes']),
                    int(page_count),
                    str(author) if author else None,
                    str(creator) if creator else None,
                    str(pdf_version),
                    str(full_text) if (full_text and len(full_text) < 1024*1024) else None,
                    str(text_preview),
                    bool(has_images),
                    bool(has_tables),
                    int(word_count),
                    str(detected_language) if detected_language else None,
                    [str(k) for k in keywords],
                    False,
                    False,
                    None
                ))
                
            except Exception as e:
                logger.warning(f"   Failed to process {row.get('file_name', 'unknown')}: {e}")
                results.append((
                    str(row['path']),
                    str(row['file_name']),
                    str(row['file_extension']),
                    int(row['file_size_bytes']),
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    [],
                    True,
                    False,
                    str(e)
                ))
        
        # Use pandas as intermediary to avoid Spark serialization issues
        import pandas as pd
        
        columns = [
            'path', 'file_name', 'file_extension', 'file_size_bytes',
            'page_count', 'author', 'creator', 'pdf_version',
            'extracted_text', 'text_preview', 'has_images', 'has_tables',
            'word_count', 'detected_language', 'keywords',
            'is_corrupted', 'ocr_applied', 'error_message'
        ]
        
        pandas_df = pd.DataFrame(results, columns=columns)
        
        # Explicitly set dtypes to help Spark infer schema correctly
        # Note: keywords is a list column, keep as object
        pandas_df = pandas_df.astype({
            'path': 'string',
            'file_name': 'string',
            'file_extension': 'string',
            'file_size_bytes': 'Int64',
            'page_count': 'Int32',
            'author': 'string',
            'creator': 'string',
            'pdf_version': 'string',
            'extracted_text': 'string',
            'text_preview': 'string',
            'has_images': 'bool',
            'has_tables': 'bool',
            'word_count': 'Int32',
            'detected_language': 'string',
            'is_corrupted': 'bool',
            'ocr_applied': 'bool',
            'error_message': 'string'
        })
        # keywords column stays as object (list of strings)
        
        active_spark = SparkSession.getActiveSession()
        if active_spark is None:
            raise RuntimeError("No active Spark session found")
        
        df = active_spark.createDataFrame(pandas_df)
        
        # Add processing metadata
        df = df.withColumn("processed_at", current_timestamp())
        df = df.withColumn("processing_status", 
                          when(col("is_corrupted"), lit("failed")).otherwise(lit("success")))
        
        # Add storage reference
        df = df.withColumn("bronze_path", col("path"))
        
        logger.info("✅ PDF analysis completed")
        return df
    
    def _write_unstructured_to_silver(
        self,
        df: DataFrame,
        source: str,
        entity: str
    ) -> str:
        """Write unstructured file metadata to Silver catalog"""
        table_name = f"{self.catalog_name}.{source}.{entity}_files"
        
        logger.info(f"💾 Writing unstructured metadata to Silver: {table_name}")
        
        try:
            # Ensure namespace exists
            namespace = f"{self.catalog_name}.{source}"
            try:
                self.spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {namespace}")
            except Exception as e:
                logger.warning(f"Could not create namespace {namespace}: {e}")
            
            # DataFrame is already cached and has content column dropped
            row_count = df.count()
            logger.info(f"   Writing {row_count} rows to Iceberg table")
            
            # Use write() instead of writeTo() for better schema compatibility
            df.write \
                .format("iceberg") \
                .mode("overwrite") \
                .option("write.format.default", "parquet") \
                .saveAsTable(table_name)
            
            logger.info(f"   ✅ Created Iceberg metadata catalog: {table_name}")
            return table_name
            
        except Exception as e:
            logger.error(f"Failed to write unstructured metadata: {e}")
            raise
            raise
    
    def _read_bronze(
        self,
        source: str,
        entity: str,
        domain: str,
        execution_mode: str,
        watermark_column: Optional[str],
        watermark_value: Optional[str]
    ) -> DataFrame:
        """Read data from Bronze layer"""
        # MinIO path structure: syniqai-bronze/{domain}/{source_type}/{entity}/
        # source = source_type (postgres, s3, mariadb, etc.)
        bronze_path = f"s3a://syniqai-bronze/{domain}/{source}/{entity}/"
        
        logger.info(f"📖 Reading from Bronze: {bronze_path}")
        
        try:
            # Use pathGlobFilter to only read .parquet files, exclude _metadata.json
            df = self.spark.read \
                .option("pathGlobFilter", "*.parquet") \
                .option("recursiveFileLookup", "true") \
                .parquet(bronze_path)
            
            # Apply incremental filter if specified
            if execution_mode == "incremental" and watermark_column and watermark_value:
                logger.info(f"   Filtering: {watermark_column} > {watermark_value}")
                df = df.filter(col(watermark_column) > watermark_value)
            
            return df
            
        except Exception as e:
            logger.error(f"Failed to read Bronze data: {e}")
            raise
    
    def _apply_quality_rules(
        self,
        df: DataFrame,
        source: str,
        entity: str,
        rules: List[Dict[str, Any]]
    ) -> Tuple[DataFrame, Optional[DataFrame], Dict[str, Any]]:
        """
        Apply data quality rules and separate quarantine records
        
        Returns:
            (clean_df, quarantine_df, metrics)
        """
        initial_count = df.count()
        quality_metrics = {
            "completeness": 100.0,
            "conformity": 100.0,
            "uniqueness": 100.0,
            "validity": 100.0,
            "rules_passed": 0,
            "rules_failed": 0
        }
        
        if not rules:
            logger.info("   No quality rules specified, skipping validation")
            return df, None, quality_metrics
        
        logger.info(f"🔍 Applying {len(rules)} quality rules...")
        
        # Add quality flags column
        df = df.withColumn("_quality_flags", lit(""))
        df = df.withColumn("_should_quarantine", lit(False))
        
        for rule in rules:
            rule_id = rule.get('id') or rule.get('rule_id')
            rule_name = rule.get('name') or rule.get('rule_name')
            rule_type = rule.get('type') or rule.get('rule_type')
            condition = rule.get('condition') or rule.get('condition_expression')
            action = rule.get('action', 'pass')
            
            logger.info(f"   Applying rule: {rule_name} ({rule_type})")
            
            try:
                # Evaluate rule condition
                if rule_type == 'not_null':
                    column_name = rule.get('column') or rule.get('target_column')
                    failed_condition = col(column_name).isNull()
                    
                elif rule_type == 'valid_range':
                    column_name = rule.get('column') or rule.get('target_column')
                    min_val = rule.get('min_value', float('-inf'))
                    max_val = rule.get('max_value', float('inf'))
                    failed_condition = ~((col(column_name) >= min_val) & (col(column_name) <= max_val))
                    
                elif rule_type == 'regex_match':
                    column_name = rule.get('column') or rule.get('target_column')
                    pattern = rule.get('pattern')
                    failed_condition = ~col(column_name).rlike(pattern)
                    
                elif rule_type == 'custom_sql':
                    # Custom SQL condition (must return boolean)
                    failed_condition = ~self.spark.sql(condition)
                    
                else:
                    logger.warning(f"   Unknown rule type: {rule_type}, skipping")
                    continue
                
                # Mark rows that fail this rule
                df = df.withColumn(
                    "_quality_flags",
                    when(failed_condition, concat_ws(",", col("_quality_flags"), lit(rule_id)))
                    .otherwise(col("_quality_flags"))
                )
                
                # Mark for quarantine if action is 'quarantine' or 'reject'
                if action in ['quarantine', 'reject']:
                    df = df.withColumn(
                        "_should_quarantine",
                        when(failed_condition, lit(True)).otherwise(col("_should_quarantine"))
                    )
                    
                # Update metrics
                failed_count = df.filter(failed_condition).count()
                if failed_count > 0:
                    quality_metrics['rules_failed'] += 1
                else:
                    quality_metrics['rules_passed'] += 1
                    
            except Exception as e:
                logger.error(f"   Failed to apply rule {rule_name}: {e}")
                continue
        
        # Separate quarantine records
        quarantine_df = df.filter(col("_should_quarantine") == True)
        clean_df = df.filter(col("_should_quarantine") == False)
        
        # Calculate quality metrics
        final_count = clean_df.count()
        quarantine_count = quarantine_df.count()
        
        if initial_count > 0:
            quality_metrics['completeness'] = (final_count / initial_count) * 100
            quality_metrics['conformity'] = ((initial_count - quarantine_count) / initial_count) * 100
        
        logger.info(f"   ✅ Quality rules applied: {quality_metrics['rules_passed']} passed, {quality_metrics['rules_failed']} failed")
        
        return clean_df, quarantine_df, quality_metrics
    
    def _apply_custom_sql(
        self,
        df: DataFrame,
        custom_sql: str,
        source: str,
        entity: str
    ) -> DataFrame:
        """Apply custom SQL transformation"""
        logger.info("🔧 Applying custom SQL transformation...")
        
        try:
            # Register DataFrame as temp view
            temp_table = f"{source}_{entity}_temp"
            df.createOrReplaceTempView(temp_table)
            
            # Execute custom SQL
            result_df = self.spark.sql(custom_sql)
            
            logger.info(f"   ✅ Custom SQL applied successfully")
            return result_df
            
        except Exception as e:
            logger.error(f"Custom SQL failed: {e}")
            logger.warning("Continuing with original DataFrame")
            return df
    
    def _remove_duplicates(self, df: DataFrame) -> Tuple[DataFrame, int]:
        """Remove duplicate rows"""
        initial_count = df.count()
        
        # Remove duplicates (keep first occurrence)
        dedup_df = df.dropDuplicates()
        
        final_count = dedup_df.count()
        duplicates_removed = initial_count - final_count
        
        if duplicates_removed > 0:
            logger.info(f"♻️  Removed {duplicates_removed:,} duplicate rows")
        
        return dedup_df, duplicates_removed
    
    def _standardize_data(self, df: DataFrame) -> DataFrame:
        """Standardize data types and formats"""
        logger.info("🔄 Standardizing data formats...")
        
        # Trim string columns
        string_columns = [field.name for field in df.schema.fields if isinstance(field.dataType, StringType)]
        for col_name in string_columns:
            df = df.withColumn(col_name, trim(col(col_name)))
        
        # Handle nulls in numeric columns (fill with 0 or appropriate default)
        numeric_types = (IntegerType, DoubleType)
        numeric_columns = [field.name for field in df.schema.fields if isinstance(field.dataType, numeric_types)]
        for col_name in numeric_columns:
            df = df.withColumn(col_name, coalesce(col(col_name), lit(0)))
        
        # Parse and standardize timestamp columns
        timestamp_columns = [field.name for field in df.schema.fields 
                            if 'date' in field.name.lower() or 'time' in field.name.lower()]
        for col_name in timestamp_columns:
            if col_name in [f.name for f in df.schema.fields]:
                try:
                    df = df.withColumn(col_name, to_timestamp(col(col_name)))
                except:
                    pass
        
        return df
    
    def _add_metadata_columns(
        self,
        df: DataFrame,
        source: str,
        entity: str,
        job_id: str
    ) -> DataFrame:
        """Add metadata columns for tracking"""
        return df \
            .withColumn("_source_system", lit(source)) \
            .withColumn("_entity_name", lit(entity)) \
            .withColumn("_ingestion_timestamp", current_timestamp()) \
            .withColumn("_job_id", lit(job_id)) \
            .withColumn("_record_hash", md5(concat_ws("||", *df.columns)))
    
    def _write_to_silver(
        self,
        df: DataFrame,
        source: str,
        entity: str,
        execution_mode: str,
        watermark_column: Optional[str]
    ) -> str:
        """Write data to Silver as Iceberg table"""
        table_name = f"{self.catalog_name}.{source}.{entity}"
        
        logger.info(f"💾 Writing to Silver: {table_name}")
        
        try:
            # Ensure namespace (database) exists
            namespace = f"{self.catalog_name}.{source}"
            try:
                self.spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {namespace}")
                logger.info(f"   ✅ Namespace ensured: {namespace}")
            except Exception as e:
                logger.warning(f"Could not create namespace {namespace}: {e}")
            
            if execution_mode == "full":
                # Full refresh: createOrReplace
                df.writeTo(table_name) \
                    .using("iceberg") \
                    .tableProperty("write.format.default", "parquet") \
                    .tableProperty("write.parquet.compression-codec", "snappy") \
                    .createOrReplace()
                
                logger.info(f"   ✅ Created/replaced Iceberg table")
                
            else:
                # Incremental: append or merge
                df.writeTo(table_name) \
                    .using("iceberg") \
                    .append()
                
                logger.info(f"   ✅ Appended to Iceberg table")
            
            return table_name
            
        except Exception as e:
            logger.error(f"Failed to write to Silver: {e}")
            raise
    
    def _write_quarantine(
        self,
        quarantine_df: DataFrame,
        source: str,
        entity: str,
        job_id: str
    ):
        """Write quarantined records to separate location"""
        quarantine_path = f"s3a://{self.quarantine_bucket}/quarantine/{source}/{entity}/{job_id}/"
        
        logger.info(f"🚨 Writing quarantine data: {quarantine_path}")
        
        try:
            # Add quarantine metadata
            quarantine_df = quarantine_df \
                .withColumn("_quarantine_timestamp", current_timestamp()) \
                .withColumn("_quarantine_job_id", lit(job_id))
            
            # Write as Parquet
            quarantine_df.write \
                .mode("overwrite") \
                .parquet(quarantine_path)
            
            count = quarantine_df.count()
            logger.info(f"   ✅ Wrote {count:,} quarantine records")
            
        except Exception as e:
            logger.error(f"Failed to write quarantine data: {e}")
    
    def _get_max_watermark(self, df: DataFrame, watermark_column: str) -> Optional[str]:
        """Get maximum watermark value from DataFrame"""
        try:
            max_val = df.agg({watermark_column: "max"}).collect()[0][0]
            return str(max_val) if max_val else None
        except:
            return None
    
    def query_silver_table(
        self,
        source: str,
        entity: str,
        limit: int = 100,
        filters: Dict[str, Any] = None
    ) -> List[Dict[str, Any]]:
        """
        Query Silver Iceberg table
        
        Args:
            source: Source system
            entity: Entity name
            limit: Maximum rows to return
            filters: Optional filters to apply
        
        Returns:
            List of records as dictionaries
        """
        table_name = f"{self.catalog_name}.{source}.{entity}"
        
        try:
            df = self.spark.table(table_name)
            
            # Apply filters
            if filters:
                for col_name, value in filters.items():
                    df = df.filter(col(col_name) == value)
            
            # Limit and collect
            records = df.limit(limit).toPandas().to_dict('records')
            return records
            
        except Exception as e:
            logger.error(f"Failed to query Silver table: {e}")
            return []
    
    def get_table_stats(self, source: str, entity: str) -> Dict[str, Any]:
        """Get statistics for Silver table"""
        table_name = f"{self.catalog_name}.{source}.{entity}"
        
        try:
            df = self.spark.table(table_name)
            
            stats = {
                "row_count": df.count(),
                "column_count": len(df.columns),
                "columns": df.columns,
                "schema": str(df.schema)
            }
            
            return stats
            
        except Exception as e:
            logger.error(f"Failed to get table stats: {e}")
            return {}
    
    def cleanup(self):
        """Cleanup resources"""
        if self.spark:
            self.spark.stop()
            logger.info("🛑 Spark session stopped")


# Convenience function for direct usage
def transform_to_silver_spark(
    source: str,
    entity: str,
    domain: str = "general",
    execution_mode: str = "full",
    rules: List[Dict[str, Any]] = None,
    **kwargs
) -> Dict[str, Any]:
    """
    Convenience function to transform Bronze → Silver using Spark
    
    Args:
        source: Source system name
        entity: Entity/table name
        domain: Domain for rule filtering
        execution_mode: 'full' or 'incremental'
        rules: List of quality rules
        **kwargs: Additional arguments (watermark_column, watermark_value, custom_sql)
    
    Returns:
        Transformation result dictionary
    """
    transformer = SilverTransformerSpark()
    
    try:
        result = transformer.transform(
            source=source,
            entity=entity,
            domain=domain,
            execution_mode=execution_mode,
            rules=rules,
            **kwargs
        )
        return result
    finally:
        transformer.cleanup()


if __name__ == "__main__":
    # Test the Spark transformer
    logger.info("🧪 Testing SilverTransformerSpark...")
    
    transformer = SilverTransformerSpark()
    
    # Example: Transform a Bronze table
    result = transformer.transform(
        source="postgres",
        entity="customers",
        domain="finance",
        execution_mode="full",
        rules=[
            {
                "id": "COMP-001",
                "name": "Customer ID Not Null",
                "type": "not_null",
                "column": "customer_id",
                "action": "quarantine"
            }
        ]
    )
    
    print(json.dumps(result, indent=2))
    
    transformer.cleanup()
