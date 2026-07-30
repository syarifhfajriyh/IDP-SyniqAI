"""
Silver Layer Processing Service
Orchestrates data transformation from Bronze to Silver
"""

import sys
from pathlib import Path
import io
import pandas as pd
import logging
from datetime import datetime
from typing import Dict, Optional
import traceback

# Add data lakehouse path for SilverTransformer
lakehouse_path = Path(__file__).parent.parent.parent / "data lakehouse" / "syniq_project"
sys.path.insert(0, str(lakehouse_path))

try:
    from silver_transformer import SilverTransformer
    from silver_quality_gate import SilverQualityGate
except ImportError as e:
    logging.warning(f"Could not import silver modules: {e}")
    SilverTransformer = None
    SilverQualityGate = None

from minio_utils import MinIOClient
from silver_job_tracker import SilverJobTracker

logger = logging.getLogger(__name__)

class SilverProcessingService:
    """Service for processing Bronze data into Silver layer"""
    
    def __init__(self, minio_client: MinIOClient, job_tracker: SilverJobTracker):
        """
        Initialize the service.
        
        Args:
            minio_client: MinIO client for data operations
            job_tracker: In-memory job tracker
        """
        self.minio = minio_client
        self.job_tracker = job_tracker
        
        # Note: Buckets are ensured during storage initialization
        # No need to check again here
        
        # Initialize transformer if available
        config_path = lakehouse_path / "config" / "cleaning_rules.yaml"
        if SilverTransformer and config_path.exists():
            self.transformer = SilverTransformer(str(config_path))
            logger.info("Initialized SilverTransformer with config")
        else:
            self.transformer = None
            logger.warning("SilverTransformer not available - using basic transformations")
    
    def process_to_silver(self, source: str, entity: str, 
                         source_type: str = "postgres") -> str:
        """
        Process a Bronze table to Silver layer.
        
        Args:
            source: Source system name (e.g., 'postgres')
            entity: Entity/table name
            source_type: Type of source (postgres, mongodb, s3, etc.)
        
        Returns:
            job_id for tracking the processing
        """
        # Generate job ID
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        job_id = f"silver_{source}_{entity}_{timestamp}"
        
        # Create job
        bronze_path = f"bronze/{source}/{entity}/"
        table_name = f"{source}.{entity}"
        
        self.job_tracker.create_job(
            job_id=job_id,
            table_name=table_name,
            source=source,
            entity=entity,
            bronze_path=bronze_path
        )
        
        # Process asynchronously
        import threading
        thread = threading.Thread(
            target=self._process_job,
            args=(job_id, source, entity, source_type)
        )
        thread.daemon = True
        thread.start()
        
        return job_id
    
    def _process_job(self, job_id: str, source: str, entity: str, source_type: str):
        """Background processing of a silver job"""
        try:
            # Update status
            self.job_tracker.update_status(
                job_id, "processing", 
                message="Reading Bronze data...",
                progress=10
            )
            
            # Read Bronze data
            df = self._read_bronze_data(source, entity)
            if df is None or df.empty:
                raise ValueError(f"No data found in bronze/{source}/{entity}")
            
            original_rows = len(df)
            logger.info(f"Read {original_rows} rows from Bronze")
            
            # Apply transformations
            self.job_tracker.update_status(
                job_id, "processing",
                message="Applying transformations...",
                progress=30
            )
            
            bronze_location = f"bronze/{source}/{entity}/data.parquet"
            
            if self.transformer:
                cleaned_df = self.transformer.transform(
                    df, 
                    source_system=source_type,
                    bronze_location=bronze_location
                )
                cleaning_summary = self.transformer.get_cleaning_summary()
            else:
                # Basic cleaning if transformer not available
                cleaned_df = self._basic_cleaning(df)
                cleaning_summary = {"basic_cleaning": True}
            
            # Quality checks
            self.job_tracker.update_status(
                job_id, "processing",
                message="Running quality checks...",
                progress=60
            )
            
            quality_issues = self._run_quality_checks(cleaned_df, original_rows)
            
            # Calculate quality score
            quality_score = self._calculate_quality_score(
                original_rows, len(cleaned_df), quality_issues
            )
            
            # Write to Silver
            self.job_tracker.update_status(
                job_id, "processing",
                message="Writing to Silver layer...",
                progress=80
            )
            
            silver_path = self._write_silver_data(
                cleaned_df, source, entity, quality_score, cleaning_summary
            )
            
            # Update job with results
            self.job_tracker.update_results(
                job_id=job_id,
                silver_path=silver_path,
                row_count=len(cleaned_df),
                quality_score=quality_score,
                cleaning_summary=cleaning_summary
            )
            
            self.job_tracker.update_status(
                job_id, "completed",
                message=f"Successfully processed {len(cleaned_df)} rows to Silver",
                progress=100
            )
            
            logger.info(f"Job {job_id} completed successfully")
            
        except Exception as e:
            error_msg = str(e)
            logger.error(f"Job {job_id} failed: {error_msg}")
            logger.error(traceback.format_exc())
            
            # Save to failed folder
            try:
                self._save_to_failed(job_id, source, entity, error_msg)
            except:
                pass
            
            self.job_tracker.update_status(
                job_id, "failed",
                message=f"Processing failed: {error_msg}",
                error_message=error_msg,
                progress=0
            )
    
    def _read_bronze_data(self, source: str, entity: str) -> Optional[pd.DataFrame]:
        """Read data from Bronze layer in MinIO"""
        try:
            logger.info(f"[Silver] Attempting to read Bronze data: source={source}, entity={entity}")
            df = self.minio.read_parquet("bronze", source, entity)
            if df is not None:
                logger.info(f"[Silver] Successfully read {len(df)} rows from Bronze")
            else:
                logger.warning(f"[Silver] read_parquet returned None for {source}/{entity}")
            return df
        except Exception as e:
            logger.error(f"[Silver] Error reading Bronze data for {source}/{entity}: {e}")
            import traceback
            logger.error(f"[Silver] Traceback: {traceback.format_exc()}")
            return None
    
    def _basic_cleaning(self, df: pd.DataFrame) -> pd.DataFrame:
        """Basic cleaning when transformer is not available"""
        # Remove duplicates
        df = df.drop_duplicates()
        
        # Drop rows with all nulls
        df = df.dropna(how='all')
        
        return df
    
    def _run_quality_checks(self, df: pd.DataFrame, 
                           original_rows: int) -> list:
        """Run quality checks on the transformed data"""
        issues = []
        
        try:
            if SilverQualityGate:
                qg = SilverQualityGate(df)
                
                # Run basic checks
                qg.missing_value_detection()
                qg.duplicate_detection()
                qg.data_volume_check(min_rows=max(1, original_rows // 2))
                
                issues = qg.issues
            else:
                # Basic checks
                if df.isnull().sum().sum() > 0:
                    issues.append("Missing values detected")
                if df.duplicated().sum() > 0:
                    issues.append("Duplicate rows found")
        
        except Exception as e:
            logger.error(f"Error in quality checks: {e}")
            issues.append(f"Quality check error: {str(e)}")
        
        return issues
    
    def _calculate_quality_score(self, original_rows: int, 
                                 clean_rows: int, issues: list) -> float:
        """Calculate overall quality score"""
        # Base score from data retention
        retention_rate = clean_rows / original_rows if original_rows > 0 else 0
        base_score = retention_rate * 80  # Max 80 points for retention
        
        # Deduct points for issues
        issue_penalty = min(len(issues) * 5, 20)  # Max 20 points penalty
        
        score = max(0, min(100, base_score + 20 - issue_penalty))
        return round(score, 2)
    
    def _write_silver_data(self, df: pd.DataFrame, source: str, entity: str,
                          quality_score: float, cleaning_summary: Dict) -> str:
        """Write transformed data to Silver layer in MinIO"""
        try:
            # Convert to Parquet
            parquet_buffer = io.BytesIO()
            df.to_parquet(parquet_buffer, index=False, engine='pyarrow')
            parquet_buffer.seek(0)
            
            # Upload to MinIO (use syniqai-silver bucket)
            bucket = self.minio._get_bucket_name("silver")
            object_path = f"{source}/{entity}/data.parquet"
            self.minio.client.put_object(
                bucket,
                object_path,
                parquet_buffer,
                length=len(parquet_buffer.getvalue()),
                content_type='application/x-parquet'
            )
            
            # Save metadata
            metadata = {
                "source": source,
                "entity": entity,
                "row_count": len(df),
                "column_count": len(df.columns),
                "columns": list(df.columns),
                "quality_score": quality_score,
                "cleaning_summary": cleaning_summary,
                "processed_at": datetime.now().isoformat(),
                "silver_path": f"{bucket}/{object_path}"
            }
            
            self.minio.save_metadata("silver", source, entity, metadata)
            
            logger.info(f"Wrote {len(df)} rows to {bucket}/{object_path}")
            return f"{bucket}/{object_path}"
            
        except Exception as e:
            logger.error(f"Error writing to Silver: {e}")
            raise
    
    def _save_to_failed(self, job_id: str, source: str, entity: str, 
                       error_message: str):
        """Save failed job information to quarantine bucket"""
        try:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            error_path = f"silver/{source}/{entity}/{timestamp}/error.json"
            bucket = self.minio._get_bucket_name("quarantine")
            
            error_info = {
                "job_id": job_id,
                "source": source,
                "entity": entity,
                "error": error_message,
                "timestamp": datetime.now().isoformat(),
                "bronze_path": f"bronze/{source}/{entity}/"
            }
            
            import json
            error_json = json.dumps(error_info, indent=2)
            
            self.minio.client.put_object(
                bucket,
                error_path,
                io.BytesIO(error_json.encode('utf-8')),
                len(error_json),
                content_type='application/json'
            )
            
            logger.info(f"Saved error info to {bucket}/{error_path}")
            
        except Exception as e:
            logger.error(f"Error saving to failed bucket: {e}")
    
    def get_silver_preview(self, source: str, entity: str, 
                          page: int = 1, page_size: int = 100) -> Optional[Dict]:
        """
        Get preview of Silver data with statistics.
        
        Args:
            source: Source system
            entity: Entity name
            page: Page number (1-indexed)
            page_size: Rows per page
        
        Returns:
            Dict with rows, columns, statistics
        """
        try:
            # Read from Silver
            df = self.minio.read_parquet("silver", source, entity)
            
            if df is None:
                return None
            
            total_rows = len(df)
            
            # Pagination
            start_idx = (page - 1) * page_size
            end_idx = start_idx + page_size
            page_df = df.iloc[start_idx:end_idx]
            
            # Convert to records
            rows = page_df.to_dict('records')
            
            # Column info
            columns = []
            for col in df.columns:
                null_count = df[col].isnull().sum()
                columns.append({
                    "name": col,
                    "type": str(df[col].dtype),
                    "null_count": int(null_count),
                    "null_percentage": round((null_count / total_rows) * 100, 2)
                })
            
            # Statistics for numeric columns
            statistics = {}
            numeric_cols = df.select_dtypes(include=['int64', 'float64']).columns
            for col in numeric_cols:
                statistics[col] = {
                    "min": float(df[col].min()),
                    "max": float(df[col].max()),
                    "mean": float(df[col].mean()),
                    "median": float(df[col].median())
                }
            
            # Get metadata
            metadata = self.minio.client.get_object(
                "silver", 
                f"{source}/{entity}/_metadata.json"
            )
            import json
            meta_info = json.loads(metadata.read().decode('utf-8'))
            
            return {
                "rows": rows,
                "total_rows": total_rows,
                "page": page,
                "page_size": page_size,
                "total_pages": (total_rows + page_size - 1) // page_size,
                "columns": columns,
                "statistics": statistics,
                "quality_score": meta_info.get("quality_score"),
                "processed_at": meta_info.get("processed_at")
            }
            
        except Exception as e:
            logger.error(f"Error getting Silver preview: {e}")
            return None
