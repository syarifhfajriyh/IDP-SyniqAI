"""
Data Quality Profiler

Generates comprehensive data quality profiles including statistics,
missing values, duplicates, outliers, and quality scores.
"""

import pandas as pd
import numpy as np
from datetime import datetime
from typing import Dict, Any
import json
from minio import Minio
from io import BytesIO
from pathlib import Path
import sys
sys.path.append(str(Path(__file__).parent.parent))
from utils.logger import get_logger

logger = get_logger(__name__)


class DataProfiler:
    """
    Generate comprehensive data quality profiles.
    
    Profiles include:
    - Row counts
    - Null percentages
    - Data types
    - Numeric statistics (min, max, mean, std)
    - Categorical value distributions
    - Duplicate detection
    - Anomaly detection
    """
    
    def __init__(self, minio_client: Minio):
        self.minio = minio_client
        self.bucket = "quality"
        self._ensure_bucket()
    
    def _ensure_bucket(self):
        """Create quality bucket if not exists"""
        try:
            if not self.minio.bucket_exists(self.bucket):
                self.minio.make_bucket(self.bucket)
                logger.info(f"✅ Created quality bucket: {self.bucket}")
        except Exception as e:
            logger.error(f"Failed to create quality bucket: {e}")
    
    def profile_dataframe(
        self,
        df: pd.DataFrame,
        source: str,
        entity: str,
        sample_size: int = 10000
    ) -> Dict[str, Any]:
        """
        Generate comprehensive profile of a DataFrame.
        
        Args:
            df: DataFrame to profile
            source: Source system name
            entity: Table/collection name
            sample_size: Max rows to analyze for distributions
        
        Returns:
            Profile dictionary
        """
        logger.info(f"🔍 Profiling {source}.{entity} ({len(df):,} rows)")
        
        # Sample if too large
        df_sample = df.sample(min(sample_size, len(df))) if len(df) > sample_size else df
        
        profile = {
            "metadata": {
                "source": source,
                "entity": entity,
                "profiled_at": datetime.utcnow().isoformat(),
                "total_rows": len(df),
                "sampled_rows": len(df_sample),
                "total_columns": len(df.columns)
            },
            "overview": self._compute_overview(df),
            "columns": self._profile_columns(df_sample),
            "quality_score": 0.0
        }
        
        # Compute overall quality score
        profile["quality_score"] = self._compute_quality_score(profile)
        
        # Save to MinIO
        self._save_profile(source, entity, profile)
        
        logger.info(f"✅ Profile generated - Quality Score: {profile['quality_score']:.2f}%")
        return profile
    
    def _compute_overview(self, df: pd.DataFrame) -> Dict:
        """Compute high-level dataset statistics"""
        memory_mb = df.memory_usage(deep=True).sum() / 1024 / 1024
        duplicates = df.duplicated().sum()
        missing_cells = df.isnull().sum().sum()
        total_cells = len(df) * len(df.columns)
        
        return {
            "row_count": int(len(df)),
            "column_count": int(len(df.columns)),
            "memory_usage_mb": float(memory_mb),
            "duplicate_rows": int(duplicates),
            "duplicate_percentage": float(duplicates / len(df) * 100) if len(df) > 0 else 0.0,
            "missing_cells": int(missing_cells),
            "missing_percentage": float(missing_cells / total_cells * 100) if total_cells > 0 else 0.0
        }
    
    def _profile_columns(self, df: pd.DataFrame) -> Dict:
        """Profile each column individually"""
        columns = {}
        
        for col in df.columns:
            try:
                col_profile = {
                    "dtype": str(df[col].dtype),
                    "missing_count": int(df[col].isnull().sum()),
                    "missing_percentage": float(df[col].isnull().sum() / len(df) * 100) if len(df) > 0 else 0.0,
                    "unique_count": int(df[col].nunique()),
                    "unique_percentage": float(df[col].nunique() / len(df) * 100) if len(df) > 0 else 0.0
                }
                
                # Numeric columns
                if pd.api.types.is_numeric_dtype(df[col]):
                    col_profile.update(self._profile_numeric_column(df[col]))
                
                # Categorical columns
                elif pd.api.types.is_string_dtype(df[col]) or pd.api.types.is_object_dtype(df[col]):
                    col_profile.update(self._profile_categorical_column(df[col]))
                
                # Datetime columns
                elif pd.api.types.is_datetime64_any_dtype(df[col]):
                    col_profile.update(self._profile_datetime_column(df[col]))
                
                columns[col] = col_profile
            
            except Exception as e:
                logger.warning(f"Failed to profile column {col}: {e}")
                columns[col] = {"error": str(e)}
        
        return columns
    
    def _profile_numeric_column(self, series: pd.Series) -> Dict:
        """Profile numeric column"""
        non_null = series.dropna()
        
        if len(non_null) == 0:
            return {}
        
        return {
            "statistics": {
                "min": float(non_null.min()),
                "max": float(non_null.max()),
                "mean": float(non_null.mean()),
                "median": float(non_null.median()),
                "std": float(non_null.std()),
                "q25": float(non_null.quantile(0.25)),
                "q75": float(non_null.quantile(0.75))
            },
            "zeros_count": int((non_null == 0).sum()),
            "negative_count": int((non_null < 0).sum()),
            "outliers": self._detect_outliers(non_null)
        }
    
    def _profile_categorical_column(self, series: pd.Series) -> Dict:
        """Profile categorical column"""
        non_null = series.dropna()
        
        if len(non_null) == 0:
            return {}
        
        value_counts = non_null.value_counts()
        str_series = non_null.astype(str)
        
        return {
            "top_values": [
                {"value": str(val), "count": int(count), "percentage": float(count / len(non_null) * 100)}
                for val, count in value_counts.head(10).items()
            ],
            "avg_length": float(str_series.str.len().mean()) if len(str_series) > 0 else 0.0,
            "min_length": int(str_series.str.len().min()) if len(str_series) > 0 else 0,
            "max_length": int(str_series.str.len().max()) if len(str_series) > 0 else 0
        }
    
    def _profile_datetime_column(self, series: pd.Series) -> Dict:
        """Profile datetime column"""
        non_null = series.dropna()
        
        if len(non_null) == 0:
            return {}
        
        return {
            "min_date": non_null.min().isoformat(),
            "max_date": non_null.max().isoformat(),
            "range_days": int((non_null.max() - non_null.min()).days)
        }
    
    def _detect_outliers(self, series: pd.Series) -> Dict:
        """Detect outliers using IQR method"""
        q1 = series.quantile(0.25)
        q3 = series.quantile(0.75)
        iqr = q3 - q1
        
        lower_bound = q1 - 1.5 * iqr
        upper_bound = q3 + 1.5 * iqr
        
        outliers = series[(series < lower_bound) | (series > upper_bound)]
        
        return {
            "count": int(len(outliers)),
            "percentage": float(len(outliers) / len(series) * 100) if len(series) > 0 else 0.0,
            "lower_bound": float(lower_bound),
            "upper_bound": float(upper_bound)
        }
    
    def _compute_quality_score(self, profile: Dict) -> float:
        """
        Compute overall data quality score (0-100).
        
        Factors:
        - Completeness (no missing values) - 40%
        - Uniqueness (no duplicates) - 30%
        - Validity (no outliers) - 30%
        """
        overview = profile["overview"]
        
        # Completeness score
        completeness = 100 - overview["missing_percentage"]
        
        # Uniqueness score
        uniqueness = 100 - overview["duplicate_percentage"]
        
        # Validity score (based on outliers)
        total_outliers = sum(
            col.get("outliers", {}).get("count", 0)
            for col in profile["columns"].values()
            if "outliers" in col
        )
        validity = 100 - (total_outliers / overview["row_count"] * 100) if overview["row_count"] > 0 else 100
        validity = max(0, validity)  # Cap at 0
        
        # Weighted score
        quality_score = (completeness * 0.4) + (uniqueness * 0.3) + (validity * 0.3)
        
        return round(quality_score, 2)
    
    def _save_profile(self, source: str, entity: str, profile: Dict):
        """Save profile to MinIO"""
        today = datetime.utcnow()
        path = f"profiles/{source}/{entity}/{today.year}/{today.month:02d}/{today.day:02d}/profile.json"
        
        data = json.dumps(profile, indent=2).encode('utf-8')
        
        try:
            self.minio.put_object(
                self.bucket,
                path,
                BytesIO(data),
                len(data),
                content_type="application/json"
            )
            logger.info(f"💾 Saved profile to: {path}")
        except Exception as e:
            logger.error(f"Failed to save profile: {e}")
    
    def get_latest_profile(self, source: str, entity: str) -> Dict | None:
        """Get most recent profile for a table"""
        prefix = f"profiles/{source}/{entity}/"
        
        try:
            objects = self.minio.list_objects(self.bucket, prefix=prefix, recursive=True)
            profiles = sorted(
                [obj.object_name for obj in objects if obj.object_name.endswith('profile.json')],
                reverse=True
            )
            
            if profiles:
                response = self.minio.get_object(self.bucket, profiles[0])
                return json.loads(response.read().decode('utf-8'))
        except Exception as e:
            logger.error(f"Failed to load profile: {e}")
        
        return None
