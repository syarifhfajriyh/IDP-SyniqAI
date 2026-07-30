"""
Silver Layer Transformation Engine with PySpark
Handles structured data transformations with comprehensive data quality rules
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, when, trim, lower, upper, regexp_replace, regexp_extract,
    coalesce, lit, current_timestamp, md5, concat_ws, sha2,
    length, isnan, isnull, date_format, to_date, to_timestamp,
    round as spark_round, abs as spark_abs, count, sum as spark_sum
)
from pyspark.sql.types import *
from typing import Dict, List, Optional, Any, Tuple
import re
from datetime import datetime
import logging

logger = logging.getLogger(__name__)


class DataQualityRule:
    """Data quality rule definition"""
    def __init__(self, name: str, rule_type: str, column: str, 
                 params: Optional[Dict] = None, severity: str = "error"):
        self.name = name
        self.rule_type = rule_type
        self.column = column
        self.params = params or {}
        self.severity = severity  # error, warning, info


class SilverTransformationEngine:
    """
    Enterprise-grade Silver transformation engine with:
    - Data quality validation
    - Null handling
    - Email/phone validation
    - Data normalization
    - Type casting
    - Deduplication
    - CDC support
    """
    
    def __init__(self, spark: SparkSession, 
                 minio_endpoint: str = "localhost:9000",
                 minio_access_key: str = "minioadmin",
                 minio_secret_key: str = "minioadmin"):
        self.spark = spark
        self.minio_endpoint = minio_endpoint
        self.minio_access_key = minio_access_key
        self.minio_secret_key = minio_secret_key
        
        # Configure Spark for Iceberg + MinIO
        self._configure_spark()
        
    def _configure_spark(self):
        """Configure Spark for Iceberg and S3/MinIO access"""
        spark_conf = self.spark.sparkContext.getConf()
        
        # S3A configurations for MinIO
        spark_conf.set("spark.hadoop.fs.s3a.endpoint", f"http://{self.minio_endpoint}")
        spark_conf.set("spark.hadoop.fs.s3a.access.key", self.minio_access_key)
        spark_conf.set("spark.hadoop.fs.s3a.secret.key", self.minio_secret_key)
        spark_conf.set("spark.hadoop.fs.s3a.path.style.access", "true")
        spark_conf.set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        spark_conf.set("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        
        logger.info("✓ Spark configured for Iceberg + MinIO")
    
    # ============================================================
    # DATA QUALITY RULES
    # ============================================================
    
    def apply_quality_rules(self, df: DataFrame, 
                           rules: List[DataQualityRule]) -> Tuple[DataFrame, Dict]:
        """
        Apply data quality rules and return cleaned DataFrame + quality report
        
        Returns:
            (cleaned_df, quality_report)
        """
        quality_report = {
            "total_records": df.count(),
            "rules_applied": len(rules),
            "violations": {},
            "cleaned_records": 0,
            "rejected_records": 0
        }
        
        # Add quality columns
        df = df.withColumn("_quality_score", lit(100.0))
        df = df.withColumn("_quality_flags", lit(""))
        df = df.withColumn("_is_valid", lit(True))
        
        for rule in rules:
            df, violations = self._apply_single_rule(df, rule)
            quality_report["violations"][rule.name] = violations
        
        # Calculate final stats
        valid_df = df.filter(col("_is_valid") == True)
        rejected_df = df.filter(col("_is_valid") == False)
        
        quality_report["cleaned_records"] = valid_df.count()
        quality_report["rejected_records"] = rejected_df.count()
        
        return valid_df, quality_report
    
    def _apply_single_rule(self, df: DataFrame, rule: DataQualityRule) -> Tuple[DataFrame, int]:
        """Apply a single quality rule"""
        
        if rule.rule_type == "not_null":
            return self._rule_not_null(df, rule)
        elif rule.rule_type == "email":
            return self._rule_email(df, rule)
        elif rule.rule_type == "phone":
            return self._rule_phone(df, rule)
        elif rule.rule_type == "range":
            return self._rule_range(df, rule)
        elif rule.rule_type == "length":
            return self._rule_length(df, rule)
        elif rule.rule_type == "regex":
            return self._rule_regex(df, rule)
        elif rule.rule_type == "enum":
            return self._rule_enum(df, rule)
        else:
            logger.warning(f"Unknown rule type: {rule.rule_type}")
            return df, 0
    
    def _rule_not_null(self, df: DataFrame, rule: DataQualityRule) -> Tuple[DataFrame, int]:
        """Validate non-null values"""
        violations = df.filter(col(rule.column).isNull()).count()
        
        if rule.severity == "error":
            df = df.withColumn("_is_valid", 
                              col("_is_valid") & col(rule.column).isNotNull())
        
        if violations > 0:
            df = df.withColumn("_quality_flags",
                              when(col(rule.column).isNull(),
                                   concat_ws(",", col("_quality_flags"), lit(f"NULL_{rule.column}")))
                              .otherwise(col("_quality_flags")))
            df = df.withColumn("_quality_score",
                              when(col(rule.column).isNull(), col("_quality_score") - 10)
                              .otherwise(col("_quality_score")))
        
        return df, violations
    
    def _rule_email(self, df: DataFrame, rule: DataQualityRule) -> Tuple[DataFrame, int]:
        """Validate email format"""
        email_pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
        
        df = df.withColumn(f"_{rule.column}_is_valid_email",
                          col(rule.column).rlike(email_pattern))
        
        violations = df.filter(~col(f"_{rule.column}_is_valid_email")).count()
        
        if rule.severity == "error":
            df = df.withColumn("_is_valid",
                              col("_is_valid") & col(f"_{rule.column}_is_valid_email"))
        
        if violations > 0:
            df = df.withColumn("_quality_flags",
                              when(~col(f"_{rule.column}_is_valid_email"),
                                   concat_ws(",", col("_quality_flags"), lit(f"INVALID_EMAIL_{rule.column}")))
                              .otherwise(col("_quality_flags")))
            df = df.withColumn("_quality_score",
                              when(~col(f"_{rule.column}_is_valid_email"), col("_quality_score") - 15)
                              .otherwise(col("_quality_score")))
        
        return df, violations
    
    def _rule_phone(self, df: DataFrame, rule: DataQualityRule) -> Tuple[DataFrame, int]:
        """Validate phone number format"""
        phone_pattern = r'^\+?[0-9]{10,15}$'
        
        # Clean phone numbers first
        df = df.withColumn(f"_{rule.column}_cleaned",
                          regexp_replace(col(rule.column), r'[^0-9+]', ''))
        
        df = df.withColumn(f"_{rule.column}_is_valid_phone",
                          col(f"_{rule.column}_cleaned").rlike(phone_pattern))
        
        violations = df.filter(~col(f"_{rule.column}_is_valid_phone")).count()
        
        if rule.severity == "error":
            df = df.withColumn("_is_valid",
                              col("_is_valid") & col(f"_{rule.column}_is_valid_phone"))
        
        return df, violations
    
    def _rule_range(self, df: DataFrame, rule: DataQualityRule) -> Tuple[DataFrame, int]:
        """Validate numeric range"""
        min_val = rule.params.get("min")
        max_val = rule.params.get("max")
        
        condition = lit(True)
        if min_val is not None:
            condition = condition & (col(rule.column) >= min_val)
        if max_val is not None:
            condition = condition & (col(rule.column) <= max_val)
        
        violations = df.filter(~condition).count()
        
        if rule.severity == "error":
            df = df.withColumn("_is_valid", col("_is_valid") & condition)
        
        return df, violations
    
    def _rule_length(self, df: DataFrame, rule: DataQualityRule) -> Tuple[DataFrame, int]:
        """Validate string length"""
        min_len = rule.params.get("min", 0)
        max_len = rule.params.get("max", 10000)
        
        condition = (length(col(rule.column)) >= min_len) & (length(col(rule.column)) <= max_len)
        violations = df.filter(~condition).count()
        
        if rule.severity == "error":
            df = df.withColumn("_is_valid", col("_is_valid") & condition)
        
        return df, violations
    
    def _rule_regex(self, df: DataFrame, rule: DataQualityRule) -> Tuple[DataFrame, int]:
        """Validate against regex pattern"""
        pattern = rule.params.get("pattern")
        
        condition = col(rule.column).rlike(pattern)
        violations = df.filter(~condition).count()
        
        if rule.severity == "error":
            df = df.withColumn("_is_valid", col("_is_valid") & condition)
        
        return df, violations
    
    def _rule_enum(self, df: DataFrame, rule: DataQualityRule) -> Tuple[DataFrame, int]:
        """Validate against allowed values"""
        allowed_values = rule.params.get("values", [])
        
        condition = col(rule.column).isin(allowed_values)
        violations = df.filter(~condition).count()
        
        if rule.severity == "error":
            df = df.withColumn("_is_valid", col("_is_valid") & condition)
        
        return df, violations
    
    # ============================================================
    # DATA NORMALIZATION
    # ============================================================
    
    def normalize_data(self, df: DataFrame, 
                      normalization_config: Dict[str, Dict]) -> DataFrame:
        """
        Apply data normalization transformations
        
        normalization_config example:
        {
            "email": {"type": "lowercase", "trim": True},
            "name": {"type": "titlecase", "trim": True},
            "phone": {"type": "phone_format"},
            "amount": {"type": "round", "decimals": 2}
        }
        """
        for column, config in normalization_config.items():
            if column not in df.columns:
                continue
            
            norm_type = config.get("type")
            
            if norm_type == "lowercase":
                df = df.withColumn(column, lower(col(column)))
            elif norm_type == "uppercase":
                df = df.withColumn(column, upper(col(column)))
            elif norm_type == "titlecase":
                df = df.withColumn(column, 
                                  regexp_replace(
                                      trim(col(column)),
                                      r'\b(\w)',
                                      lambda m: m.group(1).upper()
                                  ))
            elif norm_type == "trim":
                df = df.withColumn(column, trim(col(column)))
            elif norm_type == "phone_format":
                # Remove all non-numeric except +
                df = df.withColumn(column, 
                                  regexp_replace(col(column), r'[^0-9+]', ''))
            elif norm_type == "round":
                decimals = config.get("decimals", 2)
                df = df.withColumn(column, spark_round(col(column), decimals))
            
            # Apply trim if specified
            if config.get("trim", False) and norm_type != "trim":
                df = df.withColumn(column, trim(col(column)))
        
        return df
    
    # ============================================================
    # DEDUPLICATION
    # ============================================================
    
    def deduplicate(self, df: DataFrame, 
                   key_columns: List[str],
                   keep: str = "first") -> DataFrame:
        """
        Remove duplicate records based on key columns
        
        Args:
            df: Input DataFrame
            key_columns: Columns to use for deduplication
            keep: 'first' or 'last' record to keep
        """
        from pyspark.sql.window import Window
        from pyspark.sql.functions import row_number
        
        window = Window.partitionBy(key_columns).orderBy(
            col("_ingestion_timestamp").desc() if keep == "last" 
            else col("_ingestion_timestamp").asc()
        )
        
        df = df.withColumn("_row_num", row_number().over(window))
        df = df.filter(col("_row_num") == 1).drop("_row_num")
        
        return df
    
    # ============================================================
    # TYPE CASTING
    # ============================================================
    
    def cast_types(self, df: DataFrame, 
                   type_mapping: Dict[str, str]) -> DataFrame:
        """
        Cast columns to specified types
        
        type_mapping example:
        {
            "age": "integer",
            "amount": "double",
            "date": "date",
            "timestamp": "timestamp"
        }
        """
        for column, target_type in type_mapping.items():
            if column not in df.columns:
                continue
            
            if target_type == "integer":
                df = df.withColumn(column, col(column).cast(IntegerType()))
            elif target_type == "long":
                df = df.withColumn(column, col(column).cast(LongType()))
            elif target_type == "double":
                df = df.withColumn(column, col(column).cast(DoubleType()))
            elif target_type == "float":
                df = df.withColumn(column, col(column).cast(FloatType()))
            elif target_type == "string":
                df = df.withColumn(column, col(column).cast(StringType()))
            elif target_type == "date":
                df = df.withColumn(column, to_date(col(column)))
            elif target_type == "timestamp":
                df = df.withColumn(column, to_timestamp(col(column)))
            elif target_type == "boolean":
                df = df.withColumn(column, col(column).cast(BooleanType()))
        
        return df
    
    # ============================================================
    # COMPLETE TRANSFORMATION PIPELINE
    # ============================================================
    
    def transform_bronze_to_silver(self,
                                   bronze_table: str,
                                   silver_table: str,
                                   quality_rules: List[DataQualityRule],
                                   normalization_config: Optional[Dict] = None,
                                   type_mapping: Optional[Dict] = None,
                                   dedup_columns: Optional[List[str]] = None,
                                   partition_by: Optional[List[str]] = None) -> Dict:
        """
        Complete Bronze → Silver transformation pipeline
        
        Returns transformation report
        """
        start_time = datetime.now()
        
        logger.info(f"Starting transformation: {bronze_table} → {silver_table}")
        
        # 1. Read from Bronze Iceberg table
        bronze_df = self.spark.table(bronze_table)
        initial_count = bronze_df.count()
        
        logger.info(f"Read {initial_count} records from Bronze")
        
        # 2. Apply data quality rules
        cleaned_df, quality_report = self.apply_quality_rules(bronze_df, quality_rules)
        
        logger.info(f"Quality check: {quality_report['cleaned_records']} valid, "
                   f"{quality_report['rejected_records']} rejected")
        
        # 3. Apply normalization
        if normalization_config:
            cleaned_df = self.normalize_data(cleaned_df, normalization_config)
            logger.info(f"Applied normalization to {len(normalization_config)} columns")
        
        # 4. Cast types
        if type_mapping:
            cleaned_df = self.cast_types(cleaned_df, type_mapping)
            logger.info(f"Cast {len(type_mapping)} columns to target types")
        
        # 5. Deduplicate
        if dedup_columns:
            before_dedup = cleaned_df.count()
            cleaned_df = self.deduplicate(cleaned_df, dedup_columns)
            after_dedup = cleaned_df.count()
            duplicates_removed = before_dedup - after_dedup
            logger.info(f"Removed {duplicates_removed} duplicate records")
        else:
            duplicates_removed = 0
        
        # 6. Add Silver metadata
        cleaned_df = cleaned_df.withColumn("_silver_processed_at", current_timestamp())
        cleaned_df = cleaned_df.withColumn("_silver_quality_score", col("_quality_score"))
        
        # Remove internal quality columns
        cleaned_df = cleaned_df.drop("_quality_flags", "_is_valid")
        
        # 7. Write to Silver Iceberg table
        if partition_by:
            cleaned_df.writeTo(silver_table).using("iceberg").partitionedBy(*partition_by).createOrReplace()
        else:
            cleaned_df.writeTo(silver_table).using("iceberg").createOrReplace()
        
        final_count = cleaned_df.count()
        
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        
        # Build transformation report
        report = {
            "bronze_table": bronze_table,
            "silver_table": silver_table,
            "start_time": start_time.isoformat(),
            "end_time": end_time.isoformat(),
            "duration_seconds": duration,
            "initial_records": initial_count,
            "final_records": final_count,
            "rejected_records": quality_report["rejected_records"],
            "duplicates_removed": duplicates_removed,
            "quality_report": quality_report,
            "success": True
        }
        
        logger.info(f"✓ Transformation complete: {final_count} records in Silver")
        
        return report
    
    # ============================================================
    # CDC SUPPORT
    # ============================================================
    
    def transform_cdc_to_silver(self,
                               bronze_cdc_table: str,
                               silver_table: str,
                               primary_keys: List[str],
                               quality_rules: List[DataQualityRule],
                               normalization_config: Optional[Dict] = None) -> Dict:
        """
        Transform CDC data from Bronze to Silver with UPSERT logic
        
        Handles INSERT, UPDATE, DELETE operations from CDC stream
        """
        start_time = datetime.now()
        
        logger.info(f"Starting CDC transformation: {bronze_cdc_table} → {silver_table}")
        
        # Read CDC data from Bronze
        cdc_df = self.spark.table(bronze_cdc_table)
        
        # Apply quality rules
        cleaned_df, quality_report = self.apply_quality_rules(cdc_df, quality_rules)
        
        # Apply normalization
        if normalization_config:
            cleaned_df = self.normalize_data(cleaned_df, normalization_config)
        
        # Add Silver metadata
        cleaned_df = cleaned_df.withColumn("_silver_processed_at", current_timestamp())
        
        # Merge into Silver table (UPSERT)
        from pyspark.sql.functions import expr
        
        # Create temp view for merge
        cleaned_df.createOrReplaceTempView("cdc_updates")
        
        merge_keys = " AND ".join([f"target.{k} = source.{k}" for k in primary_keys])
        
        merge_query = f"""
        MERGE INTO {silver_table} target
        USING cdc_updates source
        ON {merge_keys}
        WHEN MATCHED AND source._cdc_operation = 'DELETE' THEN DELETE
        WHEN MATCHED AND source._cdc_operation IN ('UPDATE', 'INSERT') THEN UPDATE SET *
        WHEN NOT MATCHED AND source._cdc_operation IN ('INSERT', 'UPDATE') THEN INSERT *
        """
        
        self.spark.sql(merge_query)
        
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        
        report = {
            "bronze_cdc_table": bronze_cdc_table,
            "silver_table": silver_table,
            "start_time": start_time.isoformat(),
            "end_time": end_time.isoformat(),
            "duration_seconds": duration,
            "records_processed": cleaned_df.count(),
            "quality_report": quality_report,
            "success": True
        }
        
        logger.info(f"✓ CDC transformation complete")
        
        return report
