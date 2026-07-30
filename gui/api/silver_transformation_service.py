"""
Silver Transformation Service - Backend API Integration
Handles Silver layer transformations with PySpark and Iceberg
"""

import os
import sys
import logging
from pathlib import Path
from typing import Dict, List, Optional, Any
from datetime import datetime
import json

# Import job tracker for monitoring
from silver_job_tracker import job_tracker

# Fix Windows HADOOP_HOME issue - import the fix utility
try:
    from windows_hadoop_fix import setup_windows_hadoop, get_windows_spark_configs
    if os.name == 'nt':
        setup_windows_hadoop()
except ImportError:
    # Fallback to basic setup if utility not found
    if os.name == 'nt':  # Windows
        temp_hadoop = os.path.join(os.environ.get('TEMP', 'C:\\Temp'), 'hadoop')
        temp_hadoop_bin = os.path.join(temp_hadoop, 'bin')
        os.makedirs(temp_hadoop_bin, exist_ok=True)
        os.environ['HADOOP_HOME'] = temp_hadoop
        os.environ['hadoop.home.dir'] = temp_hadoop

from pyspark.sql import SparkSession

# Add project paths
PROJECT_ROOT = Path(__file__).parent.parent
LAKEHOUSE_ROOT = PROJECT_ROOT.parent / "data lakehouse" / "syniq_project"
sys.path.insert(0, str(PROJECT_ROOT))
sys.path.insert(0, str(LAKEHOUSE_ROOT))

from transformations.silver_engine import SilverTransformationEngine, DataQualityRule

logger = logging.getLogger(__name__)


class SilverTransformationService:
    """Service for managing Silver transformations via API"""
    
    def __init__(self):
        self.spark = None
        self.engine = None
        self._initialize_spark()
    
    def _initialize_spark(self):
        """Initialize Spark session with S3A support (no Iceberg catalog)"""
        try:
            logger.info("🔧 Initializing Spark session for Silver service...")
            logger.info("📦 Downloading Spark packages if needed...")
            
            builder = SparkSession.builder \
                .appName("SilverTransformationService") \
                .master("local[*]") \
                .config("spark.jars.packages", 
                       "org.apache.hadoop:hadoop-aws:3.3.4,"
                       "com.amazonaws:aws-java-sdk-bundle:1.12.262") \
                .config("spark.hadoop.fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
                .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
                .config("spark.hadoop.fs.s3.endpoint", "http://localhost:9000") \
                .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000") \
                .config("spark.hadoop.fs.s3.access.key", "admin") \
                .config("spark.hadoop.fs.s3a.access.key", "admin") \
                .config("spark.hadoop.fs.s3.secret.key", "password123") \
                .config("spark.hadoop.fs.s3a.secret.key", "password123") \
                .config("spark.hadoop.fs.s3.path.style.access", "true") \
                .config("spark.hadoop.fs.s3a.path.style.access", "true") \
                .config("spark.hadoop.fs.s3.connection.ssl.enabled", "false") \
                .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
                .config("spark.sql.adaptive.enabled", "true") \
                .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
            
            # Windows-specific: Apply comprehensive Windows fixes
            if os.name == 'nt':
                try:
                    from windows_hadoop_fix import get_windows_spark_configs
                    for key, value in get_windows_spark_configs().items():
                        builder = builder.config(key, value)
                except ImportError:
                    # Fallback to basic Windows config
                    builder = builder \
                        .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2") \
                        .config("spark.speculation", "false") \
                        .config("spark.sql.warehouse.dir", "file:///C:/tmp/spark-warehouse") \
                        .config("spark.driver.host", "localhost")
            
            self.spark = builder.getOrCreate()
            
            self.spark.sparkContext.setLogLevel("WARN")
            self.engine = SilverTransformationEngine(self.spark)
            logger.info("✅ Spark session initialized for Silver transformations")
            logger.info("✅ Hadoop AWS libraries loaded successfully")
            
        except Exception as e:
            logger.error(f"❌ Failed to initialize Spark: {e}")
            logger.error("💡 Check internet connection for Maven package downloads")
            raise
    
    def get_available_bronze_tables(self) -> List[str]:
        """List all available Bronze tables from MinIO"""
        try:
            # List parquet files in MinIO bronze bucket using Spark
            bronze_path = "s3a://syniqai-bronze/"
            
            # Try to list files in bronze bucket
            try:
                files = self.spark._jvm.org.apache.hadoop.fs.FileSystem.get(
                    self.spark._jsc.hadoopConfiguration()
                ).listStatus(
                    self.spark._jvm.org.apache.hadoop.fs.Path(bronze_path)
                )
                
                # Extract domain folders
                domains = set()
                for file_status in files:
                    if file_status.isDirectory():
                        path_str = str(file_status.getPath())
                        domain = path_str.split('/')[-1]
                        domains.add(domain)
                
                return list(domains)
            except:
                # Fallback: return known domains
                logger.info("Using default Bronze domains (MinIO listing not available)")
                return ["finance", "retail", "healthcare", "ecommerce"]
                
        except Exception as e:
            logger.error(f"Failed to list Bronze tables: {e}")
            return ["finance", "retail"]  # Default fallback
    
    def get_table_schema(self, table_name: str) -> Dict:
        """Get schema of a Bronze table"""
        try:
            df = self.spark.table(table_name)
            schema_info = []
            
            for field in df.schema.fields:
                schema_info.append({
                    "name": field.name,
                    "type": str(field.dataType),
                    "nullable": field.nullable
                })
            
            return {
                "table": table_name,
                "columns": schema_info,
                "record_count": df.count()
            }
        except Exception as e:
            logger.error(f"Failed to get schema for {table_name}: {e}")
            return {"error": str(e)}
    
    def preview_data(self, table_name: str, limit: int = 10) -> Dict:
        """Preview data from a table"""
        try:
            df = self.spark.table(table_name)
            data = df.limit(limit).toPandas().to_dict('records')
            
            return {
                "table": table_name,
                "records": data,
                "total_count": df.count()
            }
        except Exception as e:
            logger.error(f"Failed to preview {table_name}: {e}")
            return {"error": str(e)}
    
    def validate_quality_rules(self, table_name: str, 
                               rules_config: Dict) -> Dict:
        """
        Validate quality rules without applying transformation
        Returns quality report only
        """
        try:
            df = self.spark.table(table_name)
            
            # Parse rules
            rules = self._parse_quality_rules(rules_config)
            
            # Apply rules
            _, quality_report = self.engine.apply_quality_rules(df, rules)
            
            return {
                "success": True,
                "table": table_name,
                "quality_report": quality_report
            }
        except Exception as e:
            logger.error(f"Failed to validate rules: {e}")
            return {
                "success": False,
                "error": str(e)
            }
    
    def transform_table(self,
                       bronze_table: str,
                       silver_table: str,
                       transformation_config: Dict) -> Dict:
        """
        Execute Silver transformation with configuration
        
        transformation_config:
        {
            "quality_rules": [...],
            "normalization": {...},
            "type_mapping": {...},
            "dedup_columns": [...],
            "partition_by": [...]
        }
        """
        # Create tracking job
        job_id = job_tracker.create_job(
            table_name=silver_table,
            source=bronze_table.split('.')[0] if '.' in bronze_table else 'bronze',
            entity=bronze_table.split('.')[-1] if '.' in bronze_table else bronze_table
        )
        
        try:
            # Update job to running
            job_tracker.update_status(job_id, 'running', progress=10)
            
            # Parse configuration
            quality_rules = self._parse_quality_rules(
                transformation_config.get("quality_rules", [])
            )
            
            normalization_config = transformation_config.get("normalization")
            type_mapping = transformation_config.get("type_mapping")
            dedup_columns = transformation_config.get("dedup_columns")
            partition_by = transformation_config.get("partition_by")
            
            # Update progress
            job_tracker.update_status(job_id, 'running', progress=30)
            
            # Execute transformation
            report = self.engine.transform_bronze_to_silver(
                bronze_table=bronze_table,
                silver_table=silver_table,
                quality_rules=quality_rules,
                normalization_config=normalization_config,
                type_mapping=type_mapping,
                dedup_columns=dedup_columns,
                partition_by=partition_by
            )
            
            # Update job with results
            job_tracker.update_status(job_id, 'running', progress=90)
            
            # Extract metrics from report
            row_count = report.get('row_count', 0)
            quality_score = report.get('quality_score', 0.0)
            
            # Build cleaning summary from report
            cleaning_summary = {
                'rows_quarantined': report.get('rows_quarantined', 0),
                'rows_cleaned': report.get('rows_cleaned', 0),
                'rows_validated': report.get('rows_validated', row_count)
            }
            
            # Mark job as completed
            job_tracker.update_results(
                job_id=job_id,
                status='completed',
                row_count=row_count,
                quality_score=quality_score,
                cleaning_summary=cleaning_summary,
                message=f"Transformation completed successfully"
            )
            
            return {
                "success": True,
                "report": report,
                "job_id": job_id
            }
            
        except Exception as e:
            logger.error(f"Transformation failed: {e}")
            
            # Mark job as failed
            job_tracker.update_results(
                job_id=job_id,
                status='failed',
                error_message=str(e)
            )
            
            return {
                "success": False,
                "error": str(e),
                "job_id": job_id
            }
    
    def transform_cdc_table(self,
                           bronze_cdc_table: str,
                           silver_table: str,
                           primary_keys: List[str],
                           transformation_config: Dict) -> Dict:
        """Execute CDC transformation"""
        # Create tracking job
        job_id = job_tracker.create_job(
            table_name=silver_table,
            source=bronze_cdc_table.split('.')[0] if '.' in bronze_cdc_table else 'bronze_cdc',
            entity=bronze_cdc_table.split('.')[-1] if '.' in bronze_cdc_table else bronze_cdc_table
        )
        
        try:
            # Update job to running
            job_tracker.update_status(job_id, 'running', progress=10)
            
            quality_rules = self._parse_quality_rules(
                transformation_config.get("quality_rules", [])
            )
            
            normalization_config = transformation_config.get("normalization")
            
            # Update progress
            job_tracker.update_status(job_id, 'running', progress=30)
            
            report = self.engine.transform_cdc_to_silver(
                bronze_cdc_table=bronze_cdc_table,
                silver_table=silver_table,
                primary_keys=primary_keys,
                quality_rules=quality_rules,
                normalization_config=normalization_config
            )
            
            # Update job with results
            job_tracker.update_status(job_id, 'running', progress=90)
            
            # Extract metrics from report
            row_count = report.get('row_count', 0)
            quality_score = report.get('quality_score', 0.0)
            
            cleaning_summary = {
                'rows_quarantined': report.get('rows_quarantined', 0),
                'rows_cleaned': report.get('rows_cleaned', 0),
                'rows_validated': report.get('rows_validated', row_count)
            }
            
            # Mark job as completed
            job_tracker.update_results(
                job_id=job_id,
                status='completed',
                row_count=row_count,
                quality_score=quality_score,
                cleaning_summary=cleaning_summary,
                message=f"CDC transformation completed successfully"
            )
            
            return {
                "success": True,
                "report": report,
                "job_id": job_id
            }
            
        except Exception as e:
            logger.error(f"CDC transformation failed: {e}")
            
            # Mark job as failed
            job_tracker.update_results(
                job_id=job_id,
                status='failed',
                error_message=str(e)
            )
            
            return {
                "success": False,
                "error": str(e),
                "job_id": job_id
            }
    
    def _parse_quality_rules(self, rules_config: List[Dict]) -> List[DataQualityRule]:
        """Parse quality rules from configuration"""
        rules = []
        
        for rule_config in rules_config:
            rule = DataQualityRule(
                name=rule_config.get("name"),
                rule_type=rule_config.get("type"),
                column=rule_config.get("column"),
                params=rule_config.get("params", {}),
                severity=rule_config.get("severity", "error")
            )
            rules.append(rule)
        
        return rules
    
    def get_transformation_templates(self) -> Dict:
        """Get pre-defined transformation templates"""
        return {
            "user_data_cleaning": {
                "description": "Clean user data with email validation and deduplication",
                "quality_rules": [
                    {"name": "email_required", "type": "not_null", "column": "email", "severity": "error"},
                    {"name": "email_valid", "type": "email", "column": "email", "severity": "error"},
                    {"name": "name_required", "type": "not_null", "column": "name", "severity": "error"},
                    {"name": "phone_valid", "type": "phone", "column": "phone", "severity": "warning"}
                ],
                "normalization": {
                    "email": {"type": "lowercase", "trim": True},
                    "name": {"type": "titlecase", "trim": True},
                    "phone": {"type": "phone_format"}
                },
                "dedup_columns": ["email"]
            },
            "transaction_data_cleaning": {
                "description": "Clean transaction data with amount validation",
                "quality_rules": [
                    {"name": "amount_required", "type": "not_null", "column": "amount", "severity": "error"},
                    {"name": "amount_positive", "type": "range", "column": "amount", 
                     "params": {"min": 0}, "severity": "error"},
                    {"name": "transaction_id_required", "type": "not_null", "column": "transaction_id", "severity": "error"}
                ],
                "normalization": {
                    "amount": {"type": "round", "decimals": 2},
                    "currency": {"type": "uppercase", "trim": True}
                },
                "type_mapping": {
                    "amount": "double",
                    "transaction_date": "timestamp"
                },
                "dedup_columns": ["transaction_id"]
            },
            "customer_data_enrichment": {
                "description": "Enrich customer data with normalization",
                "quality_rules": [
                    {"name": "customer_id_required", "type": "not_null", "column": "customer_id", "severity": "error"},
                    {"name": "email_valid", "type": "email", "column": "email", "severity": "warning"},
                    {"name": "age_range", "type": "range", "column": "age", 
                     "params": {"min": 18, "max": 120}, "severity": "warning"}
                ],
                "normalization": {
                    "email": {"type": "lowercase", "trim": True},
                    "first_name": {"type": "titlecase", "trim": True},
                    "last_name": {"type": "titlecase", "trim": True},
                    "country": {"type": "uppercase", "trim": True}
                },
                "type_mapping": {
                    "age": "integer",
                    "registration_date": "date"
                }
            }
        }
    
    def apply_template(self, template_name: str, 
                      bronze_table: str,
                      silver_table: str) -> Dict:
        """Apply a pre-defined template"""
        templates = self.get_transformation_templates()
        
        if template_name not in templates:
            return {
                "success": False,
                "error": f"Template not found: {template_name}"
            }
        
        template = templates[template_name]
        
        return self.transform_table(
            bronze_table=bronze_table,
            silver_table=silver_table,
            transformation_config=template
        )
    
    def get_silver_tables(self) -> List[Dict]:
        """List all Silver tables with metadata from MinIO"""
        try:
            # List Silver bucket contents using S3A
            silver_path = "s3a://silver/"
            
            result = []
            try:
                # Try to read directories from MinIO
                fs = self.spark._jvm.org.apache.hadoop.fs.FileSystem.get(
                    self.spark._jsc.hadoopConfiguration()
                )
                
                # List domains in silver bucket
                silver_files = fs.listStatus(
                    self.spark._jvm.org.apache.hadoop.fs.Path(silver_path)
                )
                
                for file_status in silver_files:
                    if file_status.isDirectory():
                        path_str = str(file_status.getPath())
                        domain = path_str.split('/')[-1]
                        
                        # List tables within domain
                        domain_path = f"{silver_path}{domain}"
                        try:
                            domain_files = fs.listStatus(
                                self.spark._jvm.org.apache.hadoop.fs.Path(domain_path)
                            )
                            
                            for table_status in domain_files:
                                if table_status.isDirectory():
                                    table_name = str(table_status.getPath()).split('/')[-1]
                                    table_path = f"{domain_path}/{table_name}"
                                    
                                    # Try to get record count
                                    try:
                                        df = self.spark.read.parquet(table_path)
                                        record_count = df.count()
                                        column_count = len(df.columns)
                                    except:
                                        record_count = 0
                                        column_count = 0
                                    
                                    result.append({
                                        "table_name": table_name,
                                        "full_name": f"{domain}.{table_name}",
                                        "domain": domain,
                                        "record_count": record_count,
                                        "column_count": column_count,
                                        "path": table_path
                                    })
                        except:
                            pass
                
                return result
            except Exception as e:
                logger.warning(f"MinIO listing failed: {e}, returning mock data")
                return []
        
        except Exception as e:
            logger.error(f"Failed to list Silver tables: {e}")
            return []
    
    def stop(self):
        """Stop Spark session"""
        if self.spark:
            self.spark.stop()
            logger.info("Spark session stopped")


# Singleton instance
_service_instance = None

def get_silver_service() -> SilverTransformationService:
    """Get or create Silver transformation service instance"""
    global _service_instance
    
    if _service_instance is None:
        _service_instance = SilverTransformationService()
    
    return _service_instance
