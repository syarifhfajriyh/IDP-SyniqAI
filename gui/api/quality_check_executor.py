"""
Quality Check Executor
Executes data quality rules against Bronze layer data in MinIO
Quarantines failed records to syniqai-quarantine bucket
"""
import duckdb
import json
import logging
import os
from typing import List, Dict, Any, Optional
from datetime import datetime
import time
import uuid
from pathlib import Path

from database import quarantine_repo, execution_log_repo
from minio_utils import MinIOClient

logger = logging.getLogger(__name__)


class QualityCheckExecutor:
    """Executes quality checks and manages quarantine records"""
    
    def __init__(self):
        self.minio_client = MinIOClient()
        self.quarantine_bucket = "syniqai-quarantine"
        self._ensure_quarantine_bucket()
    
    def _ensure_quarantine_bucket(self):
        """Ensure quarantine bucket exists in MinIO"""
        try:
            if not self.minio_client.client.bucket_exists(self.quarantine_bucket):
                self.minio_client.client.make_bucket(self.quarantine_bucket)
                logger.info(f"✓ Created quarantine bucket: {self.quarantine_bucket}")
        except Exception as e:
            logger.warning(f"Could not verify quarantine bucket: {e}")
    
    def configure_duckdb_s3(self, conn: duckdb.DuckDBPyConnection):
        """Configure DuckDB to connect to MinIO S3"""
        s3_endpoint = os.getenv("MINIO_ENDPOINT", "localhost:9000").replace("http://", "").replace("https://", "")
        s3_access_key = os.getenv("MINIO_ACCESS_KEY", "admin")
        s3_secret_key = os.getenv("MINIO_SECRET_KEY", "password123")

        conn.execute("INSTALL httpfs;")
        conn.execute("LOAD httpfs;")
        conn.execute(f"SET s3_endpoint='{s3_endpoint}';")
        conn.execute("SET s3_use_ssl=false;")
        conn.execute(f"SET s3_access_key_id='{s3_access_key}';")
        conn.execute(f"SET s3_secret_access_key='{s3_secret_key}';")
        conn.execute("SET s3_url_style='path';")
    
    async def execute_checks(
        self,
        table_name: str,
        domain: str,
        source: str,
        rules: List[Dict[str, Any]],
        limit: Optional[int] = None,
        execution_id: str = None
    ) -> Dict[str, Any]:
        """
        Execute all quality rules for a table
        Returns summary of execution results
        """
        start_time = time.time()
        
        if not execution_id:
            execution_id = str(uuid.uuid4())
        
        # Normalize source name
        normalized_source = source.lower().replace('sql', '')
        normalized_domain = domain.lower()
        
        # Construct S3 path to Bronze data
        s3_path = f"s3://syniqai-bronze/{normalized_domain}/{normalized_source}/{table_name}/*.parquet"
        
        logger.info(f"🔍 Starting quality check: {table_name}")
        logger.info(f"📂 Data path: {s3_path}")
        logger.info(f"📋 Rules to apply: {len(rules)}")
        
        # Initialize DuckDB connection
        conn = duckdb.connect(database=':memory:')
        self.configure_duckdb_s3(conn)
        
        try:
            # Read data from MinIO
            if limit:
                query = f"SELECT * FROM '{s3_path}' LIMIT {limit}"
            else:
                query = f"SELECT * FROM '{s3_path}'"
            
            data_df = conn.execute(query).fetchdf()
            total_rows = len(data_df)
            
            logger.info(f"✓ Loaded {total_rows} rows from Bronze layer")
            
            # Execute each rule
            results = {
                "execution_id": execution_id,
                "table_name": table_name,
                "domain": domain,
                "source": source,
                "total_rows": total_rows,
                "rules_executed": 0,
                "rules_passed": 0,
                "rules_failed": 0,
                "rows_passed": 0,
                "rows_failed": 0,
                "rows_quarantined": 0,
                "rule_results": [],
                "execution_time_ms": 0
            }
            
            passed_rows = set(range(total_rows))  # Track passing row indices
            quarantined_rows = {}  # rule_id -> list of row indices
            
            for rule in sorted(rules, key=lambda r: r.get('execution_order', 100)):
                rule_result = await self._execute_single_rule(
                    conn=conn,
                    data_df=data_df,
                    rule=rule,
                    table_name=table_name,
                    domain=domain,
                    execution_id=execution_id
                )
                
                results["rules_executed"] += 1
                results["rule_results"].append(rule_result)
                
                if rule_result["status"] == "passed":
                    results["rules_passed"] += 1
                elif rule_result["status"] == "failed":
                    results["rules_failed"] += 1
                
                # Track failing rows
                if rule_result.get("failed_row_indices"):
                    rule_id = rule["rule_id"]
                    quarantined_rows[rule_id] = rule_result["failed_row_indices"]
                    
                    # Remove from passed set
                    passed_rows -= set(rule_result["failed_row_indices"])
            
            results["rows_passed"] = len(passed_rows)
            results["rows_failed"] = total_rows - len(passed_rows)
            
            # Write quarantined records to MinIO
            if quarantined_rows:
                quarantine_count = await self._write_quarantine_records(
                    data_df=data_df,
                    quarantined_rows=quarantined_rows,
                    table_name=table_name,
                    domain=domain,
                    source=source,
                    execution_id=execution_id,
                    rules_dict={r["rule_id"]: r for r in rules}
                )
                results["rows_quarantined"] = quarantine_count
            
            execution_time_ms = int((time.time() - start_time) * 1000)
            results["execution_time_ms"] = execution_time_ms
            
            # Calculate quality score
            if total_rows > 0:
                results["quality_score"] = round((len(passed_rows) / total_rows) * 100, 2)
            else:
                results["quality_score"] = 100
            
            logger.info(f"✓ Quality check complete: {results['quality_score']}% pass rate")
            
            return results
            
        except Exception as e:
            logger.error(f"Error executing quality checks: {e}")
            return {
                "execution_id": execution_id,
                "error": str(e),
                "status": "failed"
            }
        finally:
            conn.close()
    
    async def _execute_single_rule(
        self,
        conn: duckdb.DuckDBPyConnection,
        data_df: Any,
        rule: Dict[str, Any],
        table_name: str,
        domain: str,
        execution_id: str
    ) -> Dict[str, Any]:
        """Execute a single quality rule"""
        rule_start = time.time()
        rule_id = rule["rule_id"]
        rule_name = rule["rule_name"]
        condition = rule["condition_expression"]
        action = rule.get("action", "flag")
        
        logger.info(f"  ► Executing rule: {rule_name}")
        
        try:
            # Register dataframe as table in DuckDB
            conn.register('data_table', data_df)
            
            # Build validation query (find FAILING rows)
            # The condition_expression should be the PASSING condition
            # We need to find rows that DON'T meet it
            validation_query = f"""
                SELECT * FROM data_table 
                WHERE NOT ({condition})
            """
            
            logger.info(f"    🔍 Condition: {condition}")
            logger.info(f"    🔍 Query: {validation_query}")
            
            failed_rows_df = conn.execute(validation_query).fetchdf()
            failed_count = len(failed_rows_df)
            passed_count = len(data_df) - failed_count
            
            # Debug: Log sample of data for null checks
            if 'null' in condition.lower() or 'not null' in condition.lower():
                logger.info(f"    🔍 NULL CHECK DETECTED!")
                # Log actual data to see what we're dealing with
                sample_query = f"SELECT * FROM data_table LIMIT 5"
                sample_df = conn.execute(sample_query).fetchdf()
                logger.info(f"    📊 Sample data (first 5 rows):")
                for idx, row in sample_df.iterrows():
                    logger.info(f"       Row {idx}: {dict(row)}")
                
                # Check for string "null" values
                columns_in_condition = [col.strip() for col in condition.split() if col.isupper() and col != 'IS' and col != 'NOT' and col != 'NULL' and col != 'AND' and col != 'OR']
                for col in columns_in_condition:
                    if col in data_df.columns:
                        null_count = data_df[col].isna().sum()
                        str_null_count = (data_df[col].astype(str) == 'null').sum()
                        logger.info(f"    📊 Column '{col}': {null_count} actual NULLs, {str_null_count} string 'null' values")
                        
                        if str_null_count > 0 and 'IS NOT NULL' in condition:
                            logger.warning(f"    ⚠️  WARNING: Found string 'null' values in {col}!")
                            logger.warning(f"    ⚠️  Your condition '{condition}' will PASS string 'null' (only checks for SQL NULL)")
                            logger.warning(f"    💡 Solution: Add 'AND {col} != \"null\"' to your condition")
            
            # Get indices of failed rows
            failed_row_indices = failed_rows_df.index.tolist() if failed_count > 0 else []
            
            execution_time_ms = int((time.time() - rule_start) * 1000)
            
            # Log execution to database
            log_data = {
                "rule_id": rule_id,
                "domain": domain,
                "table_name": table_name,
                "execution_status": "completed",
                "total_rows_processed": len(data_df),
                "rows_passed": passed_count,
                "rows_failed": failed_count,
                "processing_time_ms": execution_time_ms,
                "error_message": None
            }
            
            try:
                log_id = execution_log_repo.log_execution(log_data)
                logger.info(f"    ✓ Logged execution to database: {log_id}")
            except Exception as log_error:
                logger.warning(f"Could not log execution: {log_error}")
                logger.exception("Full traceback:")
            
            pass_rate = round((passed_count / len(data_df)) * 100, 2) if len(data_df) > 0 else 100
            
            logger.info(f"    ✓ {rule_name}: {passed_count}/{len(data_df)} passed ({pass_rate}%)")
            
            return {
                "rule_id": rule_id,
                "rule_name": rule_name,
                "status": "passed" if failed_count == 0 else "failed",
                "rows_processed": len(data_df),
                "rows_passed": passed_count,
                "rows_failed": failed_count,
                "pass_rate": pass_rate,
                "execution_time_ms": execution_time_ms,
                "action": action,
                "failed_row_indices": failed_row_indices
            }
            
        except Exception as e:
            logger.error(f"    ✗ Rule execution failed: {e}")
            
            # Log failed execution
            log_data = {
                "rule_id": rule_id,
                "domain": domain,
                "table_name": table_name,
                "execution_status": "failed",
                "total_rows_processed": 0,
                "rows_passed": 0,
                "rows_failed": 0,
                "processing_time_ms": int((time.time() - rule_start) * 1000),
                "error_message": str(e)
            }
            
            try:
                execution_log_repo.log_execution(log_data)
            except Exception as log_error:
                logger.warning(f"Could not log failed execution: {log_error}")
            
            return {
                "rule_id": rule_id,
                "rule_name": rule_name,
                "status": "error",
                "error": str(e),
                "rows_processed": 0,
                "rows_passed": 0,
                "rows_failed": 0
            }
    
    async def _write_quarantine_records(
        self,
        data_df: Any,
        quarantined_rows: Dict[str, List[int]],
        table_name: str,
        domain: str,
        source: str,
        execution_id: str,
        rules_dict: Dict[str, Dict[str, Any]]
    ) -> int:
        """Write failed records to quarantine bucket"""
        try:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            quarantine_path = f"{domain}/{source}/{table_name}/{execution_id}_{timestamp}"
            
            total_quarantined = 0
            
            for rule_id, row_indices in quarantined_rows.items():
                if not row_indices:
                    continue
                
                rule = rules_dict.get(rule_id, {})
                rule_name = rule.get("rule_name", "unknown")
                severity = rule.get("severity", "medium")
                
                # Extract failed rows
                failed_df = data_df.iloc[row_indices]
                
                # Write to MinIO as Parquet
                file_key = f"{quarantine_path}/rule_{rule_name.replace(' ', '_')}.parquet"
                
                # Convert to Parquet bytes
                import io
                buffer = io.BytesIO()
                failed_df.to_parquet(buffer, index=False)
                buffer.seek(0)
                
                # Upload to MinIO
                self.minio_client.client.put_object(
                    bucket_name=self.quarantine_bucket,
                    object_name=file_key,
                    data=buffer,
                    length=buffer.getbuffer().nbytes,
                    content_type='application/octet-stream'
                )
                
                logger.info(f"    ✓ Quarantined {len(row_indices)} rows to: {file_key}")
                
                # Create quarantine records in database
                for idx in row_indices:
                    row_data = data_df.iloc[idx].to_dict()
                    
                    quarantine_record = {
                        "rule_id": rule_id,
                        "domain": domain,
                        "source_table": table_name,
                        "failed_row_data": json.dumps(row_data, default=str),
                        "failure_reason": f"Failed rule: {rule_name}",
                        "severity": severity,
                        "quarantine_metadata": json.dumps({
                            "execution_id": execution_id,
                            "s3_path": f"s3://{self.quarantine_bucket}/{file_key}",
                            "source": source,
                            "original_index": int(idx)
                        })
                    }
                    
                    try:
                        quarantine_repo.create_quarantine_record(quarantine_record)
                        total_quarantined += 1
                    except Exception as db_error:
                        logger.warning(f"Could not create quarantine DB record: {db_error}")
            
            logger.info(f"✓ Quarantined {total_quarantined} total records")
            return total_quarantined
            
        except Exception as e:
            logger.error(f"Error writing quarantine records: {e}")
            return 0
