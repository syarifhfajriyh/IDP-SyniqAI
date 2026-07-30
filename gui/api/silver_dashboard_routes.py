"""
Silver Dashboard & Data Catalog API Routes
REST endpoints for Silver layer metrics, system health, and data catalog
"""

from fastapi import APIRouter, HTTPException
from typing import Dict, List, Optional
import logging
from datetime import datetime, timedelta
from pathlib import Path
import psycopg2
from psycopg2.extras import RealDictCursor
import requests

logger = logging.getLogger(__name__)

router = APIRouter()

# Database connection helper
def get_db_connection():
    """Get PostgreSQL connection for metadata"""
    try:
        conn = psycopg2.connect(
            host="localhost",
            port=5432,
            database="syniqai_lakehouse",
            user="syniqai_user",
            password="syniqai_pass",
            cursor_factory=RealDictCursor
        )
        return conn
    except Exception as e:
        logger.error(f"Database connection failed: {e}")
        return None


@router.get("/dashboard/metrics")
async def get_dashboard_metrics():
    """Get overall dashboard metrics"""
    try:
        from silver_transformation_service import get_silver_service
        
        # Get Silver service
        service = get_silver_service()
        
        # Get Bronze and Silver tables
        bronze_tables = service.get_available_bronze_tables()
        silver_tables = service.get_silver_tables()
        
        # Calculate metrics from database
        conn = get_db_connection()
        stats = {
            "totalPipelines": 0,
            "activePipelines": 0,
            "totalDatasets": len(bronze_tables) + len(silver_tables),
            "dataProcessedToday": "0 GB",
            "successRate": 100.0,
            "avgExecutionTime": "0 min",
            "failedJobsToday": 0,
            "dataQualityScore": 95.0
        }
        
        if conn:
            try:
                cursor = conn.cursor()
                
                # Count total pipelines (transformation jobs)
                cursor.execute("""
                    SELECT COUNT(DISTINCT table_name) as pipeline_count
                    FROM execution_logs
                    WHERE operation = 'transform'
                """)
                result = cursor.fetchone()
                if result:
                    stats["totalPipelines"] = int(result.get('pipeline_count', 0))
                
                # Count active pipelines (last 24h)
                cursor.execute("""
                    SELECT COUNT(DISTINCT table_name) as active_count
                    FROM execution_logs
                    WHERE operation = 'transform'
                    AND executed_at > NOW() - INTERVAL '24 hours'
                """)
                result = cursor.fetchone()
                if result:
                    stats["activePipelines"] = int(result.get('active_count', 0))
                
                # Calculate success rate (last 24h)
                cursor.execute("""
                    SELECT 
                        COUNT(*) as total,
                        SUM(CASE WHEN status = 'success' THEN 1 ELSE 0 END) as success_count
                    FROM execution_logs
                    WHERE operation = 'transform'
                    AND executed_at > NOW() - INTERVAL '24 hours'
                """)
                result = cursor.fetchone()
                if result and result.get('total', 0) > 0:
                    success_count = int(result.get('success_count', 0))
                    total = int(result.get('total', 1))
                    stats["successRate"] = round((success_count / total) * 100, 1)
                    stats["failedJobsToday"] = total - success_count
                
                # Calculate average execution time
                cursor.execute("""
                    SELECT AVG(duration_seconds) as avg_duration
                    FROM execution_logs
                    WHERE operation = 'transform'
                    AND status = 'success'
                    AND executed_at > NOW() - INTERVAL '24 hours'
                """)
                result = cursor.fetchone()
                if result and result.get('avg_duration'):
                    avg_seconds = float(result.get('avg_duration', 0))
                    stats["avgExecutionTime"] = f"{avg_seconds / 60:.1f} min"
                
                # Calculate data quality score
                cursor.execute("""
                    SELECT AVG(quality_score) as avg_quality
                    FROM quality_check_results
                    WHERE checked_at > NOW() - INTERVAL '24 hours'
                """)
                result = cursor.fetchone()
                if result and result.get('avg_quality'):
                    stats["dataQualityScore"] = round(float(result.get('avg_quality', 95.0)), 1)
                
                cursor.close()
            except Exception as e:
                logger.warning(f"Failed to get stats from database: {e}")
            finally:
                conn.close()
        
        return {
            "success": True,
            "metrics": stats
        }
    except Exception as e:
        logger.error(f"Failed to get dashboard metrics: {e}")
        # Return defaults on error
        return {
            "success": False,
            "metrics": {
                "totalPipelines": 0,
                "activePipelines": 0,
                "totalDatasets": 0,
                "dataProcessedToday": "0 GB",
                "successRate": 100.0,
                "avgExecutionTime": "0 min",
                "failedJobsToday": 0,
                "dataQualityScore": 95.0
            }
        }


@router.get("/dashboard/recent-pipelines")
async def get_recent_pipelines(limit: int = 10):
    """Get recent pipeline runs"""
    try:
        conn = get_db_connection()
        pipelines = []
        
        if conn:
            try:
                cursor = conn.cursor()
                cursor.execute("""
                    SELECT 
                        table_name,
                        status,
                        duration_seconds,
                        executed_at,
                        records_processed
                    FROM execution_logs
                    WHERE operation = 'transform'
                    ORDER BY executed_at DESC
                    LIMIT %s
                """, (limit,))
                
                results = cursor.fetchall()
                for row in results:
                    # Calculate time ago
                    executed_at = row['executed_at']
                    if executed_at:
                        time_diff = datetime.now() - executed_at
                        if time_diff.seconds < 60:
                            time_ago = f"{time_diff.seconds} sec ago"
                        elif time_diff.seconds < 3600:
                            time_ago = f"{time_diff.seconds // 60} min ago"
                        else:
                            time_ago = f"{time_diff.seconds // 3600} hours ago"
                    else:
                        time_ago = "Unknown"
                    
                    duration = row['duration_seconds'] or 0
                    duration_str = f"{duration:.1f} min" if duration > 0 else "N/A"
                    
                    # Determine status and progress
                    status = row['status'] or 'unknown'
                    progress = 100 if status == 'success' else (0 if status == 'failed' else 50)
                    
                    pipelines.append({
                        "name": row['table_name'] or "Unknown Pipeline",
                        "status": status,
                        "progress": progress,
                        "lastRun": time_ago,
                        "duration": duration_str
                    })
                
                cursor.close()
            except Exception as e:
                logger.warning(f"Failed to get pipelines from database: {e}")
            finally:
                conn.close()
        
        # If no pipelines found, return mock data
        if not pipelines:
            pipelines = [
                {"name": "No pipelines executed yet", "status": "idle", "progress": 0, "lastRun": "N/A", "duration": "N/A"}
            ]
        
        return {
            "success": True,
            "pipelines": pipelines
        }
    except Exception as e:
        logger.error(f"Failed to get recent pipelines: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/dashboard/system-health")
async def get_system_health():
    """Get system component health status"""
    health = []
    
    # 1. Check PostgreSQL
    try:
        conn = psycopg2.connect(
            host="localhost",
            port=5432,
            database="syniqai_lakehouse",
            user="syniqai_user",
            password="syniqai_pass",
            connect_timeout=2
        )
        conn.close()
        health.append({"component": "Postgres", "status": "healthy", "uptime": "99.9%"})
    except:
        health.append({"component": "Postgres", "status": "down", "uptime": "0%"})
    
    # 2. Check MinIO
    try:
        resp = requests.get("http://localhost:9000/minio/health/live", timeout=2)
        status = "healthy" if resp.status_code == 200 else "degraded"
        health.append({"component": "MinIO", "status": status, "uptime": "99.8%"})
    except:
        health.append({"component": "MinIO", "status": "down", "uptime": "0%"})
    
    # 3. Check Spark (via Silver service)
    try:
        from silver_transformation_service import get_silver_service
        service = get_silver_service()
        # Try to list tables (tests Spark connection)
        service.get_available_bronze_tables()
        health.append({"component": "Spark Cluster", "status": "healthy", "uptime": "98.5%"})
    except:
        health.append({"component": "Spark Cluster", "status": "warning", "uptime": "0%"})
    
    # 4. Check Kafka
    try:
        resp = requests.get("http://localhost:9092", timeout=2)
        health.append({"component": "Kafka", "status": "healthy", "uptime": "97.2%"})
    except:
        health.append({"component": "Kafka", "status": "warning", "uptime": "0%"})
    
    return {
        "success": True,
        "components": health
    }


@router.get("/dashboard/quality-issues")
async def get_quality_issues(limit: int = 10):
    """Get recent data quality issues"""
    try:
        conn = get_db_connection()
        issues = []
        
        if conn:
            try:
                cursor = conn.cursor()
                cursor.execute("""
                    SELECT 
                        table_name as dataset,
                        rule_name as rule,
                        severity,
                        failed_records as count
                    FROM quality_check_results
                    WHERE failed_records > 0
                    ORDER BY checked_at DESC
                    LIMIT %s
                """, (limit,))
                
                results = cursor.fetchall()
                for row in results:
                    issues.append({
                        "dataset": row['dataset'] or "unknown",
                        "rule": row['rule'] or "Unknown Rule",
                        "severity": row['severity'] or "medium",
                        "count": int(row['count'] or 0)
                    })
                
                cursor.close()
            except Exception as e:
                logger.warning(f"Failed to get quality issues from database: {e}")
            finally:
                conn.close()
        
        # Return empty if no issues
        if not issues:
            issues = []
        
        return {
            "success": True,
            "issues": issues
        }
    except Exception as e:
        logger.error(f"Failed to get quality issues: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/catalog/datasets")
async def get_catalog_datasets(
    domain: Optional[str] = None,
    source: Optional[str] = None,
    search: Optional[str] = None
):
    """Get data catalog with all Bronze and Silver tables"""
    try:
        import storage
        
        # Get Bronze tables from MinIO directly (not Iceberg)
        bronze_mgr = storage.get_bronze_manager()
        domains = ["finance", "healthcare", "general", "customer", "sales", "operations"]
        
        datasets = []
        
        # Add Bronze tables from storage
        for dom in domains:
            try:
                domain_tables = bronze_mgr.list_tables(dom)
                for table in domain_tables:
                    table_name = table.get("table_name", "unknown")
                    source_type = table.get("source_type", "unknown")
                    
                    datasets.append({
                        "id": f"bronze_{dom}_{table_name}",
                        "name": table_name,
                        "domain": dom,
                        "source": source_type.capitalize(),
                        "layer": "bronze",
                        "rowCount": "Unknown",
                        "size": f"{table.get('total_size', 0) / 1024:.2f} KB",
                        "columns": 0,  # Will be populated when schema is loaded
                        "lastUpdated": table.get("last_modified", "Unknown"),
                        "updateFrequency": "Real-time (CDC)",
                        "description": f"Bronze layer table: {table_name} from {source_type}",
                        "owner": "Data Engineering",
                        "tags": ["bronze", "raw", table.get("format", "parquet")],
                        "quality": 85,
                        "fileCount": table.get("file_count", 0),
                        "isPartitioned": table.get("is_partitioned", False)
                    })
            except Exception as e:
                logger.warning(f"Error listing {dom} tables: {e}")
                continue
        
        # Add Silver tables if they exist
        try:
            objects = storage.storage_manager.list_objects("silver")
            silver_tables = {}
            for obj in objects:
                if obj["object_name"].endswith(".parquet"):
                    parts = obj["object_name"].split("/")
                    if len(parts) >= 2:
                        table_name = parts[1] if len(parts) > 1 else parts[0]
                        if table_name not in silver_tables:
                            silver_tables[table_name] = {
                                "size": 0,
                                "last_modified": obj["last_modified"]
                            }
                        silver_tables[table_name]["size"] += obj["size"]
            
            for table_name, info in silver_tables.items():
                datasets.append({
                    "id": f"silver_{table_name}",
                    "name": table_name,
                    "domain": "unknown",
                    "source": "Transformed",
                    "layer": "silver",
                    "rowCount": "Unknown",
                    "size": f"{info['size'] / 1024:.2f} KB",
                    "columns": 0,
                    "lastUpdated": info["last_modified"],
                    "updateFrequency": "On Transform",
                    "description": f"Silver layer table: {table_name} (cleaned & validated)",
                    "owner": "Data Engineering",
                    "tags": ["silver", "cleaned", "validated"],
                    "quality": 95
                })
        except Exception as e:
            logger.warning(f"Error listing Silver tables: {e}")

        # Add Gold tables (real syniqai-gold bucket contents, same list_objects
        # pattern as Silver above — this "data mart" catalog is the first place
        # Bronze/Silver/Gold are ever merged into one browsable list)
        try:
            objects = storage.storage_manager.list_objects("gold")
            gold_tables = {}
            for obj in objects:
                if obj["object_name"].endswith(".parquet"):
                    parts = obj["object_name"].split("/")
                    if len(parts) >= 2:
                        table_name = parts[1] if len(parts) > 1 else parts[0]
                        if table_name not in gold_tables:
                            gold_tables[table_name] = {
                                "size": 0,
                                "last_modified": obj["last_modified"]
                            }
                        gold_tables[table_name]["size"] += obj["size"]

            for table_name, info in gold_tables.items():
                datasets.append({
                    "id": f"gold_{table_name}",
                    "name": table_name,
                    "domain": "unknown",
                    "source": "Aggregated",
                    "layer": "gold",
                    "rowCount": "Unknown",
                    "size": f"{info['size'] / 1024:.2f} KB",
                    "columns": 0,
                    "lastUpdated": info["last_modified"],
                    "updateFrequency": "On Aggregation",
                    "description": f"Gold layer table: {table_name} (business-ready)",
                    "owner": "Data Engineering",
                    "tags": ["gold", "aggregated", "business-ready"],
                    "quality": 98
                })
        except Exception as e:
            logger.warning(f"Error listing Gold tables: {e}")

        # Add CDC-sourced Bronze tables (real Iceberg tables written by
        # spark_cdc_consumer.py under catalog "local" — invisible to Bronze.jsx,
        # which only reads the batch-parquet store above. Surfaced here so the
        # data mart is the first place CDC Bronze data is actually browsable.)
        try:
            import sys
            from pathlib import Path as _Path
            gold_layer_path = _Path(__file__).parent.parent.parent / "data lakehouse" / "syniq_project"
            sys.path.insert(0, str(gold_layer_path))
            from utils.spark_iceberg_config import create_spark_session_with_iceberg

            spark = create_spark_session_with_iceberg()
            try:
                cdc_tables = spark.sql("SHOW TABLES IN local.bronze").collect()
                for row in cdc_tables:
                    table_name = row.tableName
                    if table_name.startswith("_"):
                        continue  # skip internal tables like _cdc_errors
                    try:
                        table_df = spark.read.format("iceberg").table(f"local.bronze.{table_name}")
                        row_count = table_df.count()
                        col_count = len(table_df.columns)
                    except Exception:
                        row_count, col_count = "Unknown", 0

                    datasets.append({
                        "id": f"bronze_cdc_{table_name}",
                        "name": table_name,
                        "domain": "unknown",
                        "source": "CDC",
                        "layer": "bronze",
                        "rowCount": row_count,
                        "size": "Unknown",
                        "columns": col_count,
                        "lastUpdated": "Unknown",
                        "updateFrequency": "Real-time (CDC)",
                        "description": f"CDC Bronze table: {table_name} (Iceberg, streamed via Debezium/Kafka)",
                        "owner": "Data Engineering",
                        "tags": ["bronze", "cdc", "iceberg"],
                        "quality": 85
                    })
            finally:
                spark.stop()
        except Exception as e:
            logger.warning(f"Error listing CDC Bronze (Iceberg) tables: {e}")

        # Apply filters
        if domain and domain != 'all':
            datasets = [d for d in datasets if d['domain'] == domain]
        
        if source and source != 'all':
            datasets = [d for d in datasets if source.lower() in d['source'].lower()]
        
        if search:
            search_lower = search.lower()
            datasets = [d for d in datasets if 
                       search_lower in d['name'].lower() or 
                       search_lower in d['description'].lower()]
        
        return {
            "success": True,
            "datasets": datasets,
            "count": len(datasets)
        }
    except Exception as e:
        logger.error(f"Failed to get catalog datasets: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/catalog/domains")
async def get_catalog_domains():
    """Get available domains with dataset counts"""
    try:
        import storage
        
        bronze_mgr = storage.get_bronze_manager()
        domains_data = []
        
        domain_list = ["finance", "healthcare", "general", "customer", "sales", "operations"]
        
        for domain in domain_list:
            try:
                tables = bronze_mgr.list_tables(domain)
                count = len(tables)
                if count > 0:
                    domains_data.append({
                        "name": domain,
                        "count": count
                    })
            except Exception as e:
                logger.warning(f"Error getting {domain} count: {e}")
                continue
        
        return {
            "success": True,
            "domains": domains_data
        }
    except Exception as e:
        logger.error(f"Failed to get domains: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/catalog/sources")
async def get_catalog_sources():
    """Get available source systems"""
    # Return static source list (can be enhanced to query from metadata)
    sources = [
        "all",
        "Iceberg",
        "MariaDB",
        "PostgreSQL",
        "MySQL",
        "Oracle",
        "REST API",
        "Kafka"
    ]
    
    return {
        "success": True,
        "sources": sources
    }
