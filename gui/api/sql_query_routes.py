"""
SQL Query Execution Routes
REST endpoints for custom SQL query execution against Bronze layer data in MinIO
"""

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import Dict, List, Optional, Any
import logging
import traceback
from datetime import datetime
import duckdb
import pandas as pd

logger = logging.getLogger(__name__)


def configure_duckdb_s3():
    """Configure DuckDB connection with MinIO S3 settings"""
    conn = duckdb.connect(database=':memory:', read_only=False)
    
    try:
        conn.execute("""
            INSTALL httpfs;
            LOAD httpfs;
            SET s3_endpoint='localhost:9000';
            SET s3_use_ssl=false;
            SET s3_url_style='path';
            SET s3_access_key_id='admin';
            SET s3_secret_access_key='password123';
        """)
        # Create bronze schema for table organization
        conn.execute("CREATE SCHEMA IF NOT EXISTS bronze")
        logger.info("✅ DuckDB S3 configured for MinIO access")
    except Exception as e:
        logger.warning(f"⚠️ Failed to configure S3: {e}")
    
    return conn

router = APIRouter()


@router.get("/sql/ping")
async def ping():
    """Test endpoint to verify SQL routes are working"""
    print("🏓 SQL PING endpoint hit!")
    return {"status": "ok", "message": "SQL routes are active"}


class SQLQueryRequest(BaseModel):
    query: str
    limit: Optional[int] = 1000
    tables: Optional[List[Dict[str, str]]] = None  # [{"name": "bronze.finance_transactions", "path": "s3a://..."}]


class SQLQueryResponse(BaseModel):
    success: bool
    columns: List[str]
    rows: List[Dict[str, Any]]
    row_count: int
    execution_time_ms: int
    message: Optional[str] = None
    error: Optional[str] = None


class SQLValidateRequest(BaseModel):
    query: str


class SQLValidateResponse(BaseModel):
    valid: bool
    message: str
    suggestions: Optional[List[str]] = None


@router.post("/sql/execute", response_model=SQLQueryResponse)
def execute_sql_query(request: SQLQueryRequest):
    """
    Execute custom SQL query against Bronze layer data
    Uses DuckDB to query Parquet files directly from MinIO
    """
    print("\n" + "="*70)
    print("🚀 SQL QUERY EXECUTION STARTED")
    print("="*70)
    
    start_time = datetime.now()
    conn = None
    
    try:
        print(f"📊 Executing SQL query (limit: {request.limit})")
        print(f"Query: {request.query[:200]}...")
        print(f"Tables parameter: {request.tables}")
        print(f"Tables type: {type(request.tables)}")
        print(f"Tables length: {len(request.tables) if request.tables else 0}")
        
        logger.info(f"📊 Executing SQL query (limit: {request.limit})")
        logger.info(f"Query: {request.query[:200]}...")
        logger.info(f"Tables: {request.tables}")
        
        # Initialize DuckDB with S3/MinIO configuration
        print("Initializing DuckDB connection...")
        conn = configure_duckdb_s3()
        print("✅ DuckDB connection initialized")
        
        # Register tables as views
        registered_tables = []
        
        if request.tables:
            print(f"📋 Registering {len(request.tables)} user-selected tables...")
            logger.info(f"📋 Registering {len(request.tables)} user-selected tables...")
            for table_info in request.tables:
                table_name = table_info.get("name")
                s3_path = table_info.get("path")
                
                print(f"   Processing: {table_name} -> {s3_path}")
                logger.info(f"   Processing: {table_name} -> {s3_path}")
                
                if not table_name or not s3_path:
                    print(f"⚠️ Skipping invalid table info: {table_info}")
                    logger.warning(f"⚠️ Skipping invalid table info: {table_info}")
                    continue
                
                try:
                    # Convert s3a:// paths to s3:// for DuckDB
                    if s3_path.startswith("s3a://"):
                        s3_path = s3_path.replace("s3a://", "s3://")
                    
                    # Ensure path has wildcard for parquet files
                    if not s3_path.endswith(".parquet") and not s3_path.endswith("*.parquet"):
                        s3_path = s3_path.rstrip("/") + "/*.parquet"
                    
                    print(f"   Adjusted path: {s3_path}")
                    logger.info(f"   Adjusted path: {s3_path}")
                    
                    # Create view from S3 path
                    view_sql = f"CREATE OR REPLACE VIEW {table_name} AS SELECT * FROM '{s3_path}'"
                    print(f"   Executing: {view_sql}")
                    conn.execute(view_sql)
                    
                    print(f"   View created, counting rows...")
                    # Verify table was registered
                    row_count = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
                    registered_tables.append(table_name)
                    print(f"✅ Registered {table_name} ({row_count:,} rows) from {s3_path}")
                    logger.info(f"✅ Registered {table_name} ({row_count:,} rows) from {s3_path}")
                    
                except Exception as e:
                    print(f"❌ Could not register {table_name} from {s3_path}: {e}")
                    print(f"   Exception type: {type(e).__name__}")
                    logger.error(f"❌ Could not register {table_name} from {s3_path}: {e}")
                    logger.error(f"   Exception type: {type(e).__name__}")
                    logger.error(f"   Traceback: {traceback.format_exc()}")
        else:
            # Auto-discover from Bronze layer (fallback)
            logger.info("🔍 No tables specified, auto-discovering from Bronze layer...")
            logger.warning("⚠️ Auto-discovery is a fallback - tables should be passed from frontend!")
        
        if not registered_tables:
            error_msg = "⚠️ No tables could be registered"
            logger.error(error_msg)
            logger.error("💡 Make sure tables parameter is sent from frontend with valid paths")
            logger.error(f"   Received tables parameter: {request.tables}")
            
            conn.close()
            
            return SQLQueryResponse(
                success=False,
                columns=[],
                rows=[],
                row_count=0,
                execution_time_ms=int((datetime.now() - start_time).total_seconds() * 1000),
                error=error_msg,
                message="No tables available. Please select tables from Data Catalog first."
            )
        else:
            logger.info(f"✅ Registered {len(registered_tables)} tables: {', '.join(registered_tables)}")
        
        # Execute the SQL query
        logger.info(f"🔍 Executing query: {request.query}")
        result_df = conn.execute(request.query).fetchdf()
        
        # Apply limit
        limited_df = result_df.head(request.limit)
        
        # Get column names
        columns = list(limited_df.columns)
        
        # Convert to list of dicts
        rows = limited_df.to_dict('records')
        
        # Clean up timestamps and NaN values
        for row in rows:
            for key, value in row.items():
                if pd.isna(value):
                    row[key] = None
                elif isinstance(value, (pd.Timestamp, datetime)):
                    row[key] = value.isoformat()
        
        # Get total count
        total_count = len(result_df)
        
        # Calculate execution time
        execution_time = (datetime.now() - start_time).total_seconds() * 1000
        
        logger.info(f"✅ Query executed successfully: {len(rows)} rows returned in {execution_time:.0f}ms")
        
        conn.close()
        
        return SQLQueryResponse(
            success=True,
            columns=columns,
            rows=rows,
            row_count=total_count,
            execution_time_ms=int(execution_time),
            message=f"Query executed successfully. Showing {len(rows)} of {total_count} rows."
        )
        
    except Exception as e:
        execution_time = (datetime.now() - start_time).total_seconds() * 1000
        error_msg = str(e)
        error_trace = traceback.format_exc()
        
        print(f"❌ SQL query execution failed: {error_msg}")
        print(f"Traceback: {error_trace}")
        logger.error(f"❌ SQL query execution failed: {error_msg}")
        logger.error(f"Traceback: {error_trace}")
        
        # Close  connection if it was opened
        if conn:
            try:
                conn.close()
            except:
                pass
        
        return SQLQueryResponse(
            success=False,
            columns=[],
            rows=[],
            row_count=0,
            execution_time_ms=int(execution_time),
            error=error_msg,
            message="Query execution failed. Check your SQL syntax and table names."
        )


@router.post("/sql/validate", response_model=SQLValidateResponse)
def validate_sql_query(request: SQLValidateRequest):
    """
    Validate SQL query syntax without executing it
    Uses DuckDB SQL parser
    """
    try:
        logger.info("🔍 Validating SQL query")
        
        # Initialize DuckDB with S3 configuration
        conn = configure_duckdb_s3()
        
        # Try to parse the query using EXPLAIN (doesn't execute, just validates)
        try:
            # Use EXPLAIN to validate query without executing
            conn.execute(f"EXPLAIN {request.query}")
            
            conn.close()
            
            logger.info("✅ Query validation passed")
            return SQLValidateResponse(
                valid=True,
                message="Query syntax is valid"
            )
        except Exception as e:
            error_msg = str(e)
            logger.warning(f"⚠️ Query validation failed: {error_msg}")
            
            conn.close()
            
            # Provide helpful suggestions
            suggestions = []
            if "Table" in error_msg and "not found" in error_msg:
                suggestions.append("Ensure table names are correct (e.g., bronze.finance_transactions)")
                suggestions.append("Tables must be registered before running queries")
            if "column" in error_msg.lower():
                suggestions.append("Check column names - they are case-sensitive")
            if "syntax" in error_msg.lower():
                suggestions.append("Check your SQL syntax - ensure all keywords are spelled correctly")
            
            return SQLValidateResponse(
                valid=False,
                message=f"Query validation failed: {error_msg}",
                suggestions=suggestions if suggestions else None
            )
            
    except Exception as e:
        logger.error(f"❌ Validation error: {e}")
        return SQLValidateResponse(
            valid=False,
            message=f"Validation error: {str(e)}"
        )


@router.get("/sql/tables")
def list_available_tables():
    """
    List all available Bronze tables with their MinIO paths
    Returns table metadata from Data Catalog for SQL editor
    """
    try:
        logger.info("📋 Listing available Bronze tables with paths")
        
        from backend import storage
        bronze_mgr = storage.get_bronze_manager()
        
        all_tables = []
        domains = ["finance", "healthcare", "general", "crm", "sales"]
        
        for domain in domains:
            try:
                tables = bronze_mgr.list_tables(domain)
                for table in tables:
                    table_name = f"bronze.{table['table_name']}"
                    # Build S3 path from metadata (use s3:// for DuckDB, not s3a://)
                    source_type = table.get('source_type', 'postgres').lower()
                    s3_path = f"s3://syniqai-bronze/{domain}/{source_type}/{table['table_name']}/*.parquet"
                    
                    all_tables.append({
                        "name": table_name,
                        "display_name": table['table_name'],
                        "path": s3_path,
                        "domain": domain,
                        "source_type": source_type,
                        "row_count": table.get('row_count', 0),
                        "size": table.get('size', 0),
                        "last_modified": table.get('last_modified', ''),
                        "columns": table.get('columns', [])
                    })
            except Exception as e:
                logger.debug(f"⏭️  No tables in {domain}: {e}")
        
        logger.info(f"✅ Found {len(all_tables)} available tables")
        
        return {
            "success": True,
            "tables": all_tables,
            "total_tables": len(all_tables)
        }
        
    except Exception as e:
        logger.error(f"❌ Failed to list tables: {e}")
        import traceback
        traceback.print_exc()
        return {
            "success": False,
            "tables": [],
            "total_tables": 0,
            "error": str(e)
        }


@router.get("/sql/schema/{table_name}")
def get_table_schema(table_name: str):
    """
    Get schema details for a specific Bronze table using DuckDB
    """
    try:
        logger.info(f"📊 Getting schema for {table_name}")
        
        # Initialize DuckDB with S3 configuration
        conn = configure_duckdb_s3()
        
        # Map table names to paths (using correct MinIO structure with s3://)
        table_paths = {
            "bronze.finance_transactions": "s3://syniqai-bronze/finance/postgres/finance_transactions/*.parquet",
            "bronze.user_profiles": "s3://syniqai-bronze/crm/postgres/user_profiles/*.parquet",
            "bronze.clickstream_events": "s3://syniqai-bronze/clickstream/kafka/events/*.parquet",
            "bronze.customers": "s3://syniqai-bronze/crm/postgres/customers/*.parquet",
            "bronze.orders": "s3://syniqai-bronze/sales/postgres/orders/*.parquet"
        }
        
        if table_name not in table_paths:
            conn.close()
            raise HTTPException(status_code=404, detail=f"Table {table_name} not found")
        
        # Get schema using DESCRIBE
        s3_path = table_paths[table_name]
        schema_query = f"DESCRIBE SELECT * FROM '{s3_path}' LIMIT 1"
        schema_df = conn.execute(schema_query).fetchdf()
        
        # Get schema info from DESCRIBE output
        schema_info = []
        for _, row in schema_df.iterrows():
            schema_info.append({
                "name": row['column_name'],
                "type": row['column_type'],
                "nullable": row.get('null', 'YES') == 'YES'
            })
        
        # Get sample data
        sample_query = f"SELECT * FROM '{s3_path}' LIMIT 5"
        sample_df = conn.execute(sample_query).fetchdf()
        sample_rows = sample_df.to_dict('records')
        
        # Clean up NaN values
        for row in sample_rows:
            for key, value in row.items():
                if pd.isna(value):
                    row[key] = None
                elif isinstance(value, (pd.Timestamp, datetime)):
                    row[key] = value.isoformat()
        
        # Get row count
        count_query = f"SELECT COUNT(*) as cnt FROM '{s3_path}'"
        row_count = conn.execute(count_query).fetchone()[0]
        
        conn.close()
        
        return {
            "success": True,
            "table_name": table_name,
            "row_count": row_count,
            "column_count": len(schema_info),
            "schema": schema_info,
            "sample_rows": sample_rows
        }
        
    except Exception as e:
        logger.error(f"❌ Failed to get schema for {table_name}: {e}")
        raise HTTPException(status_code=500, detail=str(e))
