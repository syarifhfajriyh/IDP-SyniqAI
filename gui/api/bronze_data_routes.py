"""
Bronze Data Inspection Routes - Fetch Real Data from MinIO
============================================================

Provides endpoints to read actual Bronze layer data from MinIO Parquet files
for preview, schema, and statistics in the Data Catalog UI.
"""
from fastapi import APIRouter, HTTPException, Query
from typing import Optional, List, Dict, Any
from pydantic import BaseModel
import logging
import duckdb
import pandas as pd
from datetime import datetime

logger = logging.getLogger(__name__)

router = APIRouter()


class QueryRequest(BaseModel):
    """Request model for custom query execution"""
    query: str
    table_name: Optional[str] = None
    domain: Optional[str] = 'finance'
    source: Optional[str] = 'postgres'
    limit: int = 1000


def get_minio_table_path(domain: str, source: str, table_name: str) -> str:
    """Construct MinIO S3 path for Bronze table"""
    # Format: s3://syniqai-bronze/{domain}/{source}/{table_name}/*.parquet
    # Example: s3://syniqai-bronze/finance/postgres/finance_transactions/*.parquet
    return f"s3://syniqai-bronze/{domain.lower()}/{source.lower()}/{table_name.lower()}/*.parquet"


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
        logger.info("DuckDB S3 configured for MinIO access")
    except Exception as e:
        logger.warning(f"Failed to configure S3: {e}")
    
    return conn


@router.get("/preview-data/{table_name}")
async def get_preview_data(
    table_name: str,
    domain: Optional[str] = Query('finance', description="Domain name"),
    source: Optional[str] = Query('postgres', description="Source type"),
    limit: int = Query(100, ge=1, le=1000, description="Number of rows to fetch")
):
    """
    Get preview data (sample rows) from MinIO Bronze storage
    
    This reads actual Parquet files from MinIO and returns first N rows.
    Used by the Preview tab in Data Catalog.
    """
    try:
        conn = configure_duckdb_s3()
        table_path = get_minio_table_path(domain.lower(), source.lower(), table_name)
        
        # Read data from MinIO
        query = f"SELECT * FROM '{table_path}' LIMIT {limit}"
        logger.info(f"Fetching preview data: {query}")
        
        df = conn.execute(query).fetchdf()
        conn.close()
        
        # Convert to JSON-serializable format
        rows = df.to_dict('records')
        
        # Convert timestamps to strings
        for row in rows:
            for key, value in row.items():
                if pd.isna(value):
                    row[key] = None
                elif isinstance(value, (pd.Timestamp, datetime)):
                    row[key] = value.isoformat()
        
        return {
            'success': True,
            'table_name': table_name,
            'domain': domain,
            'source': source,
            'rows': rows,
            'row_count': len(rows),
            'columns': list(df.columns)
        }
        
    except Exception as e:
        logger.error(f"Failed to fetch preview data: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to read data from MinIO: {str(e)}"
        )


@router.get("/schema/{table_name}")
async def get_table_schema(
    table_name: str,
    domain: Optional[str] = Query('finance', description="Domain name"),
    source: Optional[str] = Query('postgres', description="Source type")
):
    """
    Get table schema from MinIO Bronze storage
    
    Reads Parquet file metadata to extract column names, types, and nullability.
    Used by the Schema tab in Data Catalog.
    """
    try:
        conn = configure_duckdb_s3()
        table_path = get_minio_table_path(domain.lower(), source.lower(), table_name)
        
        # Get schema using DESCRIBE
        query = f"DESCRIBE SELECT * FROM '{table_path}' LIMIT 1"
        logger.info(f"Fetching schema: {query}")
        
        schema_df = conn.execute(query).fetchdf()
        conn.close()
        
        # Convert to schema format
        schema = []
        for _, row in schema_df.iterrows():
            col_name = row['column_name']
            col_type = row['column_type']
            nullable = row.get('null', 'YES')
            
            schema.append({
                'name': col_name,
                'type': col_type,
                'nullable': nullable == 'YES',
                'primaryKey': False,  # Would need metadata to determine
                'description': f'{col_name} column'
            })
        
        return {
            'success': True,
            'table_name': table_name,
            'domain': domain,
            'source': source,
            'schema': schema,
            'column_count': len(schema)
        }
        
    except Exception as e:
        logger.error(f"Failed to fetch schema: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to read schema from MinIO: {str(e)}"
        )


@router.get("/statistics/{table_name}")
async def get_table_statistics(
    table_name: str,
    domain: Optional[str] = Query('finance', description="Domain name"),
    source: Optional[str] = Query('postgres', description="Source type")
):
    """
    Get column statistics from MinIO Bronze storage
    
    Calculates:
    - Distinct values count
    - Null count and percentage
    - Min/Max/Avg for numeric columns
    
    Used by the Statistics tab in Data Catalog.
    """
    try:
        conn = configure_duckdb_s3()
        table_path = get_minio_table_path(domain.lower(), source.lower(), table_name)
        
        # First, get column names and types
        schema_query = f"DESCRIBE SELECT * FROM '{table_path}' LIMIT 1"
        schema_df = conn.execute(schema_query).fetchdf()
        
        statistics = []
        
        # Get total row count
        count_query = f"SELECT COUNT(*) as total FROM '{table_path}'"
        total_rows = conn.execute(count_query).fetchdf()['total'][0]
        
        # Calculate statistics for each column
        for _, col_info in schema_df.iterrows():
            col_name = col_info['column_name']
            col_type = col_info['column_type']
            
            stat = {
                'column': col_name,
                'type': col_type,
                'distinct': None,
                'nulls': None,
                'nullPercent': None,
                'min': '-',
                'max': '-',
                'avg': '-'
            }
            
            try:
                # Count distinct values
                distinct_query = f"SELECT COUNT(DISTINCT \"{col_name}\") as distinct_count FROM '{table_path}'"
                distinct_count = conn.execute(distinct_query).fetchdf()['distinct_count'][0]
                stat['distinct'] = str(distinct_count)
                
                # Count nulls
                null_query = f"SELECT COUNT(*) as null_count FROM '{table_path}' WHERE \"{col_name}\" IS NULL"
                null_count = conn.execute(null_query).fetchdf()['null_count'][0]
                stat['nulls'] = int(null_count)
                stat['nullPercent'] = round((null_count / total_rows * 100), 4) if total_rows > 0 else 0
                
                # For numeric columns, get min/max/avg
                if 'INT' in col_type.upper() or 'DECIMAL' in col_type.upper() or 'DOUBLE' in col_type.upper() or 'FLOAT' in col_type.upper():
                    agg_query = f"""
                        SELECT 
                            MIN(\"{col_name}\") as min_val,
                            MAX(\"{col_name}\") as max_val,
                            AVG(\"{col_name}\") as avg_val
                        FROM '{table_path}'
                    """
                    agg_result = conn.execute(agg_query).fetchdf().iloc[0]
                    stat['min'] = round(float(agg_result['min_val']), 2) if pd.notna(agg_result['min_val']) else '-'
                    stat['max'] = round(float(agg_result['max_val']), 2) if pd.notna(agg_result['max_val']) else '-'
                    stat['avg'] = round(float(agg_result['avg_val']), 2) if pd.notna(agg_result['avg_val']) else '-'
                
                # For date columns, get min/max
                elif 'DATE' in col_type.upper() or 'TIME' in col_type.upper():
                    date_query = f"""
                        SELECT 
                            MIN(\"{col_name}\") as min_val,
                            MAX(\"{col_name}\") as max_val
                        FROM '{table_path}'
                    """
                    date_result = conn.execute(date_query).fetchdf().iloc[0]
                    stat['min'] = str(date_result['min_val']) if pd.notna(date_result['min_val']) else '-'
                    stat['max'] = str(date_result['max_val']) if pd.notna(date_result['max_val']) else '-'
            
            except Exception as col_error:
                logger.warning(f"Failed to calculate stats for column {col_name}: {col_error}")
            
            statistics.append(stat)
        
        conn.close()
        
        return {
            'success': True,
            'table_name': table_name,
            'domain': domain,
            'source': source,
            'total_rows': int(total_rows),
            'statistics': statistics
        }
        
    except Exception as e:
        logger.error(f"Failed to calculate statistics: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to calculate statistics from MinIO: {str(e)}"
        )


@router.get("/table-info/{table_name}")
async def get_table_info(
    table_name: str,
    domain: Optional[str] = Query('finance', description="Domain name"),
    source: Optional[str] = Query('postgres', description="Source type")
):
    """
    Get comprehensive table information (metadata + row count + size)
    
    This provides a summary for the table header in Data Catalog.
    """
    try:
        conn = configure_duckdb_s3()
        table_path = get_minio_table_path(domain.lower(), source.lower(), table_name)
        
        # Get row count
        count_query = f"SELECT COUNT(*) as total FROM '{table_path}'"
        row_count = conn.execute(count_query).fetchdf()['total'][0]
        
        # Get column count
        schema_query = f"DESCRIBE SELECT * FROM '{table_path}' LIMIT 1"
        schema_df = conn.execute(schema_query).fetchdf()
        column_count = len(schema_df)
        
        conn.close()
        
        return {
            'success': True,
            'table_name': table_name,
            'domain': domain,
            'source': source,
            'layer': 'bronze',
            'row_count': int(row_count),
            'columns': column_count,
            'size': 'Unknown'  # Would need MinIO API to get file size
        }
        
    except Exception as e:
        logger.error(f"Failed to get table info: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to get table info from MinIO: {str(e)}"
        )

@router.post("/execute-query")
async def execute_custom_query(request: QueryRequest):
    """
    Execute a custom SQL query against Bronze data in MinIO using DuckDB
    
    The query should already have the S3 path substituted in place of table names.
    Example: SELECT * FROM 's3://syniqai-bronze/finance/postgres/finance_transactions/*.parquet' LIMIT 100
    
    Note: Uses s3:// protocol which DuckDB translates to MinIO (localhost:9000) via httpfs configuration.
    
    This endpoint is used by the Query tab in Data Catalog.
    """
    try:
        import time
        start_time = time.time()
        
        conn = configure_duckdb_s3()
        
        logger.info(f"Executing custom query: {request.query[:200]}...")
        
        # Execute the query
        result_df = conn.execute(request.query).fetchdf()
        conn.close()
        
        execution_time = int((time.time() - start_time) * 1000)  # milliseconds
        
        # Convert to JSON-serializable format
        rows = result_df.to_dict('records')
        
        # Convert timestamps to strings
        for row in rows:
            for key, value in row.items():
                if pd.isna(value):
                    row[key] = None
                elif isinstance(value, (pd.Timestamp, datetime)):
                    row[key] = value.isoformat()
        
        return {
            'success': True,
            'query': request.query[:200] + '...' if len(request.query) > 200 else request.query,
            'columns': list(result_df.columns),
            'rows': rows,
            'row_count': len(rows),
            'execution_time_ms': execution_time,
            'message': f'Query executed successfully in {execution_time}ms'
        }
        
    except Exception as e:
        error_msg = str(e)
        logger.error(f"Failed to execute query: {error_msg}")
        return {
            'success': False,
            'error': error_msg,
            'message': f'Query execution failed: {error_msg}'
        }