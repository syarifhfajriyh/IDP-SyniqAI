"""
PostgreSQL Connector - Optimized for Production
================================================
Features:
- Connection pooling (configurable pool size)
- Parallel extraction with integer partition columns
- Server-side cursors for memory efficiency
- Automatic type sanitization (JSON, Decimal, etc.)
- Incremental extraction support

Usage:
    from utils.config_loader import load_config
    from connectors import PostgresConnector
    
    config = load_config("config/db_config.yaml")
    connector = PostgresConnector(config["postgresql"])
    connector.connect()
    
    for batch in connector.extract({"entity": "customers", "mode": "full"}):
        print(f"Got {len(batch['data'])} rows")
"""

import pandas as pd
import json
import logging
import time
import math
import queue
import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from sqlalchemy import create_engine, inspect, text
from typing import Dict, Any, Iterator
from decimal import Decimal

from .base_connector import BaseConnector

try:
    import psutil
except ImportError:
    psutil = None

logger = logging.getLogger(__name__)


def timed(func):
    """Decorator to measure function execution time"""
    def wrapper(*args, **kwargs):
        start = time.time()
        result = func(*args, **kwargs)
        duration = time.time() - start
        logger.info(f"⏱️  {func.__name__} took {duration:.4f}s")
        return result
    return wrapper


class PostgresConnector(BaseConnector):
    """
    PostgreSQL connector with optimized extraction strategies.
    
    Supports:
    - Full table extraction
    - Incremental extraction (watermark-based)
    - Parallel extraction (partition by integer column)
    - Automatic chunking
    """
    
    source_type = "postgres"

    def __init__(self, connection_config: Dict[str, Any]):
        super().__init__(connection_config)
        self.engine = None

    @timed
    def connect(self) -> None:
        """
        Establish connection with PostgreSQL.
        
        Uses configuration from db_config.yaml:
        - Connection pooling (pool_size, max_overflow)
        - Timeouts (connect_timeout)
        - Health checks (pool_pre_ping)
        """
        cfg = self.connection_config
        
        # Build connection string
        conn_str = (
            f"postgresql+psycopg2://{cfg['user']}:{cfg['password']}"
            f"@{cfg['host']}:{cfg['port']}/{cfg['database']}"
        )
        
        # Get pool settings from config
        pool_size = cfg.get('pool_size', 20)
        max_overflow = cfg.get('max_overflow', 10)
        connect_timeout = cfg.get('connect_args', {}).get('connect_timeout', 10)
        
        # Create engine with config settings
        self.engine = create_engine(
            conn_str,
            pool_size=pool_size,
            max_overflow=max_overflow,
            pool_pre_ping=True,
            pool_recycle=cfg.get('pool_recycle', 3600),
            connect_args={'connect_timeout': connect_timeout}
        )
        
        logger.info(
            f"✅ Connected to PostgreSQL at {cfg['host']}:{cfg['port']}"
            f" (Pool: {pool_size}+{max_overflow})"
        )

    def validate_credentials(self) -> None:
        """Validate PostgreSQL connection and permissions"""
        try:
            with self.engine.connect() as conn:
                # Test basic connectivity
                conn.execute(text("SELECT 1"))
                
                # Check database access
                result = conn.execute(text("SELECT current_database()")).scalar()
                logger.info(f"✅ Database access verified: {result}")
                
                # Check schema access
                schema = self.connection_config.get('schema', 'public')
                conn.execute(text(f"SELECT 1 FROM information_schema.tables WHERE table_schema = :schema LIMIT 1"), 
                           {"schema": schema})
                
                logger.info(f"✅ Schema '{schema}' accessible")
                
        except Exception as e:
            logger.error(f"❌ Credential validation failed: {e}")
            raise PermissionError(f"PostgreSQL validation failed: {e}")

    def close(self) -> None:
        """Close connection pool"""
        if self.engine:
            self.engine.dispose()
            logger.info("🔌 PostgreSQL connection closed")

    def _sanitize_complex_types(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Sanitize complex types for Parquet compatibility.
        
        Handles:
        - JSON/JSONB → string
        - Decimal → float64
        - Arrays → JSON string
        """
        # Handle object columns (could be JSON/arrays)
        object_cols = df.select_dtypes(include=['object']).columns
        for col in object_cols:
            valid_sample = df[col].dropna()
            if valid_sample.empty:
                continue
            
            if isinstance(valid_sample.iloc[0], (dict, list)):
                df[col] = df[col].apply(lambda x: json.dumps(x) if x is not None else None)
                df[col] = df[col].astype("string")
        
        # Handle Decimal types
        for col in df.columns:
            if len(df[col]) > 0 and isinstance(df[col].iloc[0], Decimal):
                df[col] = df[col].astype('float64')
        
        return df

    def _get_row_count_estimate(self, table_name: str, conn) -> int:
        """Get fast row count estimate from pg_class statistics"""
        try:
            schema = self.connection_config.get('schema', 'public')
            query = text("""
                SELECT reltuples::bigint 
                FROM pg_class 
                WHERE relname = :table 
                AND relnamespace = :schema::regnamespace
            """)
            result = conn.execute(query, {"table": table_name, "schema": schema}).scalar()
            
            if result and result > 0:
                return int(result)
            
            # Fallback to exact count
            query = text(f"SELECT COUNT(*) FROM {schema}.{table_name}")
            return conn.execute(query).scalar()
            
        except Exception as e:
            logger.warning(f"⚠️ Row estimate failed: {e}")
            return 100000  # Default estimate

    def _get_min_max(self, entity: str, col_name: str) -> tuple:
        """Get min/max values for partition column"""
        schema = self.connection_config.get('schema', 'public')
        with self.engine.connect() as conn:
            query = text(f"SELECT MIN({col_name}), MAX({col_name}) FROM {schema}.{entity}")
            min_val, max_val = conn.execute(query).fetchone()
        return min_val, max_val

    def _worker_extract(self, task_config: Dict, internal_queue: queue.Queue, schema_snapshot: list):
        """
        Parallel worker function for extracting data chunks.
        
        Args:
            task_config: Worker configuration
            internal_queue: Queue for returning results
            schema_snapshot: Schema metadata
        """
        thread_name = threading.current_thread().name
        entity = task_config["entity"]
        partition_col = task_config["partition_col"]
        start_val = task_config["start"]
        end_val = task_config["end"]
        chunk_size = task_config["chunk_size"]
        schema = self.connection_config.get('schema', 'public')
        
        conn = self.engine.raw_connection()
        
        try:
            logger.info(f"   🚀 {thread_name} started: {start_val:,} → {end_val:,}")
            
            query = f"""
                SELECT * FROM {schema}.{entity} 
                WHERE {partition_col} >= {start_val} AND {partition_col} < {end_val}
                ORDER BY {partition_col}
            """
            
            # Use server-side cursor
            cursor = conn.cursor(name=f"cur_{thread_name.replace('-', '_')}_{start_val}")
            cursor.itersize = chunk_size
            cursor.execute(query)
            
            chunk_idx = 0
            while True:
                rows = cursor.fetchmany(chunk_size)
                if not rows:
                    break
                
                col_names = [desc[0] for desc in cursor.description]
                chunk_df = pd.DataFrame(rows, columns=col_names)
                chunk_df = self._sanitize_complex_types(chunk_df)
                
                payload = {
                    "data": chunk_df,
                    "metadata": {
                        "source_type": self.source_type,
                        "entity": entity,
                        "row_count": len(chunk_df),
                        "chunk_index": f"{thread_name}-{chunk_idx}",
                        "worker": thread_name,
                        "schema_snapshot": schema_snapshot,
                        "extracted_at": datetime.now(timezone.utc).isoformat()
                    }
                }
                
                internal_queue.put(payload, block=True, timeout=30)
                chunk_idx += 1
            
            cursor.close()
            logger.info(f"   🏁 {thread_name} finished: {chunk_idx} chunks")
            
        except Exception as e:
            logger.error(f"   ❌ {thread_name} Failed: {e}")
            internal_queue.put(e)
        finally:
            conn.commit()
            conn.close()
            internal_queue.put(None)  # Signal completion

    @timed
    def extract(self, extraction_plan: Dict[str, Any]) -> Iterator[Dict[str, Any]]:
        """
        Extract data from PostgreSQL table.
        
        Args:
            extraction_plan: Extraction configuration
                Required:
                - entity: Table name
                - mode: "full" or "incremental"
                
                Optional:
                - chunk_size: Rows per chunk (default: 50000)
                - partition_column: Column for parallel extraction
                - num_workers: Number of parallel workers (default: 1)
                - watermark_column: For incremental mode
                - initial_value: Starting value for incremental
        
        Yields:
            Dict with "data" (DataFrame) and "metadata"
        """
        entity = extraction_plan["entity"]
        chunk_size = extraction_plan.get("chunk_size", 
                                        self.connection_config.get('chunk_size', 50000))
        
        # Memory-aware optimization
        if psutil:
            available_gb = psutil.virtual_memory().available / (1024**3)
            if available_gb < 1.5:
                old_chunk = chunk_size
                chunk_size = min(chunk_size, 10000)
                logger.warning(f"⚠️ Low memory ({available_gb:.1f}GB)! Reduced chunk: {old_chunk:,} → {chunk_size:,}")

        partition_col = extraction_plan.get("partition_column")
        num_workers = extraction_plan.get("num_workers", 1)

        # PARALLEL EXTRACTION PATH
        if partition_col and num_workers > 1:
            min_val, max_val = self._get_min_max(entity, partition_col)
            
            if min_val is None:
                logger.warning(f"⚠️ Table '{entity}' appears empty")
                return

            total_range = max_val - min_val + 1
            
            # Skip parallel for tiny tables
            if total_range < 1000:
                logger.info(f"📉 Small range ({total_range}). Using serial extraction.")
            else:
                logger.info(f"⚡ PARALLEL MODE: {num_workers} workers, Range: {min_val:,} → {max_val:,}")
                
                step = math.ceil(total_range / num_workers)
                internal_queue = queue.Queue(maxsize=num_workers * 3)
                
                # Get schema snapshot
                inspector = inspect(self.engine)
                schema_snapshot = [
                    {"name": c["name"], "type": str(c["type"])} 
                    for c in inspector.get_columns(entity)
                ]
                
                executor = ThreadPoolExecutor(max_workers=num_workers, thread_name_prefix="PG-Worker")
                futures = []
                
                for i in range(num_workers):
                    start = min_val + (i * step)
                    end = min(start + step, max_val + 1)
                    
                    if start >= end:
                        break
                    
                    task_config = {
                        "entity": entity,
                        "partition_col": partition_col,
                        "start": start,
                        "end": end,
                        "chunk_size": chunk_size
                    }
                    
                    future = executor.submit(self._worker_extract, task_config, internal_queue, schema_snapshot)
                    futures.append(future)
                
                # Yield results as they arrive
                completed_workers = 0
                chunks_yielded = 0
                
                while completed_workers < num_workers:
                    try:
                        item = internal_queue.get(timeout=5)
                        
                        if item is None:
                            completed_workers += 1
                            internal_queue.task_done()
                            continue
                        
                        if isinstance(item, Exception):
                            executor.shutdown(wait=False, cancel_futures=True)
                            raise item
                        
                        chunks_yielded += 1
                        yield item
                        internal_queue.task_done()
                        
                    except queue.Empty:
                        if all(f.done() for f in futures):
                            # Drain remaining items
                            while not internal_queue.empty():
                                try:
                                    item = internal_queue.get_nowait()
                                    if item is not None and not isinstance(item, Exception):
                                        chunks_yielded += 1
                                        yield item
                                    internal_queue.task_done()
                                except queue.Empty:
                                    break
                            break
                
                executor.shutdown(wait=True)
                logger.info(f"✅ Parallel extraction completed: {chunks_yielded} chunks")
                return

        # SERIAL EXTRACTION PATH
        logger.info(f"🐢 SERIAL MODE: Entity={entity}, Chunk={chunk_size:,}")
        mode = extraction_plan.get("mode", "full")
        schema = self.connection_config.get('schema', 'public')
        
        conn = self.engine.raw_connection()
        cursor = conn.cursor()
        
        try:
            inspector = inspect(self.engine)
            schema_snapshot = [
                {"name": c["name"], "type": str(c["type"])} 
                for c in inspector.get_columns(entity)
            ]
            
            if mode == "incremental":
                watermark_col = extraction_plan["watermark_column"]
                start_value = extraction_plan["initial_value"]
                query = f"SELECT * FROM {schema}.{entity} WHERE {watermark_col} > '{start_value}'"
            else:
                query = f"SELECT * FROM {schema}.{entity}"
            
            cursor.execute(query)
            
            chunk_idx = 0
            while True:
                rows = cursor.fetchmany(chunk_size)
                if not rows:
                    break
                
                col_names = [desc[0] for desc in cursor.description]
                chunk_df = pd.DataFrame(rows, columns=col_names)
                chunk_df = self._sanitize_complex_types(chunk_df)
                
                yield {
                    "data": chunk_df,
                    "metadata": {
                        "source_type": self.source_type,
                        "entity": entity,
                        "chunk_index": chunk_idx,
                        "row_count": len(chunk_df),
                        "schema_snapshot": schema_snapshot,
                        "extracted_at": datetime.now(timezone.utc).isoformat()
                    }
                }
                chunk_idx += 1
                
        finally:
            cursor.close()
            conn.close()