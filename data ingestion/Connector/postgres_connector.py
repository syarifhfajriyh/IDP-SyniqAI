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
from base_connector import BaseConnector

# --- Memory Profiling (Optional) ---
try:
    import psutil
except ImportError:
    psutil = None

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("PostgresConnector")

def timed(func):
    def wrapper(*args, **kwargs):
        start = time.time()
        result = func(*args, **kwargs)
        duration = time.time() - start
        logger.info(f"⏱️  {func.__name__} took {duration:.4f}s")
        return result
    return wrapper

class PostgresConnector(BaseConnector):
    source_type = "postgres"

    def __init__(self, connection_config: Dict[str, Any], secret_handler=None):
        super().__init__(connection_config)
        self.engine = None
        self.secret_handler = secret_handler

    @timed
    def connect(self) -> None:
        cfg = self.connection_config
        if self.secret_handler:
            db_password = self.secret_handler.resolve(cfg)
        else:
            db_password = cfg.get("password")

        conn_str = (
            f"postgresql+psycopg2://{cfg['user']}:{db_password}"
            f"@{cfg['host']}:{cfg['port']}/{cfg['database']}"
        )
        
        self.engine = create_engine(
            conn_str,
            pool_size=20,           
            max_overflow=30,       
            pool_pre_ping=True,    
            pool_recycle=3600,
            connect_args={'connect_timeout': 10} 
        )
        logger.info(f"✅ Engine configured (Pool: 20+30, Timeout: 10s)")

    def validate_credentials(self) -> None:
        try:
            with self.engine.connect() as conn:
                conn.execute(text("SELECT 1"))
        except Exception as e:
            logger.error(f"Validation failed: {e}")
            raise

    def close(self) -> None:
        if self.engine: self.engine.dispose()

    def _sanitize_complex_types(self, df: pd.DataFrame) -> pd.DataFrame:
        """Sanitize complex types AND normalize numeric precision"""
        object_cols = df.select_dtypes(include=['object']).columns
        for col in object_cols:
            valid_sample = df[col].dropna()
            if valid_sample.empty: continue
            if isinstance(valid_sample.iloc[0], (dict, list)):
                df[col] = [json.dumps(x) if x is not None else None for x in df[col]]
                df[col] = df[col].astype("string")
        
        # 🔥 NEW: Normalize decimal columns to float64 to prevent schema mismatches
        from decimal import Decimal
        for col in df.columns:
            if len(df[col]) > 0 and isinstance(df[col].iloc[0], Decimal):
                df[col] = df[col].astype('float64')
        
        return df

    def _get_row_count_estimate(self, table_name, conn):
        """Fast row count from pg_class statistics"""
        try:
            query = text("SELECT reltuples::bigint FROM pg_class WHERE relname = :t")
            result = conn.execute(query, {"t": table_name}).scalar()
            if result and result > 0:
                return int(result)
            # Fallback
            query = text(f"SELECT COUNT(*) FROM {table_name}")
            return conn.execute(query).scalar()
        except Exception as e:
            logger.warning(f"Row estimate failed: {e}")
            return 100000

    def _get_column_cast_sql(self, entity):
        """Generate SQL that casts NUMERIC columns to DOUBLE PRECISION"""
        inspector = inspect(self.engine)
        columns = inspector.get_columns(entity)
        
        select_parts = []
        for col in columns:
            col_name = col['name']
            col_type = str(col['type']).upper()
            
            # Cast NUMERIC/DECIMAL to DOUBLE PRECISION
            if 'NUMERIC' in col_type or 'DECIMAL' in col_type:
                select_parts.append(f"{col_name}::double precision as {col_name}")
            else:
                select_parts.append(col_name)
        
        return ", ".join(select_parts)

    # ==========================================
    # PARALLEL WORKER LOGIC
    # ==========================================
    def _get_min_max(self, entity, col_name):
        with self.engine.connect() as conn:
            query = text(f"SELECT MIN({col_name}), MAX({col_name}) FROM {entity}")
            min_val, max_val = conn.execute(query).fetchone()
        return min_val, max_val

    def _worker_extract(self, task_config, internal_queue, schema_snapshot):
        thread_name = threading.current_thread().name
        entity = task_config["entity"]
        partition_col = task_config["partition_col"]
        start_val = task_config["start"]
        end_val = task_config["end"]
        chunk_size = task_config["chunk_size"]
        
        # 🔥 OPTIMIZED: Use raw connection for faster startup
        conn = self.engine.raw_connection()
        
        try:
            logger.info(f"   🚀 {thread_name} started: {start_val:,} → {end_val:,}")
            
            # 🔥 SIMPLE QUERY - Direct execution, no SQLAlchemy overhead
            query = f"""
                SELECT * FROM {entity} 
                WHERE {partition_col} >= {start_val} AND {partition_col} < {end_val}
                ORDER BY {partition_col}
            """
            
            # 🔥 Use server-side cursor for memory efficiency
            cursor = conn.cursor(name=f"cur_{thread_name.replace('-', '_')}_{start_val}")
            cursor.itersize = chunk_size  # Fetch in chunks
            cursor.execute(query)
            
            chunk_idx = 0
            while True:
                rows = cursor.fetchmany(chunk_size)
                if not rows:
                    break
                
                # Get column names from cursor
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
            conn.commit()  # Close transaction
            conn.close()
            internal_queue.put(None)

    # ==========================================
    # MAIN EXTRACT METHOD
    # ==========================================
    @timed
    def extract(self, extraction_plan: Dict[str, Any]) -> Iterator[Dict[str, Any]]:
        entity = extraction_plan["entity"]
        chunk_size = extraction_plan.get("chunk_size", 50000)
        
        is_manual_mode = extraction_plan.get("manual_mode", False)
        
        if psutil:
            available_gb = psutil.virtual_memory().available / (1024**3)
            
            if is_manual_mode:
                if available_gb < 1.5:
                    old_chunk = chunk_size
                    chunk_size = min(chunk_size, 10000)
                    logger.error(f"🚨 CRITICAL memory ({available_gb:.1f}GB)! Forcing {old_chunk:,} → {chunk_size:,}")
                else:
                    logger.info(f"✋ MANUAL MODE - Using chunk size: {chunk_size:,} (RAM: {available_gb:.1f}GB)")
            else:
                logger.info(f"🤖 AUTO MODE - Using optimizer chunk: {chunk_size:,} (RAM: {available_gb:.1f}GB)")
        else:
            logger.info(f"📦 Using chunk size: {chunk_size:,}")

        partition_col = extraction_plan.get("partition_column")
        num_workers = extraction_plan.get("num_workers", 1)

        if partition_col and num_workers > 1:
            min_val, max_val = self._get_min_max(entity, partition_col)
            
            if min_val is None:
                logger.warning("⚠️ Table appears empty.")
                return

            total_range = max_val - min_val + 1
            
            if total_range < 1000:
                logger.info(f"📉 Small range ({total_range}). Using SERIAL.")
            else:
                logger.info(f"⚡ PARALLEL: {num_workers} workers, {total_range:,} rows")
                
                # 🔥 OPTIMIZED: Smaller steps = faster startup
                step = math.ceil(total_range / num_workers)
                internal_queue = queue.Queue(maxsize=num_workers * 3)  # Bigger buffer
                
                inspector = inspect(self.engine)
                schema_snapshot = [{"name": c["name"], "type": str(c["type"])} 
                                 for c in inspector.get_columns(entity)]
                
                # 🔥 OPTIMIZED: Start workers immediately
                executor = ThreadPoolExecutor(max_workers=num_workers, 
                                            thread_name_prefix="PG-Worker")
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
                    
                    future = executor.submit(
                        self._worker_extract, 
                        task_config, 
                        internal_queue, 
                        schema_snapshot
                    )
                    futures.append(future)
                
                # Aggregation loop
                completed_workers = 0
                chunks_yielded = 0
                
                while completed_workers < num_workers:
                    try:
                        item = internal_queue.get(timeout=5)
                        
                        if item is None:
                            completed_workers += 1
                            internal_queue.task_done()
                            logger.info(f"   ✅ Worker done ({completed_workers}/{num_workers})")
                            continue
                        
                        if isinstance(item, Exception):
                            executor.shutdown(wait=False, cancel_futures=True)
                            raise item
                        
                        chunks_yielded += 1
                        yield item
                        internal_queue.task_done()
                        
                    except queue.Empty:
                        if all(f.done() for f in futures):
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
                logger.info(f"✅ Parallel done: {chunks_yielded} chunks")
                return

        # SERIAL EXECUTION
        logger.info(f"🐢 SERIAL MODE: Optimized Stream")
        mode = extraction_plan.get("mode", "full")
        
        # 🔥 OPTIMIZED: Use raw connection for serial too
        conn = self.engine.raw_connection()
        cursor = conn.cursor()
        
        try:
            inspector = inspect(self.engine)
            schema_snapshot = [{"name": c["name"], "type": str(c["type"])} for c in inspector.get_columns(entity)]
            
            if mode == "incremental":
                watermark_col = extraction_plan["watermark_column"]
                start_value = extraction_plan["initial_value"]
                query = f"SELECT * FROM {entity} WHERE {watermark_col} > '{start_value}'"
            else:
                query = f"SELECT * FROM {entity}"
            
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