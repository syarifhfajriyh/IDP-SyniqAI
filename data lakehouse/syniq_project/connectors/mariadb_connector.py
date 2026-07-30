# filepath: connectors/mariadb_connector.py
"""
MariaDB Connector - Production-Ready with Advanced Optimizations
=================================================================
Features:
- Engine-aware extraction (InnoDB, ColumnStore, MyISAM, Aria)
- Intelligent optimizer with auto-tuning (chunk size, workers)
- Parallel extraction with quantile-based smart chunking
- Adaptive throttling based on database health
- PyArrow backend for memory efficiency
- Consistent snapshot for InnoDB
- SSL/TLS support

Usage:
    from utils.config_loader import load_config
    from connectors import MariaDBConnector
    
    config = load_config("config/db_config.yaml")
    connector = MariaDBConnector(config["mariadb"])
    connector.connect()
    
    for batch in connector.extract({"entity": "claims", "mode": "full"}):
        df = batch["data"]
        print(f"Got {len(df)} rows")
"""

import pandas as pd
import json
import logging
import math
import time
import queue
import threading
import multiprocessing
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from sqlalchemy import create_engine, text, inspect
from typing import Dict, Any, Iterator, Tuple, List

from .base_connector import BaseConnector

try:
    import psutil
except ImportError:
    psutil = None

logger = logging.getLogger(__name__)


def timed(func):
    """Decorator to measure performance"""
    def wrapper(*args, **kwargs):
        start = time.time()
        result = func(*args, **kwargs)
        duration = time.time() - start
        logger.info(f"⏱️  {func.__name__} took {duration:.4f}s")
        return result
    return wrapper


# ==========================================
# EXTRACTION OPTIMIZER
# ==========================================
class ExtractionOptimizer:
    """Intelligent extraction parameter calculator"""
    
    def __init__(self):
        self.cpu_cores = multiprocessing.cpu_count()
        if psutil:
            self.total_ram_gb = psutil.virtual_memory().total / (1024**3)
            self.available_ram_gb = psutil.virtual_memory().available / (1024**3)
        else:
            self.total_ram_gb = 8.0
            self.available_ram_gb = 4.0
            logger.warning("⚠️ psutil not installed. Using default RAM values.")
    
    def calculate_optimal_params(
        self, 
        row_count: int,
        avg_row_size_bytes: int = 1024,
        is_remote: bool = True,
        engine_type: str = "innodb"
    ) -> Tuple[int, int]:
        """Calculate optimal chunk_size and num_workers"""
        
        # Base chunk size (target ~100MB per chunk)
        target_chunk_memory_mb = 100
        base_chunk_size = int((target_chunk_memory_mb * 1024 * 1024) / avg_row_size_bytes)
        
        # Apply table-size scaling
        if row_count < 1_000:
            chunk_size = row_count
        elif row_count < 10_000:
            chunk_size = min(base_chunk_size, 1_000)
        elif row_count < 100_000:
            chunk_size = min(base_chunk_size, 10_000)
        elif row_count < 1_000_000:
            chunk_size = min(base_chunk_size, 50_000)
        else:  # 1M+ rows
            chunk_size = min(base_chunk_size, 100_000)
        
        # Calculate workers
        chunk_memory_gb = (chunk_size * avg_row_size_bytes) / (1024**3)
        max_workers_by_ram = int((self.available_ram_gb * 0.5) / (chunk_memory_gb or 1))
        max_workers_by_cpu = self.cpu_cores
        total_chunks = (row_count / (chunk_size or 1))
        max_workers_by_efficiency = int(total_chunks ** 0.5) + 1
        
        network_factor = 0.75 if is_remote else 1.0
        
        num_workers = max(1, min(
            max_workers_by_ram,
            max_workers_by_cpu,
            max_workers_by_efficiency
        ))
        num_workers = int(num_workers * network_factor)
        
        # Don't use parallel for tiny tables
        if row_count < 1_000:
            num_workers = 1
        
        # Cap at reasonable maximum
        num_workers = min(num_workers, 16)
        
        # Ensure chunk_size doesn't exceed row_count
        chunk_size = min(chunk_size, row_count) if row_count > 0 else base_chunk_size
        
        return chunk_size, max(1, num_workers)
    
    def estimate_extraction_time(
        self, 
        row_count: int, 
        chunk_size: int, 
        num_workers: int,
        throughput_rows_per_sec: int = 50_000
    ) -> str:
        """Estimate total extraction time"""
        if row_count == 0:
            return "0s"
        
        rows_per_worker = (row_count + num_workers - 1) // num_workers
        seconds = rows_per_worker / throughput_rows_per_sec
        
        if seconds < 60:
            return f"~{int(seconds)}s"
        elif seconds < 3600:
            return f"~{int(seconds/60)}m"
        else:
            return f"~{seconds/3600:.1f}h"
    
    def get_optimization_summary(
        self, 
        row_count: int,
        chunk_size: int,
        num_workers: int
    ) -> Dict[str, any]:
        """Generate detailed summary for logging"""
        if row_count == 0:
            return {"status": "empty_table"}
        
        total_chunks = (row_count + chunk_size - 1) // chunk_size
        est_time = self.estimate_extraction_time(row_count, chunk_size, num_workers)
        
        return {
            "table_rows": f"{row_count:,}",
            "chunk_size": f"{chunk_size:,}",
            "num_workers": num_workers,
            "total_chunks": total_chunks,
            "chunks_per_worker": (total_chunks + num_workers - 1) // num_workers,
            "estimated_time": est_time,
            "system_cpu_cores": self.cpu_cores,
            "available_ram_gb": f"{self.available_ram_gb:.1f}",
            "parallel_efficiency": f"{(num_workers/self.cpu_cores)*100:.0f}%"
        }


# ==========================================
# STORAGE ENGINE CAPABILITIES
# ==========================================
ENGINE_CAPABILITIES = {
    "innodb":      {"parallel_safe": True,  "mvcc": True,  "risk": "low",    "locks": "row"},
    "columnstore": {"parallel_safe": True,  "mvcc": False, "risk": "low",    "locks": "none"},
    "myisam":      {"parallel_safe": False, "mvcc": False, "risk": "high",   "locks": "table"},
    "aria":        {"parallel_safe": False, "mvcc": False, "risk": "medium", "locks": "table"},
    "memory":      {"parallel_safe": False, "mvcc": False, "risk": "high",   "locks": "table"},
    "unknown":     {"parallel_safe": False, "mvcc": False, "risk": "high",   "locks": "unknown"}
}


# ==========================================
# MARIADB CONNECTOR
# ==========================================
class MariaDBConnector(BaseConnector):
    """
    MariaDB Connector - Engine-Aware, Parallel-Optimized, Production-Ready
    
    Features:
    ✅ Engine detection (InnoDB, ColumnStore, MyISAM, Aria)
    ✅ Intelligent auto-tuning (chunk size, workers)
    ✅ Parallel extraction with smart chunking
    ✅ Adaptive throttling based on DB health
    ✅ PyArrow backend for memory efficiency
    ✅ InnoDB consistent snapshot
    ✅ JSON flattening support
    """
    
    source_type = "mariadb"

    def __init__(self, connection_config: Dict[str, Any]):
        super().__init__(connection_config)
        self.engine = None
        self.table_metadata_cache = {} 
        self.optimizer = ExtractionOptimizer()

    @timed
    def connect(self) -> None:
        """Establish connection with SSL verification"""
        cfg = self.connection_config
        
        port = cfg.get('port', 3306)
        conn_str = (
            f"mysql+pymysql://{cfg['user']}:{cfg['password']}"
            f"@{cfg['host']}:{port}/{cfg['database']}"
        )

        # SSL Configuration
        connect_args = {'connect_timeout': cfg.get('connect_args', {}).get('connect_timeout', 10)}
        
        if cfg.get("ssl", {}).get("ca"):
            connect_args['ssl'] = {
                'ca': cfg["ssl"]["ca"],
                'check_hostname': cfg.get("ssl", {}).get("verify_cert", True)
            }
        
        # Get pool settings from config
        pool_size = cfg.get('pool_size', 20)
        max_overflow = cfg.get('max_overflow', 10)
        pool_recycle = cfg.get('pool_recycle', 3600)
        
        # Create engine with REPEATABLE READ for InnoDB consistency
        self.engine = create_engine(
            conn_str,
            pool_size=pool_size,
            max_overflow=max_overflow,
            pool_recycle=pool_recycle,
            pool_pre_ping=True,
            connect_args=connect_args,
            isolation_level="REPEATABLE READ"
        )
        
        # Connection + SSL verification
        try:
            with self.engine.connect() as test_conn:
                # Check SSL status
                ssl_check = test_conn.execute(text("SHOW STATUS LIKE 'Ssl_cipher'")).fetchone()
                if ssl_check and ssl_check[1]:
                    logger.info(f"🔒 SSL Enabled: {ssl_check[1]}")
                else:
                    logger.warning("⚠️  SSL NOT enabled - connection is unencrypted!")
                
                # Get version
                result = test_conn.execute(text("SELECT VERSION()")).fetchone()
                version = result[0] if result else "Unknown"
                logger.info(f"✅ Connected to MariaDB {version} at {cfg['host']}:{port}")
                logger.info(f"   Pool: {pool_size}+{max_overflow}, Isolation: REPEATABLE READ")
        except Exception as e:
            logger.error(f"❌ Connection failed: {e}")
            raise

    def validate_credentials(self) -> None:
        """Comprehensive permission validation"""
        db_name = self.connection_config['database']
        
        required_checks = {
            "basic_connect": "SELECT 1",
            "show_databases": "SHOW DATABASES",
            "show_tables": f"SHOW TABLES FROM {db_name}",
            "read_schema": """
                SELECT TABLE_NAME FROM information_schema.TABLES 
                WHERE TABLE_SCHEMA = :db LIMIT 1
            """
        }
        
        try:
            with self.engine.connect() as conn:
                for check_name, query in required_checks.items():
                    if ":db" in query:
                        conn.execute(text(query), {"db": db_name})
                    else:
                        conn.execute(text(query))
                    logger.debug(f"✓ {check_name}")
                    
            logger.info("✅ All required permissions verified")
            
        except Exception as e:
            logger.error(f"❌ Permission check failed: {e}")
            raise PermissionError(f"Insufficient database permissions: {e}")

    # ==========================================
    # METADATA + ENGINE DETECTION
    # ==========================================
    def _detect_engine_and_pk(self, table_name: str) -> Dict[str, Any]:
        """Discover storage engine, capabilities, and PK"""
        if table_name in self.table_metadata_cache:
            return self.table_metadata_cache[table_name]

        db_name = self.connection_config['database']
        inspector = inspect(self.engine)
        
        # Get Storage Engine
        engine_query = text("""
            SELECT ENGINE FROM information_schema.TABLES 
            WHERE TABLE_SCHEMA = :db AND TABLE_NAME = :table
        """)
        with self.engine.connect() as conn:
            eng_res = conn.execute(engine_query, {"db": db_name, "table": table_name}).fetchone()
            storage_engine = eng_res[0] if eng_res else "unknown"

        # Lookup Capabilities
        caps = ENGINE_CAPABILITIES.get(storage_engine.lower(), ENGINE_CAPABILITIES["unknown"])

        # Get Primary Key
        pk_constraint = inspector.get_pk_constraint(table_name)
        pk_columns = pk_constraint.get('constrained_columns', [])
        
        pk_col = None
        is_integer_pk = False
        if len(pk_columns) == 1:
            pk_col = pk_columns[0]
            columns_info = inspector.get_columns(table_name)
            for col in columns_info:
                if col['name'] == pk_col:
                    type_str = str(col['type']).lower()
                    if any(x in type_str for x in ['int', 'bigint', 'smallint', 'tinyint']):
                        is_integer_pk = True
                    break

        meta = {
            "storage_engine": storage_engine,
            "capabilities": caps,
            "pk_column": pk_col,
            "is_integer_pk": is_integer_pk
        }
        self.table_metadata_cache[table_name] = meta
        
        # Risk Warnings
        logger.info(f"🧐 Table '{table_name}': Engine={storage_engine} | PK={pk_col} | Risk={caps['risk'].upper()}")
        if not caps["parallel_safe"]:
            logger.warning(f"⚠️  Engine '{storage_engine}' is NOT parallel-safe. Will use serial extraction.")
        if caps["risk"] != "low":
            logger.warning(f"⚠️  {storage_engine.upper()} lacks MVCC - potential locking during extraction!")

        return meta

    def _get_min_max(self, table_name: str, pk_col: str) -> Tuple[int, int]:
        """Get PK range for chunking"""
        with self.engine.connect() as conn:
            query = text(f"SELECT MIN({pk_col}), MAX({pk_col}) FROM {table_name}")
            result = conn.execute(query).fetchone()
            if result and result[0] is not None:
                return result[0], result[1]
            return None, None

    def _get_quantile_boundaries(self, table_name: str, pk_col: str, num_workers: int) -> List[int]:
        """
        Smart Chunking via Quantile Sampling
        
        Calculate worker boundaries based on actual data distribution.
        Solves the "density problem" where deleted rows create idle workers.
        """
        with self.engine.connect() as conn:
            # Get total count
            count_query = text(f"SELECT COUNT(*) FROM {table_name}")
            total = conn.execute(count_query).scalar()
            
            if total < num_workers * 1000:
                logger.info("📊 Table too small for quantile sampling, using min/max")
                return None
            
            # Calculate quantile offsets
            step = total // num_workers
            boundaries = []
            
            logger.info(f"📊 Calculating quantile boundaries (sampling {num_workers + 1} points from {total:,} rows)...")
            
            for i in range(num_workers + 1):
                offset = min(i * step, total - 1)
                query = text(f"""
                    SELECT {pk_col} FROM {table_name} 
                    ORDER BY {pk_col} 
                    LIMIT 1 OFFSET :offset
                """)
                result = conn.execute(query, {"offset": offset}).scalar()
                if result is not None:
                    boundaries.append(result)
            
            if len(boundaries) < 2:
                logger.warning("⚠️  Quantile sampling failed, falling back to min/max")
                return None
            
            logger.info(f"✅ Quantile boundaries: {boundaries[:3]}...{boundaries[-3:]} (showing first/last 3)")
            return boundaries

    def _get_row_count_estimate(self, table_name: str) -> int:
        """Get approximate row count from statistics"""
        db_name = self.connection_config['database']
        
        with self.engine.connect() as conn:
            query = text("""
                SELECT TABLE_ROWS 
                FROM information_schema.TABLES 
                WHERE TABLE_SCHEMA = :db AND TABLE_NAME = :table
            """)
            result = conn.execute(query, {"db": db_name, "table": table_name}).fetchone()
            
            if result and result[0]:
                estimate = result[0]
                logger.info(f"📊 Estimated rows: ~{estimate:,}")
                return estimate
            return 0

    def _check_database_health(self) -> dict:
        """
        Adaptive Throttling - Monitor MariaDB load
        
        Returns throttle recommendation to prevent overwhelming production databases.
        """
        try:
            with self.engine.connect() as conn:
                # Get active connections
                threads = conn.execute(text("SHOW GLOBAL STATUS LIKE 'Threads_running'")).fetchone()
                active_threads = int(threads[1]) if threads else 0
                
                # Get buffer pool hit ratio (InnoDB health indicator)
                try:
                    bp_reads = conn.execute(text(
                        "SHOW GLOBAL STATUS LIKE 'Innodb_buffer_pool_reads'"
                    )).fetchone()
                    bp_requests = conn.execute(text(
                        "SHOW GLOBAL STATUS LIKE 'Innodb_buffer_pool_read_requests'"
                    )).fetchone()
                    
                    reads = int(bp_reads[1]) if bp_reads else 0
                    requests = int(bp_requests[1]) if bp_requests else 1
                    
                    if requests > 0:
                        hit_ratio = (1 - (reads / requests)) * 100
                    else:
                        hit_ratio = 100.0
                except Exception:
                    hit_ratio = 100.0
                
                # Calculate throttle (50+ threads = start throttling)
                throttle_seconds = 0
                if active_threads > 50:
                    throttle_seconds = min((active_threads - 50) * 0.5, 10)
                
                # Also throttle if buffer pool is struggling
                if hit_ratio < 80:
                    throttle_seconds = max(throttle_seconds, 2.0)
                
                return {
                    "active_threads": active_threads,
                    "buffer_pool_hit_ratio": round(hit_ratio, 2),
                    "throttle_seconds": throttle_seconds
                }
        except Exception as e:
            logger.warning(f"⚠️  Health check failed: {e}")
            return {"active_threads": 0, "buffer_pool_hit_ratio": 100.0, "throttle_seconds": 0}

    # ==========================================
    # DATA PROCESSING
    # ==========================================
    def _sanitize_and_flatten(self, df: pd.DataFrame, json_config: Dict[str, Any]) -> pd.DataFrame:
        """
        PyArrow-aware JSON flattening + type sanitization
        """
        if df.empty:
            return df
        
        # Flatten JSON columns
        if json_config:
            for col_name, keys in json_config.items():
                if col_name in df.columns:
                    parsed = df[col_name].apply(
                        lambda x: json.loads(x) if isinstance(x, str) and x else (x if isinstance(x, dict) else {})
                    )
                    for key in keys:
                        df[f"{col_name}_{key}"] = parsed.apply(lambda d: d.get(key, None))
        
        # PyArrow-aware sanitization
        for col in df.columns:
            dtype_str = str(df[col].dtype)
            
            if dtype_str == "object":
                sample = df[col].dropna()
                if not sample.empty:
                    first_val = sample.iloc[0]
                    if isinstance(first_val, (dict, list)):
                        df[col] = df[col].apply(lambda x: json.dumps(x) if x is not None else None)
                        try:
                            df[col] = df[col].astype("string[pyarrow]")
                        except Exception:
                            df[col] = df[col].astype("string")
        
        return df

    # ==========================================
    # PARALLEL WORKER
    # ==========================================
    def _worker_extract(self, task_config, internal_queue, schema_snapshot, json_config):
        """
        Parallel worker with InnoDB consistent snapshot and adaptive throttling
        """
        thread_name = threading.current_thread().name
        entity = task_config["entity"]
        query_text = task_config["query"]
        params = task_config["params"]
        storage_engine = task_config.get("storage_engine", "unknown")
        
        # Check database health before starting
        health = self._check_database_health()
        if health["throttle_seconds"] > 0:
            logger.warning(
                f"⏳ {thread_name} throttling {health['throttle_seconds']:.1f}s "
                f"(DB load: {health['active_threads']} threads, "
                f"buffer pool: {health['buffer_pool_hit_ratio']:.1f}%)"
            )
            time.sleep(health["throttle_seconds"])
        
        conn = self.engine.connect()
        try:
            # Start consistent snapshot for InnoDB
            if storage_engine.lower() == "innodb":
                conn.execute(text("START TRANSACTION WITH CONSISTENT SNAPSHOT"))
                logger.debug(f"🔒 {thread_name} acquired consistent snapshot")
            
            # Use PyArrow backend for memory efficiency
            chunk_df = pd.read_sql(
                text(query_text), 
                conn, 
                params=params,
                dtype_backend="pyarrow"
            )
            
            if not chunk_df.empty:
                chunk_df = self._sanitize_and_flatten(chunk_df, json_config)
                
                payload = {
                    "data": chunk_df,
                    "metadata": {
                        "source_type": self.source_type,
                        "entity": entity,
                        "storage_engine": storage_engine,
                        "engine_risk_level": task_config.get("risk", "unknown"),
                        "row_count": len(chunk_df),
                        "chunk_index": task_config["chunk_id"],
                        "worker": thread_name,
                        "schema_snapshot": schema_snapshot,
                        "extracted_at": datetime.now(timezone.utc).isoformat()
                    }
                }
                internal_queue.put(payload)
            
            # Commit transaction
            if storage_engine.lower() == "innodb":
                conn.execute(text("COMMIT"))
                
        except Exception as e:
            logger.error(f"❌ {thread_name} Failed: {e}")
            if storage_engine.lower() == "innodb":
                conn.execute(text("ROLLBACK"))
            internal_queue.put(e)
        finally:
            conn.close()

    # ==========================================
    # MAIN EXTRACT METHOD
    # ==========================================
    @timed
    def extract(self, extraction_plan: Dict[str, Any]) -> Iterator[Dict[str, Any]]:
        """
        Main extraction with auto-optimization and adaptive logic
        
        Args:
            extraction_plan:
                Required:
                - entity: Table name
                - mode: "full" or "incremental"
                
                Optional:
                - chunk_size: Override auto-calculated chunk size
                - num_workers: Override auto-calculated workers
                - enable_parallel: Enable/disable parallel extraction (default: True)
                - flatten_json: Dict for JSON flattening config
                - avg_row_size: Estimated row size in bytes (default: 1024)
        
        Yields:
            Dict with "data" (DataFrame) and "metadata"
        """
        entity = extraction_plan["entity"]
        json_flatten_config = extraction_plan.get("flatten_json", {})
        
        # Auto-calculate parameters
        row_count = self._get_row_count_estimate(entity)
        is_remote = self.connection_config.get("host") not in ["localhost", "127.0.0.1"]
        meta = self._detect_engine_and_pk(entity)
        storage_engine = meta["storage_engine"].lower()
        
        # Get optimal chunk size and workers
        chunk_size, num_workers = self.optimizer.calculate_optimal_params(
            row_count=row_count,
            avg_row_size_bytes=extraction_plan.get("avg_row_size", 1024),
            is_remote=is_remote,
            engine_type=storage_engine
        )
        
        # Allow plan to override
        chunk_size = extraction_plan.get("chunk_size", chunk_size)
        num_workers = extraction_plan.get("num_workers", num_workers)
        
        # Log optimization summary
        summary = self.optimizer.get_optimization_summary(row_count, chunk_size, num_workers)
        logger.info("=" * 60)
        logger.info("🧮 EXTRACTION OPTIMIZER")
        logger.info("=" * 60)
        logger.info(f"   Table: {entity}")
        logger.info(f"   Engine: {storage_engine.upper()}")
        logger.info(f"   Rows: {summary.get('table_rows', '0')}")
        logger.info(f"   Chunk Size: {summary.get('chunk_size', 'N/A')} rows")
        logger.info(f"   Workers: {summary.get('num_workers', 1)} parallel")
        logger.info(f"   Est. Time: {summary.get('estimated_time', 'N/A')}")
        logger.info(f"   System: {summary.get('system_cpu_cores', 'N/A')} cores, {summary.get('available_ram_gb', 'N/A')} GB RAM")
        logger.info("=" * 60)
        
        # Get metadata
        caps = meta["capabilities"]
        inspector = inspect(self.engine)
        schema_snapshot = [
            {"name": c["name"], "type": str(c["type"])} 
            for c in inspector.get_columns(entity)
        ]

        # Decide: Parallel or Serial?
        can_parallelize = (
            meta["is_integer_pk"] and 
            caps["parallel_safe"] and 
            num_workers > 1 and
            extraction_plan.get("enable_parallel", True)
        )

        # ==========================================
        # PATH A: PARALLEL EXECUTION
        # ==========================================
        if can_parallelize:
            pk_col = meta["pk_column"]
            
            # Try quantile-based boundaries first
            boundaries = self._get_quantile_boundaries(entity, pk_col, num_workers)
            
            if boundaries and len(boundaries) >= 2:
                logger.info(f"⚡ PARALLEL MODE (QUANTILE): {num_workers} workers | PK: {pk_col}")
                logger.info(f"   Range: {boundaries[0]}→{boundaries[-1]}")
                
                internal_queue = queue.Queue(maxsize=num_workers * 2)
                executor = ThreadPoolExecutor(max_workers=num_workers)
                futures = []
                
                for i in range(len(boundaries) - 1):
                    start = boundaries[i]
                    end = boundaries[i + 1]
                    
                    task = {
                        "entity": entity,
                        "query": f"SELECT * FROM {entity} WHERE {pk_col} >= :p_start AND {pk_col} < :p_end ORDER BY {pk_col}",
                        "params": {"p_start": start, "p_end": end},
                        "chunk_id": f"quantile-{i}",
                        "storage_engine": storage_engine,
                        "risk": caps["risk"]
                    }
                    futures.append(
                        executor.submit(self._worker_extract, task, internal_queue, schema_snapshot, json_flatten_config)
                    )
            else:
                # Fallback to min/max range splitting
                min_val, max_val = self._get_min_max(entity, pk_col)
                
                if min_val is None:
                    logger.warning(f"⚠️ Table '{entity}' appears empty.")
                    return

                total_rows = max_val - min_val + 1
                
                if total_rows < 1000:
                    logger.info(f"📉 Adaptive: Only {total_rows} rows. Using serial extraction.")
                    can_parallelize = False
                else:
                    logger.info(f"⚡ PARALLEL MODE (RANGE): {num_workers} workers | PK: {pk_col} | Range: {min_val}→{max_val}")
                    
                    step = math.ceil(total_rows / num_workers)
                    internal_queue = queue.Queue(maxsize=num_workers * 2)
                    executor = ThreadPoolExecutor(max_workers=num_workers)
                    futures = []
                    
                    for i in range(num_workers):
                        start = min_val + (i * step)
                        end = min(start + step, max_val + 1)
                        if start >= end:
                            break

                        task = {
                            "entity": entity,
                            "query": f"SELECT * FROM {entity} WHERE {pk_col} >= :p_start AND {pk_col} < :p_end ORDER BY {pk_col}",
                            "params": {"p_start": start, "p_end": end},
                            "chunk_id": f"range-{i}",
                            "storage_engine": storage_engine,
                            "risk": caps["risk"]
                        }
                        futures.append(
                            executor.submit(self._worker_extract, task, internal_queue, schema_snapshot, json_flatten_config)
                        )
            
            # Yield parallel results
            if can_parallelize:
                active_workers = len(futures)
                completed = 0
                
                while completed < active_workers:
                    try:
                        item = internal_queue.get(timeout=1)
                        if isinstance(item, Exception):
                            raise item
                        yield item
                        internal_queue.task_done()
                        completed += 1
                    except queue.Empty:
                        if all(f.done() for f in futures):
                            while not internal_queue.empty():
                                item = internal_queue.get_nowait()
                                if isinstance(item, Exception):
                                    raise item
                                yield item
                            break
                
                executor.shutdown(wait=True)
                logger.info(f"✅ Parallel extraction completed")
                return

        # ==========================================
        # PATH B: SERIAL EXECUTION
        # ==========================================
        strategy = "serial_safe" if caps["parallel_safe"] else "serial_unsafe"
        logger.info(f"🐢 SERIAL MODE ({strategy})")
        
        if not caps["parallel_safe"]:
            logger.warning(f"⚠️  Using serial due to engine limitations: {storage_engine.upper()}")
        
        conn = self.engine.connect()
        try:
            # Consistent snapshot for MVCC engines
            if caps["mvcc"]:
                conn.execute(text("START TRANSACTION WITH CONSISTENT SNAPSHOT"))
                logger.info("🔒 Consistent snapshot acquired")

            query = text(f"SELECT * FROM {entity}")
            
            # Use PyArrow backend
            chunk_iterator = pd.read_sql(
                query, 
                conn, 
                chunksize=chunk_size,
                dtype_backend="pyarrow"
            )
            
            for i, chunk_df in enumerate(chunk_iterator):
                chunk_df = self._sanitize_and_flatten(chunk_df, json_flatten_config)
                
                yield {
                    "data": chunk_df,
                    "metadata": {
                        "source_type": self.source_type,
                        "entity": entity,
                        "storage_engine": storage_engine,
                        "engine_risk_level": caps["risk"],
                        "mvcc_supported": caps["mvcc"],
                        "strategy": strategy,
                        "chunk_index": i,
                        "row_count": len(chunk_df),
                        "schema_snapshot": schema_snapshot,
                        "extracted_at": datetime.now(timezone.utc).isoformat()
                    }
                }
            
            if caps["mvcc"]:
                conn.execute(text("COMMIT"))
                
        except Exception:
            if caps["mvcc"]:
                conn.execute(text("ROLLBACK"))
            raise
        finally:
            conn.close()

    def close(self) -> None:
        """Clean up connection pool"""
        if self.engine:
            self.engine.dispose()
            logger.info("🔌 MariaDB connection closed")
