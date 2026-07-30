import os
import json
import getpass
import logging
import time
import threading
import queue
import multiprocessing
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime
from postgres_connector import PostgresConnector
from mariadb_connector import MariaDBConnector
from mariadbcloud_conn import MariaDBCloudConnector
# from dynamodb_connector import DynamoDBConnector # Uncomment if available

# Configure Logger
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s', datefmt='%H:%M:%S')
logger = logging.getLogger("Orchestrator")

# ==========================================
# 1. SECURITY
# ==========================================
class SecretManager:
    @staticmethod
    def resolve(connection_config):
        if "password" in connection_config: 
            return connection_config["password"]
        if connection_config.get("secret_ref") == "interactive":
            print(f"\n🔒 [Security] Authentication required for user '{connection_config.get('user')}'")
            return getpass.getpass(prompt="    🔑 Enter Password: ")
        raise ValueError("No valid security credentials found.")

# ==========================================
# 2. SINK (Thread-Safe Parquet Writer)
# ==========================================
class ParquetSink:
    def __init__(self, base_path="bronze_layer", run_id=None):
        self.base_path = base_path
        self.run_id = run_id or datetime.now().strftime("%Y%m%d_%H%M%S")
        self.writer = None
        self.total_rows = 0
        self.file_path = None
        self.lock = threading.Lock()

    def append_data(self, payload):
        data_df = payload["data"]
        metadata = payload["metadata"]
        entity = metadata["entity"]
        
        # Initialize file path (once per entity)
        if not self.file_path:
            entity_folder = os.path.join(self.base_path, entity, self.run_id)
            os.makedirs(entity_folder, exist_ok=True)
            self.file_path = os.path.join(entity_folder, "data.parquet")

        table = pa.Table.from_pandas(data_df)

        with self.lock:
            if self.writer is None:
                self.writer = pq.ParquetWriter(
                    self.file_path, 
                    table.schema, 
                    compression='snappy'
                )
            self.writer.write_table(table)
            self.total_rows += len(data_df)
        
        return os.path.dirname(self.file_path)

    def close(self):
        with self.lock:
            if self.writer:
                self.writer.close()
                self.writer = None

    def write_metadata(self, folder_path, last_metadata):
        self.close() 
        final_metadata = last_metadata.copy()
        final_metadata["row_count_actual"] = self.total_rows
        final_metadata["format"] = "parquet"
        final_metadata["compression"] = "snappy"
        final_metadata["run_id"] = self.run_id
        final_metadata.pop("chunk_index", None) 
        
        meta_path = os.path.join(folder_path, "metadata.json")
        with open(meta_path, "w") as f:
            json.dump(final_metadata, f, indent=4)
        logger.info(f"💾 Saved Metadata + Parquet to: {folder_path}")

# ==========================================
# 3. ASYNC WRITER WORKER
# ==========================================
def writer_worker(data_queue, sink, result_container):
    logger.info("   👷 Writer Thread Started")
    output_folder = None
    last_meta = None
    
    while True:
        try:
            # Block until data available
            payload = data_queue.get()
            
            # 1. Sentinel Check: Job complete
            if payload is None:
                data_queue.task_done()
                break
            
            # 2. CRITICAL FIX: Skip Empty Chunks
            # This prevents schema mismatch errors when workers return 0 rows
            if payload["data"].empty:
                # logger.debug(f"   ⚠️ Skipping empty chunk {payload['metadata']['chunk_index']}")
                data_queue.task_done()
                continue

            # 3. Write chunk to disk
            chunk_idx = payload["metadata"]["chunk_index"]
            rows = payload["metadata"]["row_count"]
            
            start_write = time.time()
            output_folder = sink.append_data(payload)
            duration = time.time() - start_write
            
            logger.info(f"   💾 Wrote Chunk {chunk_idx} ({rows} rows) in {duration:.2f}s")
            
            last_meta = payload["metadata"]
            data_queue.task_done()
            
        except Exception as e:
            logger.error(f"❌ Writer Thread Error: {e}")
            result_container["error"] = e
            break
    
    result_container["output_folder"] = output_folder
    result_container["last_meta"] = last_meta
    logger.info("   👷 Writer Thread Finished")

# ==========================================
# 4. HELPER FUNCTIONS FOR GUI
# ==========================================
def get_row_count(connector, table_name):
    """Fast row count estimation for GUI"""
    try:
        with connector.engine.connect() as conn:
            from sqlalchemy import text
            query = text("SELECT reltuples::bigint FROM pg_class WHERE relname = :t")
            result = conn.execute(query, {"t": table_name}).scalar()
            if result and result > 0:
                return int(result)
            # Fallback to actual count
            query = text(f"SELECT COUNT(*) FROM {table_name}")
            return conn.execute(query).scalar()
    except Exception as e:
        logger.warning(f"⚠️ Row count failed: {e}")
        return 100000

class ExtractionOptimizer:
    """Simple optimizer for GUI use"""
    
    def __init__(self):
        self.cpu_cores = multiprocessing.cpu_count()
    
    def calculate_optimal_params(self, row_count, avg_row_size_bytes=1024, is_remote=True):
        """Calculate chunk size and workers"""
        if row_count == 0:
            return 10000, 1
        
        # Worker count
        if row_count < 2_000_000:
            num_workers = 1
        elif row_count < 10_000_000:
            num_workers = 4
        else:
            num_workers = min(8, self.cpu_cores)
        
        # Chunk size
        if row_count < 10_000:
            chunk_size = row_count
        elif row_count < 1_000_000:
            min_chunks = num_workers * 4
            chunk_size = max(10_000, row_count // min_chunks)
        else:
            chunk_size = 50_000
        
        # Safety cap
        max_chunk = int((500 * 1024 * 1024) / avg_row_size_bytes)
        chunk_size = min(chunk_size, max_chunk)
        
        # Final check
        total_chunks = (row_count + chunk_size - 1) // chunk_size
        if total_chunks < num_workers:
            num_workers = max(1, total_chunks)
        
        return int(chunk_size), int(num_workers)
    
    def get_optimization_summary(self, row_count, chunk_size, num_workers):
        """Format summary for GUI"""
        total_chunks = (row_count + chunk_size - 1) // chunk_size
        est_time = row_count / 50000  # ~50k rows/sec estimate
        
        return {
            "table_rows": f"{row_count:,}",
            "chunk_size": f"{chunk_size:,}",
            "num_workers": num_workers,
            "total_chunks": total_chunks,
            "estimated_time": f"~{est_time:.1f}s"
        }

def enhance_extraction_config(extraction_request):
    """Ensure extraction config has all required fields"""
    if extraction_request.get("enable_parallel", False):
        cpu_cores = multiprocessing.cpu_count()
        max_workers = min(4, cpu_cores)
        extraction_request.setdefault("num_workers", max_workers)
        extraction_request.setdefault("partition_column", "id")
        logger.info(f"⚡ Parallel Mode: {extraction_request['num_workers']} workers")
    return extraction_request

# ==========================================
# 5. ORCHESTRATOR LOGIC
# ==========================================
def get_connector(source_type, config):
    if source_type == "postgres":
        return PostgresConnector(config, secret_handler=SecretManager)
    elif source_type == "mariadb":
        return MariaDBConnector(config, secret_handler=SecretManager)
    elif source_type == "mariadb_cloud":
        return MariaDBCloudConnector(config, secret_handler=SecretManager)
    raise ValueError(f"Unknown source type: {source_type}")

def main():
    config_file = "config.json"
    
    if os.path.exists(config_file):
        logger.info(f"📂 Found '{config_file}' - Running in Automated Mode")
        try:
            with open(config_file, "r") as f:
                user_intent = json.load(f)
        except Exception as e:
            logger.error(f"❌ Failed to read config file: {e}")
            return
    else:
        logger.error("⚠️ No config.json found.")
        return

    logger.info("🚀 Async Orchestrator Initialized")
    
    user_intent["extraction_request"] = enhance_extraction_config(user_intent["extraction_request"])
    
    connector = get_connector(user_intent["source_type"], user_intent["connection_config"])
    sink = ParquetSink(base_path="bronze_layer")

    data_queue = queue.Queue(maxsize=5)
    worker_results = {}
    
    writer_thread = threading.Thread(
        target=writer_worker,
        args=(data_queue, sink, worker_results),
        daemon=True,
        name="WriterThread"
    )
    writer_thread.start()

    try:
        connector.connect()
        connector.validate_credentials()
        
        logger.info("🌊 Starting Extraction Stream...")
        stream = connector.extract(user_intent["extraction_request"])
        
        for payload in stream:
            while True:
                try:
                    data_queue.put(payload, timeout=2)
                    logger.info(f"   📤 Queued Chunk {payload['metadata']['chunk_index']}")
                    break
                except queue.Full:
                    if not writer_thread.is_alive():
                        if "error" in worker_results: raise worker_results["error"]
                        else: raise RuntimeError("Writer thread died unexpectedly")
                    logger.debug("   ⏳ Waiting for writer capacity...")

        data_queue.put(None)
        logger.info("⏳ Waiting for writer to finish...")
        
        writer_thread.join(timeout=300)
        
        if writer_thread.is_alive():
            raise TimeoutError("Writer thread timed out")

        if "error" in worker_results:
            raise worker_results["error"]
        
        if worker_results.get("output_folder"):
            sink.write_metadata(worker_results["output_folder"], worker_results["last_meta"])
        
        logger.info("✅ Pipeline Complete!")

    except KeyboardInterrupt:
        logger.warning("⚠️ Interrupted (Ctrl+C)")
    except Exception as e:
        logger.error(f"❌ Pipeline Failed: {e}")
        import traceback
        logger.error(traceback.format_exc())
    finally:
        sink.close()
        connector.close()
        logger.info("🔌 Cleanup Done")

if __name__ == "__main__":
    main()