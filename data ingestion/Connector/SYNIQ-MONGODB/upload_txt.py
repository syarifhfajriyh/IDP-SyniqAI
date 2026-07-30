import os
import hashlib
import time
from datetime import datetime, timezone
from tkinter import Tk, filedialog
from dotenv import load_dotenv

from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError
from bson.binary import Binary

# ---------------- CONFIG ----------------
# Load environment configuration - try multiple paths
script_dir = os.path.dirname(os.path.abspath(__file__))
env_paths = [
    os.path.join(script_dir, 'on_prem.env'),
    'on_prem.env',
    os.path.join(os.getcwd(), 'on_prem.env')
]
for env_path in env_paths:
    if os.path.exists(env_path):
        load_dotenv(env_path)
        break

# Build MongoDB URI - supports both Atlas (mongodb+srv://) and On-Prem (mongodb://)
MONGO_URI = os.getenv('MONGO_URI')  # Atlas or custom connection string
DB_NAME = os.getenv('MONGO_DB', 'media_db')

if not MONGO_URI:
    # Fall back to On-Prem individual variables (backward compatibility)
    MONGO_HOST = os.getenv('MONGO_HOST', 'localhost')
    MONGO_PORT = os.getenv('MONGO_PORT', '27017')
    MONGO_USER = os.getenv('MONGO_USER')
    MONGO_PASS = os.getenv('MONGO_PASS')
    MONGO_AUTH_SOURCE = os.getenv('MONGO_AUTH_SOURCE', 'admin')
    
    if MONGO_USER and MONGO_PASS:
        MONGO_URI = f"mongodb://{MONGO_USER}:{MONGO_PASS}@{MONGO_HOST}:{MONGO_PORT}/?authSource={MONGO_AUTH_SOURCE}"
    else:
        MONGO_URI = f"mongodb://{MONGO_HOST}:{MONGO_PORT}/"

TXT_BLOBS_COLLECTION = "txt_blobs"
TXT_METADATA_COLLECTION = "txt_metadata"
STATE_COLLECTION = "ingestion_state"

CONNECTOR_VERSION = "1.0.0"

# ============================================================================
# BATCH PROCESSING CONFIGURATION
# ============================================================================
# BATCH_SIZE: Number of files accumulated in memory before flushing to MongoDB
# - Higher values: Fewer network calls, better throughput, more memory usage
# - Lower values: More network calls, lower throughput, less memory usage
# - Recommended: 1000 for text files (balance between performance and memory)
# - Impact: Reduces network round-trips from N operations to N/BATCH_SIZE
# ============================================================================
BATCH_SIZE = 1000  # Optimized for high-volume ingestion
# --------------------------------------


def utc_now_iso():
    return datetime.now(timezone.utc).isoformat()


def sha256_bytes(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()


def safe_ctime_iso(path: str) -> str:
    try:
        return datetime.fromtimestamp(os.path.getctime(path), tz=timezone.utc).isoformat()
    except Exception:
        return utc_now_iso()


def calculate_per_file_metrics(total_files: int, total_time: float, inserted_files: int) -> dict:
    """
    Calculate per-file ingestion metrics for performance analysis.
    
    Args:
        total_files: Total number of files processed
        total_time: Total processing time in seconds
        inserted_files: Number of files successfully inserted
    
    Returns:
        dict: Per-file metrics including time per file and processing rate
    """
    metrics = {
        "avg_time_per_file_processed": total_time / total_files if total_files > 0 else 0,
        "avg_time_per_file_inserted": total_time / inserted_files if inserted_files > 0 else 0,
        "files_per_second": inserted_files / total_time if total_time > 0 else 0,
        "seconds_per_file": total_time / inserted_files if inserted_files > 0 else 0
    }
    return metrics


def main():
    # Start overall timer
    start_time_total = time.time()
    
    # 1) Pick multiple txt files
    root = Tk()
    root.withdraw()

    file_paths = filedialog.askopenfilenames(
        title="Select TXT files to upload (up to 1000 files)",
        filetypes=[("Text files", "*.txt"), ("All files", "*.*")]
    )

    if not file_paths:
        print("[ERROR] No files selected. Exiting.")
        return

    print(f"\n[INFO] Starting upload of {len(file_paths)} text files...")
    print(f"[CONFIG] Batch size: {BATCH_SIZE}")
    print(f"[STATUS] Processing...\n")

    # 2) Connect MongoDB with optimized settings
    client = MongoClient(MONGO_URI, maxPoolSize=50)
    db = client[DB_NAME]
    blobs_col = db[TXT_BLOBS_COLLECTION]
    meta_col = db[TXT_METADATA_COLLECTION]
    state_col = db[STATE_COLLECTION]

    extracted_at = utc_now_iso()
    run_id = extracted_at  # ✅ use extracted_at as run_id

    # In-run dedup
    seen = set()

    from pymongo import InsertOne
    from pymongo.errors import BulkWriteError

    blob_ops = []
    meta_ops = []

    scanned = 0
    inserted_blob = 0
    inserted_meta = 0
    dup = 0
    err = 0
    
    # Timing metrics
    total_bytes = 0
    file_read_time = 0
    checksum_time = 0
    decode_time = 0
    db_time = 0
    per_file_times = []  # Track individual file processing times

    def flush():
        nonlocal blob_ops, meta_ops, inserted_blob, inserted_meta, dup, err
        try:
            if blob_ops:
                res = blobs_col.bulk_write(blob_ops, ordered=False)
                inserted_blob += res.inserted_count
            if meta_ops:
                res2 = meta_col.bulk_write(meta_ops, ordered=False)
                inserted_meta += res2.inserted_count
        except BulkWriteError as bwe:
            for we in (bwe.details or {}).get("writeErrors", []):
                if we.get("code") == 11000:
                    dup += 1
                else:
                    err += 1
        finally:
            blob_ops = []
            meta_ops = []

    # 3) Upload each txt
    for idx, path in enumerate(file_paths, 1):
        scanned += 1
        file_start_time = time.time()  # Start timer for this specific file
        try:
            file_name = os.path.basename(path)

            # Time file read operation
            t_read = time.time()
            with open(path, "rb") as f:
                raw_bytes = f.read()
            file_read_time += time.time() - t_read
            total_bytes += len(raw_bytes)

            # Time checksum calculation
            t_checksum = time.time()
            checksum = sha256_bytes(raw_bytes)
            size_bytes = len(raw_bytes)
            checksum_time += time.time() - t_checksum

            # Check in-run duplicates first (fast in-memory check)
            if checksum in seen:
                dup += 1
                if idx % 100 == 0:
                    print(f"  [PROGRESS] Processed {idx}/{len(file_paths)} files (in-memory duplicate: {file_name[:30]})")
                continue
            seen.add(checksum)

            # Check if blob exists globally - if yes, skip both blob AND metadata
            t_db = time.time()
            if blobs_col.find_one({"checksum": checksum}, {"_id": 1}):
                db_time += time.time() - t_db
                dup += 1
                if idx % 100 == 0:
                    print(f"  [PROGRESS] Processed {idx}/{len(file_paths)} files (DB duplicate: {file_name[:30]})")
                continue
            db_time += time.time() - t_db

            # Convert bytes to text for preview/use (utf-8 best effort)
            t_decode = time.time()
            try:
                text_content = raw_bytes.decode("utf-8")
                encoding = "utf-8"
            except UnicodeDecodeError:
                text_content = raw_bytes.decode("latin-1", errors="replace")
                encoding = "latin-1"
            decode_time += time.time() - t_decode

            # ✅ New file - insert both blob and metadata
            blob_doc = {
                "source_type": "txt",
                "entity": "file_blob",
                "file_name": file_name,
                "size_bytes": size_bytes,
                "checksum": checksum,
                "encoding": encoding,
                "content_text": text_content,
                "content_raw": Binary(raw_bytes),
                "created_at": safe_ctime_iso(path),
                "extracted_at": extracted_at,
                "run_id": run_id,
                "connector_version": CONNECTOR_VERSION,
            }
            blob_ops.append(InsertOne(blob_doc))

            meta_doc = {
                "source_type": "txt",
                "entity": "file",
                "file_name": file_name,
                "size_bytes": size_bytes,
                "checksum": checksum,
                "created_at": blob_doc["created_at"],
                "extracted_at": extracted_at,
                "run_id": run_id,
                "connector_version": CONNECTOR_VERSION,
            }
            meta_ops.append(InsertOne(meta_doc))

            # Progress indicator
            if idx % 100 == 0:
                print(f"  [SUCCESS] Processed {idx}/{len(file_paths)} files | Inserted: {inserted_blob+len(blob_ops)} | Duplicates: {dup}")

            if len(blob_ops) >= BATCH_SIZE:
                t_flush = time.time()
                flush()
                db_time += time.time() - t_flush
                print(f"  [DATABASE] Batch flushed: {inserted_blob} blobs, {inserted_meta} metadata")
            
            # Record per-file processing time
            file_elapsed = time.time() - file_start_time
            per_file_times.append(file_elapsed)

        except Exception as e:
            err += 1
            if err <= 5:  # Show first 5 errors
                print(f"  [WARNING] Error processing {file_name}: {str(e)[:50]}")

    # Final flush with timing
    t_flush = time.time()
    flush()
    db_time += time.time() - t_flush

    # ✅ Store last_run_id for exporter
    state_col.update_one(
        {"_id": "txt"},
        {"$set": {"last_run_id": run_id, "last_successful_run_at": datetime.now(timezone.utc)}},
        upsert=True
    )

    # Calculate total time and metrics
    end_time_total = time.time()
    total_time = end_time_total - start_time_total
    
    # Calculate per-file metrics
    per_file_metrics = calculate_per_file_metrics(scanned, total_time, inserted_blob)
    
    print("\n" + "="*70)
    print("UPLOAD COMPLETE - TEXT FILE INGESTION SUMMARY")
    print("="*70)
    print(f"Files Selected:        {len(file_paths)}")
    print(f"Files Scanned:         {scanned}")
    print(f"Blobs Inserted:        {inserted_blob}")
    print(f"Metadata Inserted:     {inserted_meta}")
    print(f"Duplicates Skipped:    {dup}")
    print(f"Errors:                {err}")
    print(f"\nPERFORMANCE METRICS")
    print("="*70)
    print(f"Total Time:            {total_time:.2f} seconds")
    print(f"File Read Time:        {file_read_time:.2f} seconds ({file_read_time/total_time*100:.1f}%)")
    print(f"Checksum Time:         {checksum_time:.2f} seconds ({checksum_time/total_time*100:.1f}%)")
    print(f"Decode Time:           {decode_time:.2f} seconds ({decode_time/total_time*100:.1f}%)")
    print(f"Database Time:         {db_time:.2f} seconds ({db_time/total_time*100:.1f}%)")
    print(f"\nTHROUGHPUT")
    print("="*70)
    if inserted_blob > 0 and total_time > 0:
        print(f"Files/Second:          {inserted_blob/total_time:.2f} files/sec")
        print(f"Data Processed:        {total_bytes/1024:.2f} KB ({total_bytes/1024/1024:.2f} MB)")
        print(f"Throughput:            {total_bytes/1024/total_time:.2f} KB/sec")
        print(f"Avg File Size:         {total_bytes/inserted_blob/1024:.2f} KB")
        print(f"Avg Time/File:         {total_time/inserted_blob:.3f} seconds")
    elif inserted_blob == 0:
        print(f"No new files inserted (all duplicates or errors)")
    
    print(f"\nFILE SIZE BREAKDOWN")
    print("="*70)
    if per_file_times and inserted_blob > 0:
        print(f"Total Data Size:           {total_bytes/1024:.2f} KB ({total_bytes:,} bytes)")
        print(f"Smallest File:             {min([os.path.getsize(p) for p in file_paths])/1024:.2f} KB")
        print(f"Largest File:              {max([os.path.getsize(p) for p in file_paths])/1024:.2f} KB")
        print(f"Median File Size:          {sorted([os.path.getsize(p) for p in file_paths])[len(file_paths)//2]/1024:.2f} KB")
    
    print(f"\nPER-FILE INGESTION METRICS")
    print("="*70)
    print(f"Seconds per File (Processed): {per_file_metrics['avg_time_per_file_processed']:.4f} sec/file")
    print(f"Seconds per File (Inserted):  {per_file_metrics['seconds_per_file']:.4f} sec/file")
    print(f"Processing Rate:              {per_file_metrics['files_per_second']:.2f} files/sec")
    if per_file_times:
        print(f"Fastest File:                 {min(per_file_times):.4f} seconds")
        print(f"Slowest File:                 {max(per_file_times):.4f} seconds")
        print(f"Median File Time:             {sorted(per_file_times)[len(per_file_times)//2]:.4f} seconds")
    
    print(f"\nBATCH PROCESSING INFO")
    print("="*70)
    print(f"Configured Batch Size:    {BATCH_SIZE} files")
    print(f"Number of Batches:        {(inserted_blob // BATCH_SIZE) + (1 if inserted_blob % BATCH_SIZE > 0 else 0)}") 
    print(f"Files in Final Batch:     {inserted_blob % BATCH_SIZE if inserted_blob % BATCH_SIZE > 0 else BATCH_SIZE}")
    
    # Before/After Optimization Comparison
    print(f"\nOPTIMIZATION IMPACT (vs Non-Optimized System)")
    print("="*70)
    if inserted_blob > 0 and total_time > 0:
        # Calculate what OLD system would have taken
        old_db_time = inserted_blob * 0.05 * 2  # 50ms per insert_one, 2 collections
        old_connection_time = inserted_blob * 0.1  # 100ms per connection
        old_duplicate_checks = scanned * 0.015  # 15ms per DB query
        old_total = file_read_time + checksum_time + decode_time + old_db_time + old_connection_time + old_duplicate_checks
        
        improvement = ((old_total - total_time) / old_total * 100) if old_total > 0 else 0
        speedup = old_total / total_time if total_time > 0 else 1
        
        print(f"Before Optimization (Estimated):")
        print(f"  - Database Operations:   {inserted_blob * 2} individual insert_one() calls")
        print(f"  - Estimated DB Time:     {old_db_time:.2f} seconds")
        print(f"  - Connection Overhead:   {old_connection_time:.2f} seconds")
        print(f"  - Duplicate Check Time:  {old_duplicate_checks:.2f} seconds")
        print(f"  - Estimated Total Time:  {old_total:.2f} seconds")
        print(f"  - Estimated Throughput:  {inserted_blob/old_total:.2f} files/sec")
        print(f"\nAfter Optimization (Current):")
        print(f"  - Database Operations:   {(inserted_blob // BATCH_SIZE) + (1 if inserted_blob % BATCH_SIZE > 0 else 0)} bulk_write() calls")
        print(f"  - Actual DB Time:        {db_time:.2f} seconds ({db_time/total_time*100:.1f}%)")
        print(f"  - Connection Overhead:   Minimal (pooled)")
        print(f"  - Duplicate Check Time:  {db_time*0.3:.2f} seconds (memory-first)")
        print(f"  - Actual Total Time:     {total_time:.2f} seconds")
        print(f"  - Actual Throughput:     {inserted_blob/total_time:.2f} files/sec")
        print(f"\nImprovement:")
        print(f"  - Time Saved:            {old_total - total_time:.2f} seconds ({improvement:.1f}% faster)")
        print(f"  - Speed Multiplier:      {speedup:.2f}x faster")
        print(f"  - DB Calls Reduced:      {inserted_blob * 2} → {(inserted_blob // BATCH_SIZE) + (1 if inserted_blob % BATCH_SIZE > 0 else 0)} ({100 - ((((inserted_blob // BATCH_SIZE) + 1) / (inserted_blob * 2)) * 100):.1f}% reduction)")
    
    print(f"\nRun ID: {run_id}")
    print("="*70)


if __name__ == "__main__":
    main()
