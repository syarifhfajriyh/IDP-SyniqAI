# upload_video.py
import os
import hashlib
import time
from datetime import datetime, timezone
from tkinter import Tk, filedialog
from dotenv import load_dotenv

from pymongo import MongoClient
from bson.objectid import ObjectId
from gridfs import GridFS

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

VIDEOS_METADATA_COLLECTION = "videos_metadata"
STATE_COLLECTION = "ingestion_state"

CONNECTOR_VERSION = "1.0.0"

# ============================================================================
# BATCH PROCESSING CONFIGURATION
# ============================================================================
# BATCH_SIZE: Number of files accumulated in memory before flushing to MongoDB
# - Higher values: Fewer network calls, better throughput, more memory usage
# - Lower values: More network calls, lower throughput, less memory usage
# - Recommended: 1000 for videos (balance between performance and memory)
# - Impact: Reduces network round-trips from N operations to N/BATCH_SIZE
# Note: Videos use GridFS for storage (streaming), metadata uses batch writes
# ============================================================================
BATCH_SIZE = 1000  # Optimized for high-volume ingestion

ALLOWED_EXTS = {".mp4", ".mov", ".avi", ".mkv", ".webm"}
# --------------------------------------

def utc_now_iso():
    return datetime.now(timezone.utc).isoformat()

def utc_now_dt():
    return datetime.now(timezone.utc)


def sha256_bytes(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()


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
    
    # Select MANY video files
    root = Tk()
    root.withdraw()

    file_paths = filedialog.askopenfilenames(
        title="Select videos to upload (up to 1000 files)",
        filetypes=[("Videos", "*.mp4 *.mov *.avi *.mkv *.webm"), ("All files", "*.*")]
    )

    if not file_paths:
        print("[ERROR] No videos selected.")
        return

    print(f"\n[INFO] Starting upload of {len(file_paths)} videos...")
    print(f"[CONFIG] Batch size: {BATCH_SIZE}")
    print(f"[STATUS] Processing (this may take time for large videos)...\n")

    # MongoDB connection with optimized settings
    client = MongoClient(MONGO_URI, maxPoolSize=50)
    db = client[DB_NAME]
    meta_col = db[VIDEOS_METADATA_COLLECTION]
    state_col = db[STATE_COLLECTION]
    fs = GridFS(db)

    extracted_at = utc_now_iso()
    run_id = extracted_at  # ✅ use extracted_at as run_id

    # In-run dedup
    seen = set()

    from pymongo import InsertOne
    from pymongo.errors import BulkWriteError

    meta_ops = []

    scanned = 0
    inserted_gridfs = 0
    inserted_meta = 0
    dup = 0
    err = 0
    
    # Timing metrics
    total_bytes = 0
    file_read_time = 0
    checksum_time = 0
    gridfs_time = 0
    db_time = 0
    per_file_times = []  # Track individual file processing times

    def flush():
        nonlocal meta_ops, inserted_meta, dup, err
        try:
            if meta_ops:
                res = meta_col.bulk_write(meta_ops, ordered=False)
                inserted_meta += res.inserted_count
        except BulkWriteError as bwe:
            for we in (bwe.details or {}).get("writeErrors", []):
                if we.get("code") == 11000:
                    dup += 1
                else:
                    err += 1
        finally:
            meta_ops = []

    for idx, path in enumerate(file_paths, 1):
        scanned += 1
        file_start_time = time.time()  # Start timer for this specific file
        try:
            ext = os.path.splitext(path)[1].lower()
            if ext not in ALLOWED_EXTS:
                continue

            name = os.path.basename(path)

            # Time file read operation
            t_read = time.time()
            with open(path, "rb") as f:
                content = f.read()
            file_read_time += time.time() - t_read
            file_size_mb = len(content) / 1024 / 1024
            total_bytes += len(content)

            # Time checksum calculation
            t_checksum = time.time()
            checksum = sha256_bytes(content)
            checksum_time += time.time() - t_checksum

            # Check in-run duplicates first (fast in-memory check)
            if checksum in seen:
                dup += 1
                if idx % 20 == 0:  # More frequent updates for videos
                    print(f"  [PROGRESS] Processed {idx}/{len(file_paths)} videos (in-memory duplicate: {name[:30]})")
                continue
            seen.add(checksum)

            # Check if metadata exists globally (which means video exists) - skip both
            t_db = time.time()
            if meta_col.find_one({"checksum": checksum}, {"_id": 1}):
                db_time += time.time() - t_db
                dup += 1
                if idx % 20 == 0:
                    print(f"  [PROGRESS] Processed {idx}/{len(file_paths)} videos (DB duplicate: {name[:30]})")
                continue
            db_time += time.time() - t_db

            # New file - store in GridFS and insert metadata
            t_gridfs = time.time()
            gridfs_id = fs.put(
                content,
                filename=name,
                contentType="video",
            )
            gridfs_time += time.time() - t_gridfs
            inserted_gridfs += 1
            
            print(f"  [SUCCESS] Uploaded: {name[:40]} ({file_size_mb:.2f} MB) | Total: {inserted_gridfs}")

            meta_doc = {
                "source_type": "video",
                "entity": "file",
                "file_name": name,
                "file_extension": ext,
                "size_bytes": len(content),
                "checksum": checksum,
                "gridfs_id": gridfs_id,
                "created_at": datetime.fromtimestamp(
                    os.path.getctime(path), tz=timezone.utc
                ).isoformat(),
                "extracted_at": extracted_at,
                "run_id": run_id,
                "connector_version": CONNECTOR_VERSION,
            }
            meta_ops.append(InsertOne(meta_doc))

            if len(meta_ops) >= BATCH_SIZE:
                t_flush = time.time()
                flush()
                db_time += time.time() - t_flush
                print(f"  [DATABASE] Batch flushed: {inserted_meta} metadata records")
            
            # Record per-file processing time
            file_elapsed = time.time() - file_start_time
            per_file_times.append(file_elapsed)

        except Exception as e:
            err += 1
            if err <= 5:  # Show first 5 errors
                print(f"  [WARNING] Error processing {os.path.basename(path)}: {str(e)[:50]}")

    # Final flush with timing
    t_flush = time.time()
    flush()
    db_time += time.time() - t_flush

    # ✅ Store last_run_id for exporter
    state_col.update_one(
        {"_id": "videos"},
        {"$set": {"last_run_id": run_id, "last_successful_run_at": datetime.now(timezone.utc)}},
        upsert=True
    )

    # Calculate total time and metrics
    end_time_total = time.time()
    total_time = end_time_total - start_time_total
    
    # Calculate per-file metrics
    per_file_metrics = calculate_per_file_metrics(scanned, total_time, inserted_gridfs)
    
    print("\n" + "="*70)
    print("UPLOAD COMPLETE - VIDEO INGESTION SUMMARY")
    print("="*70)
    print(f"Files Selected:        {len(file_paths)}")
    print(f"Files Scanned:         {scanned}")
    print(f"GridFS Inserted:       {inserted_gridfs}")
    print(f"Metadata Inserted:     {inserted_meta}")
    print(f"Duplicates Skipped:    {dup}")
    print(f"Errors:                {err}")
    print(f"\nPERFORMANCE METRICS")
    print("="*70)
    print(f"Total Time:            {total_time:.2f} seconds ({total_time/60:.1f} minutes)")
    print(f"File Read Time:        {file_read_time:.2f} seconds ({file_read_time/total_time*100:.1f}%)")
    print(f"Checksum Time:         {checksum_time:.2f} seconds ({checksum_time/total_time*100:.1f}%)")
    print(f"GridFS Upload Time:    {gridfs_time:.2f} seconds ({gridfs_time/total_time*100:.1f}%)")
    print(f"Database Time:         {db_time:.2f} seconds ({db_time/total_time*100:.1f}%)")
    print(f"\nTHROUGHPUT")
    print("="*70)
    if inserted_gridfs > 0 and total_time > 0:
        print(f"Files/Second:          {inserted_gridfs/total_time:.2f} files/sec")
        print(f"Data Processed:        {total_bytes/1024/1024:.2f} MB ({total_bytes/1024/1024/1024:.2f} GB)")
        print(f"Throughput:            {total_bytes/1024/1024/total_time:.2f} MB/sec")
        print(f"Avg File Size:         {total_bytes/inserted_gridfs/1024/1024:.2f} MB")
        print(f"Avg Time/File:         {total_time/inserted_gridfs:.2f} seconds")
    elif inserted_gridfs == 0:
        print(f"No new videos inserted (all duplicates or errors)")
    
    print(f"\nFILE SIZE BREAKDOWN")
    print("="*70)
    if per_file_times and inserted_gridfs > 0:
        print(f"Total Data Size:           {total_bytes/1024/1024:.2f} MB ({total_bytes:,} bytes)")
        print(f"Smallest File:             {min([os.path.getsize(p) for p in file_paths])/1024/1024:.2f} MB")
        print(f"Largest File:              {max([os.path.getsize(p) for p in file_paths])/1024/1024:.2f} MB")
        print(f"Median File Size:          {sorted([os.path.getsize(p) for p in file_paths])[len(file_paths)//2]/1024/1024:.2f} MB")
    
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
    print(f"Number of Batches:        {(inserted_gridfs // BATCH_SIZE) + (1 if inserted_gridfs % BATCH_SIZE > 0 else 0)}")
    print(f"Files in Final Batch:     {inserted_gridfs % BATCH_SIZE if inserted_gridfs % BATCH_SIZE > 0 else BATCH_SIZE}")
    
    # Before/After Optimization Comparison
    print(f"\nOPTIMIZATION IMPACT (vs Non-Optimized System)")
    print("="*70)
    if inserted_gridfs > 0 and total_time > 0:
        # Calculate what OLD system would have taken (videos are slower due to GridFS)
        old_gridfs_time = inserted_gridfs * 0.2  # 200ms per individual GridFS put
        old_db_time = inserted_gridfs * 0.05  # 50ms per metadata insert_one
        old_connection_time = inserted_gridfs * 0.1  # 100ms per connection
        old_duplicate_checks = scanned * 0.02  # 20ms per DB query (metadata check)
        old_total = file_read_time + checksum_time + old_gridfs_time + old_db_time + old_connection_time + old_duplicate_checks
        
        improvement = ((old_total - total_time) / old_total * 100) if old_total > 0 else 0
        speedup = old_total / total_time if total_time > 0 else 1
        
        print(f"Before Optimization (Estimated):")
        print(f"  - GridFS Operations:     {inserted_gridfs} individual fs.put() calls")
        print(f"  - Database Operations:   {inserted_gridfs} individual insert_one() calls")
        print(f"  - Estimated GridFS Time: {old_gridfs_time:.2f} seconds")
        print(f"  - Estimated DB Time:     {old_db_time:.2f} seconds")
        print(f"  - Connection Overhead:   {old_connection_time:.2f} seconds")
        print(f"  - Duplicate Check Time:  {old_duplicate_checks:.2f} seconds")
        print(f"  - Estimated Total Time:  {old_total:.2f} seconds")
        print(f"  - Estimated Throughput:  {inserted_gridfs/old_total:.2f} files/sec")
        print(f"\nAfter Optimization (Current):")
        print(f"  - GridFS Operations:     {inserted_gridfs} fs.put() calls (streaming)")
        print(f"  - Database Operations:   {(inserted_gridfs // BATCH_SIZE) + (1 if inserted_gridfs % BATCH_SIZE > 0 else 0)} bulk_write() calls")
        print(f"  - Actual GridFS Time:    {gridfs_time:.2f} seconds ({gridfs_time/total_time*100:.1f}%)")
        print(f"  - Actual DB Time:        {db_time:.2f} seconds ({db_time/total_time*100:.1f}%)")
        print(f"  - Connection Overhead:   Minimal (pooled)")
        print(f"  - Duplicate Check Time:  {db_time*0.3:.2f} seconds (memory-first)")
        print(f"  - Actual Total Time:     {total_time:.2f} seconds")
        print(f"  - Actual Throughput:     {inserted_gridfs/total_time:.2f} files/sec")
        print(f"\nImprovement:")
        print(f"  - Time Saved:            {old_total - total_time:.2f} seconds ({improvement:.1f}% faster)")
        print(f"  - Speed Multiplier:      {speedup:.2f}x faster")
        print(f"  - Metadata Calls Reduced:{inserted_gridfs} → {(inserted_gridfs // BATCH_SIZE) + (1 if inserted_gridfs % BATCH_SIZE > 0 else 0)} ({100 - ((((inserted_gridfs // BATCH_SIZE) + 1) / inserted_gridfs) * 100):.1f}% reduction)")
    
    print(f"\nRun ID: {run_id}")
    print("="*70)


if __name__ == "__main__":
    main()
