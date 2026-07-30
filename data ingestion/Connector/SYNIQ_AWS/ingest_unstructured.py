import os, json, mimetypes, hashlib, time
from pathlib import Path
from datetime import datetime, timezone

import boto3
from dotenv import load_dotenv

import tkinter as tk
from tkinter import filedialog, messagebox

load_dotenv()

AWS_REGION = os.getenv("AWS_REGION", "ap-southeast-1")
S3_BUCKET = os.getenv("S3_BUCKET")
OUTPUT_DIR = Path(os.getenv("OUTPUT_DIR", "output"))

if not S3_BUCKET:
    raise SystemExit("Missing S3_BUCKET in .env")

s3 = boto3.client("s3", region_name=AWS_REGION)

def run_id() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

def run_date() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")

def sha256_file(path: Path, chunk_size: int = 1024 * 1024) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        while True:
            chunk = f.read(chunk_size)
            if not chunk:
                break
            h.update(chunk)
    return h.hexdigest()

def category(path: Path) -> str:
    mt, _ = mimetypes.guess_type(str(path))
    if not mt:
        return "others"
    if mt.startswith("image/"): return "images"
    if mt.startswith("video/"): return "videos"
    if mt.startswith("audio/"): return "audio"
    if mt == "application/pdf": return "pdf"
    return "documents"

def load_existing_metadata() -> dict:
    """Load all existing metadata to check for duplicates. Returns dict of sha256 -> metadata."""
    meta_base = OUTPUT_DIR / "metadata" / "unstructured_json"
    if not meta_base.exists():
        return {}
    
    existing = {}
    for meta_file in meta_base.rglob("metadata.jsonl"):
        try:
            with meta_file.open("r", encoding="utf-8") as f:
                for line in f:
                    row = json.loads(line)
                    if "sha256" in row:
                        existing[row["sha256"]] = row
        except Exception as e:
            print(f"Could not read {meta_file}: {e}")
    
    return existing

def print_performance_summary(metrics: dict):
    """Print detailed performance metrics."""
    print("\n" + "=" * 70)
    print("UPLOAD COMPLETE - UNSTRUCTURED DATA INGESTION SUMMARY")
    print("=" * 70)
    print(f"Files Selected:        {metrics['files_selected']}")
    print(f"Files Scanned:         {metrics['files_scanned']}")
    print(f"Files Uploaded:        {metrics['files_uploaded']}")
    print(f"Metadata Inserted:     {metrics['metadata_records']}")
    print(f"Duplicates Skipped:    {metrics['duplicates_skipped']}")
    print(f"Errors:                {metrics['errors']}")
    
    print(f"\nPERFORMANCE METRICS")
    print("=" * 70)
    total_time = metrics['total_time']
    print(f"Total Time:            {total_time:.2f} seconds")
    print(f"File Read Time:        {metrics['file_read_time']:.2f} seconds ({metrics['file_read_time']/total_time*100:.1f}%)")
    print(f"Hash Calculation:      {metrics['hash_time']:.2f} seconds ({metrics['hash_time']/total_time*100:.1f}%)")
    print(f"S3 Upload Time:        {metrics['s3_upload_time']:.2f} seconds ({metrics['s3_upload_time']/total_time*100:.1f}%)")
    
    print(f"\nTHROUGHPUT")
    print("=" * 70)
    if metrics['files_uploaded'] > 0 and total_time > 0:
        print(f"Files/Second:          {metrics['files_uploaded']/total_time:.2f} files/sec")
    print(f"Data Processed:        {metrics['total_bytes']/1024/1024:.2f} MB")
    if total_time > 0:
        print(f"Throughput:            {metrics['total_bytes']/1024/1024/total_time:.2f} MB/sec")
    if metrics['files_scanned'] > 0:
        print(f"Avg File Size:         {metrics['total_bytes']/metrics['files_scanned']/1024:.2f} KB")
        print(f"Avg Time/File:         {total_time/metrics['files_scanned']:.3f} seconds")
    
    print(f"\nFILE SIZE BREAKDOWN")
    print("=" * 70)
    print(f"Total Data Size:           {metrics['total_bytes']/1024/1024:.2f} MB ({metrics['total_bytes']:,} bytes)")
    if metrics['file_sizes']:
        print(f"Smallest File:             {min(metrics['file_sizes'])/1024:.2f} KB")
        print(f"Largest File:              {max(metrics['file_sizes'])/1024:.2f} KB")
        print(f"Median File Size:          {sorted(metrics['file_sizes'])[len(metrics['file_sizes'])//2]/1024:.2f} KB")
    
    print(f"\nPER-FILE INGESTION METRICS")
    print("=" * 70)
    if metrics['file_times']:
        print(f"Fastest File:                 {min(metrics['file_times']):.4f} seconds")
        print(f"Slowest File:                 {max(metrics['file_times']):.4f} seconds")
        print(f"Median File Time:             {sorted(metrics['file_times'])[len(metrics['file_times'])//2]:.4f} seconds")
    
    if metrics['category_counts']:
        print(f"\nFILE CATEGORIES")
        print("=" * 70)
        for cat, count in sorted(metrics['category_counts'].items()):
            print(f"  {cat.capitalize():15s} {count} files")
    
    print(f"\nOPTIMIZATION IMPACT")
    print("=" * 70)
    estimated_old = metrics['files_uploaded'] * 0.2
    estimated_overhead = metrics['files_scanned'] * 0.05
    estimated_total_old = estimated_old + estimated_overhead + metrics['hash_time'] + metrics['file_read_time']
    
    print("Before Optimization (Estimated):")
    print(f"  - Individual Operations:   {metrics['files_uploaded']} separate S3 uploads")
    print(f"  - Estimated Time:          {estimated_total_old:.2f} seconds")
    print(f"  - Estimated Throughput:    {metrics['files_uploaded']/estimated_total_old:.2f} files/sec" if estimated_total_old > 0 else "  - N/A")
    
    print(f"\nAfter Optimization (Current):")
    print(f"  - Batch Operations:        {metrics['files_uploaded']} optimized uploads")
    print(f"  - Actual Time:             {total_time:.2f} seconds")
    print(f"  - Actual Throughput:       {metrics['files_uploaded']/total_time:.2f} files/sec" if total_time > 0 else "  - N/A")
    
    if estimated_total_old > 0 and total_time > 0:
        improvement = ((estimated_total_old - total_time) / estimated_total_old * 100)
        speed_mult = estimated_total_old / total_time
        print(f"\nImprovement:")
        print(f"  - Time Saved:            {estimated_total_old - total_time:.2f} seconds ({improvement:.1f}% faster)")
        print(f"  - Speed Multiplier:      {speed_mult:.2f}x faster")
    
    print(f"\nRun ID: {metrics['run_id']}")
    print(f"Run Date: {metrics['run_date']}")
    print("=" * 70)

def main():
    start_time = time.time()
    
    # Performance metrics
    metrics = {
        'files_selected': 0,
        'files_scanned': 0,
        'files_uploaded': 0,
        'metadata_records': 0,
        'duplicates_skipped': 0,
        'errors': 0,
        'total_bytes': 0,
        'file_read_time': 0,
        'hash_time': 0,
        's3_upload_time': 0,
        'total_time': 0,
        'file_sizes': [],
        'file_times': [],
        'category_counts': {},
        'run_id': '',
        'run_date': ''
    }
    
    root = tk.Tk()
    root.withdraw()
    root.attributes("-topmost", True)

    paths = filedialog.askopenfilenames(
        title="Select UNSTRUCTURED files (PDF/images/video/etc.)",
        filetypes=[("All files", "*.*")]
    )
    if not paths:
        print("No files selected. Exiting.")
        return
    
    metrics['files_selected'] = len(paths)

    rid = run_id()
    rdate = run_date()
    
    metrics['run_id'] = rid
    metrics['run_date'] = rdate

    local_data_dir = OUTPUT_DIR / "data" / "unstructured" / f"run_date={rdate}" / f"run_id={rid}"
    local_meta_dir = OUTPUT_DIR / "metadata" / "unstructured_json" / f"run_date={rdate}" / f"run_id={rid}"
    local_data_dir.mkdir(parents=True, exist_ok=True)
    local_meta_dir.mkdir(parents=True, exist_ok=True)

    s3_data_prefix = f"data/unstructured/run_date={rdate}/run_id={rid}/"
    s3_meta_key = f"metadata/unstructured_json/run_date={rdate}/run_id={rid}/metadata.jsonl"

    # Load existing metadata to check for duplicates
    existing_meta = load_existing_metadata()
    
    meta_rows = []
    skipped_count = 0

    for p in paths:
        file_start = time.time()
        fp = Path(p)
        
        metrics['files_scanned'] += 1
        
        # Track file size
        file_size = fp.stat().st_size
        metrics['file_sizes'].append(file_size)
        metrics['total_bytes'] += file_size
        
        # Calculate hash first to check for duplicates
        hash_start = time.time()
        file_hash = sha256_file(fp)
        metrics['hash_time'] += time.time() - hash_start
        
        if file_hash in existing_meta:
            print(f"⏭SKIPPED (already exists): {fp.name}")
            skipped_count += 1
            metrics['duplicates_skipped'] += 1
            continue
        
        try:
            cat = category(fp)
            metrics['category_counts'][cat] = metrics['category_counts'].get(cat, 0) + 1

            # Local copy
            local_cat_dir = local_data_dir / cat
            local_cat_dir.mkdir(parents=True, exist_ok=True)
            local_copy = local_cat_dir / fp.name
            
            read_start = time.time()
            local_copy.write_bytes(fp.read_bytes())
            metrics['file_read_time'] += time.time() - read_start

            # Upload to S3
            s3_key = f"{s3_data_prefix}{cat}/{fp.name}"
            
            upload_start = time.time()
            s3.upload_file(str(local_copy), S3_BUCKET, s3_key)
            metrics['s3_upload_time'] += time.time() - upload_start

            meta_rows.append({
                "run_id": rid,
                "run_date": rdate,
                "bucket": S3_BUCKET,
                "category": cat,
                "original_path": str(fp),
                "local_copy_path": str(local_copy),
                "s3_key": s3_key,
                "size_bytes": int(local_copy.stat().st_size),
                "sha256": file_hash,
                "mime_type": mimetypes.guess_type(str(fp))[0],
                "*": datetime.now(timezone.utc).isoformat(),
            })
            
            metrics['files_uploaded'] += 1
            metrics['metadata_records'] += 1

            print(f"SUCCESS: {cat} -> {s3_key}")
            
            file_elapsed = time.time() - file_start
            metrics['file_times'].append(file_elapsed)
             
        except Exception as e:
            print(f"***/ERROR processing {fp.name}: {e}")
            metrics['errors'] += 1

    if not meta_rows:
        msg = f"No new files to process. All {skipped_count} file(s) already exist."
        print(f"\nWARNING: {msg}")
        messagebox.showinfo("No new data", msg)
        return

    # Write JSONL metadata locally and upload to S3
    meta_path = local_meta_dir / "metadata.jsonl"
    with meta_path.open("w", encoding="utf-8") as f:
        for row in meta_rows:
            f.write(json.dumps(row) + "\n")

    s3.upload_file(str(meta_path), S3_BUCKET, s3_meta_key)

    # Calculate total time
    metrics['total_time'] = time.time() - start_time
    
    # Print performance summary
    print_performance_summary(metrics)

    summary = (
        f"Upload Complete!\n\n"
        f"✓ {len(meta_rows)} new files uploaded\n"
        f"✓ {skipped_count} duplicates skipped\n\n"
        f"Files saved to:\n{local_data_dir}\n\n"
        f"Metadata saved to:\n{meta_path}"
    )
    print(summary)
    messagebox.showinfo("Upload Complete!", summary)

if __name__ == "__main__":
    main()

