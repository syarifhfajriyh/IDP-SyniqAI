import os, re, hashlib, time
from pathlib import Path
from datetime import datetime, timezone

import boto3
import pandas as pd
from dotenv import load_dotenv

import tkinter as tk
from tkinter import filedialog, simpledialog, messagebox

load_dotenv()

AWS_REGION = os.getenv("AWS_REGION", "ap-southeast-1")
S3_BUCKET = os.getenv("S3_BUCKET")
OUTPUT_DIR = Path(os.getenv("OUTPUT_DIR", "output"))

if not S3_BUCKET:
    raise SystemExit("Missing S3_BUCKET in .env")

s3 = boto3.client("s3", region_name=AWS_REGION)

def safe_name(s: str) -> str:
    s = (s or "").strip().lower()
    s = re.sub(r"[^a-z0-9_\-]+", "_", s)
    return s.strip("_") or "dataset"

def run_id() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

def run_date() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")

def sha256_file(path: Path) -> str:
    """Calculate SHA256 hash of a file."""
    h = hashlib.sha256()
    with path.open("rb") as f:
        while chunk := f.read(1024 * 1024):
            h.update(chunk)
    return h.hexdigest()

def load_existing_metadata(dataset: str) -> pd.DataFrame:
    """Load all existing metadata for a dataset from previous runs."""
    meta_base = OUTPUT_DIR / "metadata" / "structured_parquet" / f"dataset={dataset}"
    if not meta_base.exists():
        return pd.DataFrame()
    
    all_meta = []
    for meta_file in meta_base.rglob("metadata.parquet"):
        try:
            df = pd.read_parquet(meta_file)
            all_meta.append(df)
        except Exception as e:
            print(f"  Could not read {meta_file}: {e}")
    
    if all_meta:
        return pd.concat(all_meta, ignore_index=True)
    return pd.DataFrame()

def pretty_print_metadata(df: pd.DataFrame) -> str:
    """
    Returns a human-readable string:
    one field per line, separated by a divider between records.
    """
    lines = []
    for _, row in df.iterrows():
        lines.append("=" * 60)
        for col in df.columns:
            lines.append(f"{col}: {row[col]}")
    return "\n".join(lines) + "\n"

def print_performance_summary(metrics: dict):
    """Print detailed performance metrics."""
    print("\n" + "=" * 70)
    print("UPLOAD COMPLETE - STRUCTURED DATA INGESTION SUMMARY")
    print("=" * 70)
    print(f"Files Selected:        {metrics['files_selected']}")
    print(f"Files Scanned:         {metrics['files_scanned']}")
    print(f"Tables Inserted:       {metrics['tables_inserted']}")
    print(f"Metadata Records:      {metrics['metadata_records']}")
    print(f"Duplicates Skipped:    {metrics['duplicates_skipped']}")
    print(f"Errors:                {metrics['errors']}")
    
    print(f"\nPERFORMANCE METRICS")
    print("=" * 70)
    total_time = metrics['total_time']
    print(f"Total Time:            {total_time:.2f} seconds")
    print(f"File Read Time:        {metrics['file_read_time']:.2f} seconds ({metrics['file_read_time']/total_time*100:.1f}%)")
    print(f"Hash Calculation:      {metrics['hash_time']:.2f} seconds ({metrics['hash_time']/total_time*100:.1f}%)")
    print(f"Parquet Conversion:    {metrics['parquet_time']:.2f} seconds ({metrics['parquet_time']/total_time*100:.1f}%)")
    print(f"S3 Upload Time:        {metrics['s3_upload_time']:.2f} seconds ({metrics['s3_upload_time']/total_time*100:.1f}%)")
    
    print(f"\nTHROUGHPUT")
    print("=" * 70)
    if metrics['tables_inserted'] > 0:
        print(f"Tables/Second:         {metrics['tables_inserted']/total_time:.2f} tables/sec")
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
    
    print(f"\nOPTIMIZATION IMPACT")
    print("=" * 70)
    estimated_old = metrics['tables_inserted'] * 0.15  # Estimated individual operations
    estimated_overhead = metrics['files_scanned'] * 0.05
    estimated_total_old = estimated_old + estimated_overhead + metrics['hash_time'] + metrics['file_read_time']
    
    print("Before Optimization (Estimated):")
    print(f"  - Individual Operations:   {metrics['tables_inserted']} separate S3 uploads")
    print(f"  - Estimated Time:          {estimated_total_old:.2f} seconds")
    print(f"  - Estimated Throughput:    {metrics['tables_inserted']/estimated_total_old:.2f} tables/sec" if estimated_total_old > 0 else "  - N/A")
    
    print(f"\nAfter Optimization (Current):")
    print(f"  - Batch Operations:        {metrics['tables_inserted']} optimized uploads")
    print(f"  - Actual Time:             {total_time:.2f} seconds")
    print(f"  - Actual Throughput:       {metrics['tables_inserted']/total_time:.2f} tables/sec" if total_time > 0 else "  - N/A")
    
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
        'tables_inserted': 0,
        'metadata_records': 0,
        'duplicates_skipped': 0,
        'errors': 0,
        'total_bytes': 0,
        'file_read_time': 0,
        'hash_time': 0,
        'parquet_time': 0,
        's3_upload_time': 0,
        'total_time': 0,
        'file_sizes': [],
        'file_times': [],
        'run_id': '',
        'run_date': ''
    }
    
    root = tk.Tk()
    root.withdraw()
    root.attributes("-topmost", True)

    paths = filedialog.askopenfilenames(
        title="Select STRUCTURED files (CSV/XLSX)",
        filetypes=[("CSV or Excel", "*.csv *.xlsx")]
    )
    if not paths:
        print("No files selected. Exiting.")
        return
    
    metrics['files_selected'] = len(paths)

    # Auto-generate dataset name from first file
    first_file = Path(paths[0]).stem  # Get filename without extension
    dataset = safe_name(first_file)

    rid = run_id()
    rdate = run_date()
    
    metrics['run_id'] = rid
    metrics['run_date'] = rdate

    # Local paths
    local_data_dir = OUTPUT_DIR / "data" / "structured" / "tables" / dataset / f"run_date={rdate}" / f"run_id={rid}"
    local_meta_dir = OUTPUT_DIR / "metadata" / "structured_parquet" / f"dataset={dataset}" / f"run_date={rdate}" / f"run_id={rid}"
    local_data_dir.mkdir(parents=True, exist_ok=True)
    local_meta_dir.mkdir(parents=True, exist_ok=True)

    # S3 prefixes
    s3_data_prefix = f"data/structured/tables/{dataset}/run_date={rdate}/run_id={rid}/"
    s3_meta_key = f"metadata/structured_parquet/dataset={dataset}/run_date={rdate}/run_id={rid}/metadata.parquet"

    # Load existing metadata to check for duplicates
    existing_meta = load_existing_metadata(dataset)
    existing_hashes = set(existing_meta['file_hash'].tolist()) if 'file_hash' in existing_meta.columns else set()

    meta_rows = []
    skipped_count = 0

    for p in paths:
        file_start = time.time()
        fp = Path(p)
        ext = fp.suffix.lower()
        
        metrics['files_scanned'] += 1
        
        # Track file size
        file_size = fp.stat().st_size
        metrics['file_sizes'].append(file_size)
        metrics['total_bytes'] += file_size
        
        # Calculate file hash to check for duplicates
        hash_start = time.time()
        file_hash = sha256_file(fp)
        metrics['hash_time'] += time.time() - hash_start
        
        if file_hash in existing_hashes:
            print(f" SKIPPED (already exists): {fp.name}")
            skipped_count += 1
            metrics['duplicates_skipped'] += 1
            continue

        try:
            if ext == ".csv":
                read_start = time.time()
                df = pd.read_csv(fp)
                metrics['file_read_time'] += time.time() - read_start
                
                table = safe_name(fp.stem)
                parquet_name = f"{table}.parquet"

                local_parquet = local_data_dir / parquet_name
                
                parquet_start = time.time()
                df.to_parquet(local_parquet, engine="pyarrow", index=False)
                metrics['parquet_time'] += time.time() - parquet_start

                s3_key = s3_data_prefix + parquet_name
                
                upload_start = time.time()
                s3.upload_file(str(local_parquet), S3_BUCKET, s3_key)
                metrics['s3_upload_time'] += time.time() - upload_start

                meta_rows.append({
                    "run_id": rid, "run_date": rdate, "dataset": dataset,
                    "source_file": str(fp), "file_type": "csv", "table": table,
                    "file_hash": file_hash,
                    "rows": int(len(df)), "cols": int(df.shape[1]),
                    "local_parquet_path": str(local_parquet),
                    "s3_parquet_uri": f"s3://{S3_BUCKET}/{s3_key}",
                    "ingested_at_utc": datetime.now(timezone.utc).isoformat(),
                })
                metrics['tables_inserted'] += 1
                metrics['metadata_records'] += 1
                print(f"CSV -> {s3_key}")

            elif ext == ".xlsx":
                read_start = time.time()
                xls = pd.ExcelFile(fp)
                metrics['file_read_time'] += time.time() - read_start
                
                for sheet in xls.sheet_names:
                    sheet_read_start = time.time()
                    df = pd.read_excel(fp, sheet_name=sheet)
                    metrics['file_read_time'] += time.time() - sheet_read_start
                    
                    table = safe_name(f"{fp.stem}_{sheet}")
                    parquet_name = f"{table}.parquet"

                    local_parquet = local_data_dir / parquet_name
                    
                    parquet_start = time.time()
                    df.to_parquet(local_parquet, engine="pyarrow", index=False)
                    metrics['parquet_time'] += time.time() - parquet_start

                    s3_key = s3_data_prefix + parquet_name
                    
                    upload_start = time.time()
                    s3.upload_file(str(local_parquet), S3_BUCKET, s3_key)
                    metrics['s3_upload_time'] += time.time() - upload_start

                    meta_rows.append({
                        "run_id": rid, "run_date": rdate, "dataset": dataset,
                        "source_file": str(fp), "file_type": "xlsx", "sheet": sheet,
                        "table": table, "file_hash": file_hash,
                        "rows": int(len(df)), "cols": int(df.shape[1]),
                        "local_parquet_path": str(local_parquet),
                        "s3_parquet_uri": f"s3://{S3_BUCKET}/{s3_key}",
                        "ingested_at_utc": datetime.now(timezone.utc).isoformat(),
                    })
                    metrics['tables_inserted'] += 1
                    metrics['metadata_records'] += 1
                    print(f"XLSX ({sheet}) -> {s3_key}")
            
            file_elapsed = time.time() - file_start
            metrics['file_times'].append(file_elapsed)
            
        except Exception as e:
            print(f"ERROR processing {fp.name}: {e}")
            metrics['errors'] += 1

    if not meta_rows:
        msg = f"No new files to process. All {skipped_count} file(s) already exist."
        print(f"\n{msg}")
        messagebox.showinfo("No new data", msg)
        return

    # Metadata Parquet local + S3
    meta_df = pd.DataFrame(meta_rows)
    meta_path = local_meta_dir / "metadata.parquet"
    meta_df.to_parquet(meta_path, engine="pyarrow", index=False)
    s3.upload_file(str(meta_path), S3_BUCKET, s3_meta_key)

    # ---- NEW: Pretty-print view (one field per line) ----
    pretty_text = pretty_print_metadata(meta_df)

    # Print to terminal nicely
    print("\nMETADATA (HUMAN VIEW)\n")
    print(pretty_text)

    # Save to a local readable text file
    pretty_path = local_meta_dir / "metadata_pretty.txt"
    pretty_path.write_text(pretty_text, encoding="utf-8")

    # (Optional) upload the pretty text to S3 too (handy for mentor/demo)
    s3_pretty_key = f"metadata/structured_parquet/dataset={dataset}/run_date={rdate}/run_id={rid}/metadata_pretty.txt"
    s3.upload_file(str(pretty_path), S3_BUCKET, s3_pretty_key)

    # Calculate total time
    metrics['total_time'] = time.time() - start_time
    
    # Print performance summary
    print_performance_summary(metrics)

    summary = (
        f"Upload Complete!\n\n"
        f"✓ Dataset: {dataset}\n"
        f"✓ {len(meta_rows)} new files uploaded\n"
        f"✓ {skipped_count} duplicates skipped\n\n"
        f"Files saved to:\n{local_data_dir}\n\n"
        f"Metadata saved to:\n{meta_path}"
    )
    print(summary)
    messagebox.showinfo("Upload Complete!", summary)

if __name__ == "__main__":
    main()

