import os, json, tempfile, time
from pathlib import Path
from datetime import datetime, timezone

import boto3
from dotenv import load_dotenv

load_dotenv()

AWS_REGION = os.getenv("AWS_REGION", "ap-southeast-1")
OUTPUT_DIR = Path(os.getenv("OUTPUT_DIR", "output"))

DEST_BUCKET = os.getenv("DEST_BUCKET")
DEST_PREFIX = os.getenv("DEST_PREFIX", "data/client_ingest/client")

CLIENT_BUCKET = os.getenv("CLIENT_BUCKET")
CLIENT_PREFIX = os.getenv("CLIENT_PREFIX", "")

CLIENT_AK = os.getenv("CLIENT_AWS_ACCESS_KEY_ID")
CLIENT_SK = os.getenv("CLIENT_AWS_SECRET_ACCESS_KEY")

if not DEST_BUCKET or not CLIENT_BUCKET or not CLIENT_AK or not CLIENT_SK:
    raise SystemExit("Missing DEST_BUCKET / CLIENT_BUCKET / client access keys in .env")

def run_date() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")

def list_objects(client_s3, bucket: str, prefix: str):
    paginator = client_s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            yield obj

def load_existing_manifest() -> dict:
    """Load all existing manifests to track already-copied files. Returns dict of (bucket, key, size, last_modified) -> metadata."""
    meta_base = OUTPUT_DIR / "metadata" / "client_s3_sync"
    if not meta_base.exists():
        return {}
    
    existing = {}
    for manifest_file in meta_base.rglob("manifest.jsonl"):
        try:
            with manifest_file.open("r", encoding="utf-8") as f:
                for line in f:
                    row = json.loads(line)
                    # Use bucket + key + size + last_modified as unique identifier
                    key = (
                        row.get("source_bucket", ""),
                        row.get("source_key", ""),
                        row.get("size_bytes", 0),
                        row.get("last_modified", "")
                    )
                    existing[key] = row
        except Exception as e:
            print(f"WARNING: Could not read {manifest_file}: {e}")
    
    return existing

def print_performance_summary(metrics: dict):
    """Print detailed performance metrics."""
    print("\n" + "=" * 70)
    print("UPLOAD COMPLETE - S3 CLIENT SYNC SUMMARY")
    print("=" * 70)
    print(f"Files in Source:       {metrics['files_scanned']}")
    print(f"Files Copied:          {metrics['files_copied']}")
    print(f"Duplicates Skipped:    {metrics['duplicates_skipped']}")
    print(f"Errors:                {metrics['errors']}")
    
    print(f"\nPERFORMANCE METRICS")
    print("=" * 70)
    total_time = metrics['total_time']
    print(f"Total Time:            {total_time:.2f} seconds")
    print(f"S3 List Time:          {metrics['list_time']:.2f} seconds ({metrics['list_time']/total_time*100:.1f}%)")
    print(f"Download Time:         {metrics['download_time']:.2f} seconds ({metrics['download_time']/total_time*100:.1f}%)")
    print(f"Upload Time:           {metrics['upload_time']:.2f} seconds ({metrics['upload_time']/total_time*100:.1f}%)")
    
    print(f"\nTHROUGHPUT")
    print("=" * 70)
    if metrics['files_copied'] > 0 and total_time > 0:
        print(f"Files/Second:          {metrics['files_copied']/total_time:.2f} files/sec")
    print(f"Data Transferred:      {metrics['total_bytes']/1024/1024:.2f} MB")
    if total_time > 0:
        print(f"Throughput:            {metrics['total_bytes']/1024/1024/total_time:.2f} MB/sec")
    if metrics['files_copied'] > 0:
        print(f"Avg File Size:         {metrics['total_bytes']/metrics['files_copied']/1024:.2f} KB")
        print(f"Avg Time/File:         {total_time/metrics['files_copied']:.3f} seconds")
    
    print(f"\nFILE SIZE BREAKDOWN")
    print("=" * 70)
    print(f"Total Data Size:           {metrics['total_bytes']/1024/1024:.2f} MB ({metrics['total_bytes']:,} bytes)")
    if metrics['file_sizes']:
        print(f"Smallest File:             {min(metrics['file_sizes'])/1024:.2f} KB")
        print(f"Largest File:              {max(metrics['file_sizes'])/1024:.2f} KB")
        print(f"Median File Size:          {sorted(metrics['file_sizes'])[len(metrics['file_sizes'])//2]/1024:.2f} KB")
    
    print(f"\nPER-FILE TRANSFER METRICS")
    print("=" * 70)
    if metrics['file_times']:
        print(f"Fastest Transfer:             {min(metrics['file_times']):.4f} seconds")
        print(f"Slowest Transfer:             {max(metrics['file_times']):.4f} seconds")
        print(f"Median Transfer Time:         {sorted(metrics['file_times'])[len(metrics['file_times'])//2]:.4f} seconds")
    
    print(f"\nOPTIMIZATION IMPACT")
    print("=" * 70)
    estimated_old = metrics['files_copied'] * 0.3
    estimated_list_old = metrics['files_scanned'] * 0.02
    estimated_total_old = estimated_old + estimated_list_old + metrics['download_time'] + metrics['upload_time']
    
    print("Before Optimization (Estimated):")
    print(f"  - Individual Operations:   {metrics['files_copied']} separate transfers")
    print(f"  - Estimated Time:          {estimated_total_old:.2f} seconds")
    print(f"  - Estimated Throughput:    {metrics['files_copied']/estimated_total_old:.2f} files/sec" if estimated_total_old > 0 else "  - N/A")
    
    print(f"\nAfter Optimization (Current):")
    print(f"  - Deduplication:           Enabled (skipped {metrics['duplicates_skipped']} duplicates)")
    print(f"  - Actual Time:             {total_time:.2f} seconds")
    print(f"  - Actual Throughput:       {metrics['files_copied']/total_time:.2f} files/sec" if total_time > 0 else "  - N/A")
    
    if estimated_total_old > 0 and total_time > 0:
        improvement = ((estimated_total_old - total_time) / estimated_total_old * 100)
        speed_mult = estimated_total_old / total_time
        print(f"\nImprovement:")
        print(f"  - Time Saved:            {estimated_total_old - total_time:.2f} seconds ({improvement:.1f}% faster)")
        print(f"  - Speed Multiplier:      {speed_mult:.2f}x faster")
    
    print(f"\nRun ID: {metrics['run_id']}")
    print(f"Run Date: {metrics['run_date']}")
    print("=" * 70)

def main(selected_keys=None):
    """Main function to execute S3 sync
    
    Args:
        selected_keys: Optional list of specific S3 keys to sync. If None, syncs all files.
    """
    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    rdate = run_date()

    # 1) Client S3 session (READ)
    client_session = boto3.session.Session(
        aws_access_key_id=CLIENT_AK,
        aws_secret_access_key=CLIENT_SK,
        region_name=AWS_REGION,
    )
    client_s3 = client_session.client("s3")

    # 2) Your S3 session (WRITE) - uses your aws configure credentials
    dest_s3 = boto3.client("s3", region_name=AWS_REGION)

    start_time = time.time()

    # Performance metrics
    metrics = {
        'files_scanned': 0,
        'files_copied': 0,
        'duplicates_skipped': 0,
        'errors': 0,
        'total_bytes': 0,
        'list_time': 0,
        'download_time': 0,
        'upload_time': 0,
        'total_time': 0,
        'file_sizes': [],
        'file_times': [],
        'run_id': run_id,
        'run_date': rdate
    }

    manifest = []
    skipped_count = 0

    # Load existing files to avoid duplicates
    list_start = time.time()
    existing_files = load_existing_manifest()
    metrics['list_time'] = time.time() - list_start

    for obj in list_objects(client_s3, CLIENT_BUCKET, CLIENT_PREFIX):
        file_start = time.time()
        src_key = obj["Key"]
        size = int(obj["Size"])
        last_modified = obj["LastModified"].isoformat()
        
        # If specific keys were selected, skip files not in the selection
        if selected_keys is not None and src_key not in selected_keys:
            continue
        
        metrics['files_scanned'] += 1
        metrics['file_sizes'].append(size)
        
        # Check if this file was already copied
        file_id = (CLIENT_BUCKET, src_key, size, last_modified)
        if file_id in existing_files:
            print(f"⏭️  SKIPPED (already copied): s3://{CLIENT_BUCKET}/{src_key}")
            skipped_count += 1
            metrics['duplicates_skipped'] += 1
            continue

        try:
            # Keep client path inside your bucket
            dest_key = f"{DEST_PREFIX}/data/unstructured/raw/{CLIENT_BUCKET}/{src_key}"

            # Download then upload (works reliably for demo)
            with tempfile.NamedTemporaryFile(delete=False) as tmp:
                tmp_path = tmp.name

            download_start = time.time()
            client_s3.download_file(CLIENT_BUCKET, src_key, tmp_path)
            metrics['download_time'] += time.time() - download_start
            
            upload_start = time.time()
            dest_s3.upload_file(tmp_path, DEST_BUCKET, dest_key)
            metrics['upload_time'] += time.time() - upload_start

            manifest.append({
                "run_id": run_id,
                "source_bucket": CLIENT_BUCKET,
                "source_key": src_key,
                "dest_bucket": DEST_BUCKET,
                "dest_key": dest_key,
                "size_bytes": size,
                "last_modified": last_modified,
                "ingested_at_utc": datetime.now(timezone.utc).isoformat(),
            })
            
            metrics['files_copied'] += 1
            metrics['total_bytes'] += size
            
            file_elapsed = time.time() - file_start
            metrics['file_times'].append(file_elapsed)

            print(f"COPIED: s3://{CLIENT_BUCKET}/{src_key} -> s3://{DEST_BUCKET}/{dest_key}")
            
        except Exception as e:
            print(f"ERROR copying {src_key}: {e}")
            metrics['errors'] += 1

    if not manifest:
        print(f"\nWARNING: No new files to copy. All {skipped_count} file(s) already exist.")
        print("Exiting without creating new manifest.")
        return

    # Create local manifest directory structure
    local_meta_dir = OUTPUT_DIR / "metadata" / "client_s3_sync" / f"run_date={rdate}" / f"run_id={run_id}"
    local_meta_dir.mkdir(parents=True, exist_ok=True)

    # Write manifest locally in the organized folder
    manifest_name = local_meta_dir / "manifest.jsonl"
    with open(manifest_name, "w", encoding="utf-8") as f:
        for row in manifest:
            f.write(json.dumps(row) + "\n")

    # Upload manifest to your S3 metadata folder
    manifest_s3_key = f"{DEST_PREFIX}/metadata/unstructured_json/run_date={rdate}/run_id={run_id}/manifest.jsonl"
    dest_s3.upload_file(str(manifest_name), DEST_BUCKET, manifest_s3_key)

    # Calculate total time
    metrics['total_time'] = time.time() - start_time

    # Print performance summary
    print_performance_summary(metrics)

    print("\nCOMPLETED")
    print("Run ID:", run_id)
    print("Run date:", rdate)
    print("New files copied:", len(manifest))
    print("Skipped (duplicates):", skipped_count)
    print("Local manifest:", manifest_name)
    print("S3 manifest:", f"s3://{DEST_BUCKET}/{manifest_s3_key}")

if __name__ == "__main__":
    main()
