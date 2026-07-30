"""
s3_batch_extractor.py
======================
SYINIQ – Part 2 Batch Path: AWS S3 CDC Extractor

Responsibility
--------------
Connect to AWS S3, list every object whose LastModified timestamp is newer
than the *last watermark*, read the file content, and publish rows in batches
to a Kafka topic named ``airflow.s3.<prefix_slug>``.

Mirrors mongodb_batch_extractor.py exactly – same WatermarkStore, same Kafka
envelope format, same chunking/retry logic – so the Spark consumer and the
Airflow DAG follow the identical pattern.

Watermark strategy
------------------
We use the S3 object ``LastModified`` (UTC timestamp) as the watermark.
Querying ``LastModified > last_run_utc`` gives us all objects written after
the last successful run.

Watermarks are persisted in ``watermarks/s3_watermarks.json``.

Supported file types
--------------------
  ┌─────────────┬──────────────────────────────────────────────────────┐
  │ Extension   │ How it is turned into Kafka records                  │
  ├─────────────┼──────────────────────────────────────────────────────┤
  │ .csv        │ Each row → one record dict                           │
  │ .json       │ Root array or single object → record(s)              │
  │ .jsonl      │ Each newline-delimited object → one record           │
  │ .parquet    │ Each row via pyarrow → one record dict               │
  │ other       │ Single file-level record (metadata + base64 content) │
  └─────────────┴──────────────────────────────────────────────────────┘

Kafka message format
--------------------
Every Kafka message is a JSON object mirroring the MongoDB envelope:

    {
        "source":         "s3",
        "bucket":         "my-bucket",
        "prefix":         "sales/",
        "file_key":       "sales/2026/03/11/data.csv",
        "file_type":      "csv",
        "batch_id":       "<uuid>",
        "total_batches":  3,
        "batch_number":   1,
        "extracted_at":   "2026-03-11T08:00:00Z",
        "watermark_from": "2026-03-11T06:00:00Z",
        "watermark_to":   "2026-03-11T08:00:00Z",
        "record_count":   500,
        "records":        [ {...}, {...}, ... ]
    }

Usage (standalone / testing)
-----------------------------
    python s3_batch_extractor.py --prefix sales/ --dry-run
    python s3_batch_extractor.py --prefix sales/ inventory/

Usage (from Airflow task)
-------------------------
    from s3_batch_extractor import S3BatchExtractor, S3ExtractorConfig
    cfg = S3ExtractorConfig.from_env()
    extractor = S3BatchExtractor(cfg)
    stats = extractor.extract_prefix("sales/")
"""

from __future__ import annotations

import argparse
import base64
import csv
import io
import json
import logging
import os
import re
import sys
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional, Tuple

# --------------------------------------------------------------------------
# Load .env from same directory
# --------------------------------------------------------------------------
_THIS_DIR = Path(__file__).resolve().parent
try:
    from dotenv import load_dotenv
    _env_path = _THIS_DIR / ".env"
    if _env_path.exists():
        load_dotenv(_env_path)
except ImportError:
    pass

# --------------------------------------------------------------------------
# Logging
# --------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(name)s  %(levelname)s  %(message)s",
)
logger = logging.getLogger("s3_batch_extractor")

# --------------------------------------------------------------------------
# Optional imports (graceful fallbacks)
# --------------------------------------------------------------------------
try:
    import boto3
    from botocore.exceptions import ClientError, NoCredentialsError
    _BOTO3_AVAILABLE = True
except ImportError:
    _BOTO3_AVAILABLE = False
    logger.warning("boto3 not installed – S3 functionality unavailable")

try:
    import pyarrow.parquet as pq
    _PARQUET_AVAILABLE = True
except ImportError:
    _PARQUET_AVAILABLE = False

try:
    from kafka import KafkaProducer
    from kafka.admin import KafkaAdminClient, NewTopic
    from kafka.errors import NoBrokersAvailable, KafkaError, TopicAlreadyExistsError
    _KAFKA_AVAILABLE = True
except ImportError:
    _KAFKA_AVAILABLE = False


# ============================================================================
# Config dataclass
# ============================================================================

@dataclass
class S3ExtractorConfig:
    """Configuration for S3BatchExtractor – same pattern as MongoDBExtractorConfig."""
    
    aws_access_key: str = ""
    aws_secret_key: str = ""
    aws_region: str = "ap-southeast-1"
    s3_bucket: str = ""
    prefixes: List[str] = field(default_factory=list)
    batch_size: int = 500
    bootstrap_servers: str = "localhost:9092"
    topic_prefix: str = "airflow.s3"
    watermark_file: str = ""
    initial_lookback_hours: int = 87600  # 10 years

    @classmethod
    def from_env(cls) -> "S3ExtractorConfig":
        """Build config from environment variables."""
        raw_prefixes = os.getenv("S3_PREFIXES", "metadata/,data/")
        prefixes = [p.strip() for p in raw_prefixes.split(",") if p.strip()]
        
        watermark_file = os.getenv("S3_WATERMARK_FILE", "")
        if not watermark_file:
            watermark_file = str(_THIS_DIR / "watermarks" / "s3_watermarks.json")
        
        return cls(
            aws_access_key=os.getenv("AWS_ACCESS_KEY_ID", ""),
            aws_secret_key=os.getenv("AWS_SECRET_ACCESS_KEY", ""),
            aws_region=os.getenv("AWS_DEFAULT_REGION", "ap-southeast-1"),
            s3_bucket=os.getenv("S3_BUCKET", ""),
            prefixes=prefixes,
            batch_size=int(os.getenv("S3_BATCH_SIZE", "500")),
            bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
            topic_prefix=os.getenv("KAFKA_TOPIC_AIRFLOW_S3_PREFIX", "airflow.s3"),
            watermark_file=watermark_file,
            initial_lookback_hours=int(os.getenv("S3_INITIAL_LOOKBACK_HOURS", "87600")),
        )


# ============================================================================
# Watermark store  (identical pattern to MongoDB extractor)
# ============================================================================

class WatermarkStore:
    """
    Persists per-prefix watermarks to a JSON file.

    Format on disk:
        {
            "sales/":     "2026-03-11T06:00:00+00:00",
            "inventory/": "2026-03-11T05:00:00+00:00"
        }
    """

    def __init__(self, file_path: str) -> None:
        self.file_path = Path(file_path)
        self.file_path.parent.mkdir(parents=True, exist_ok=True)
        self.watermarks: Dict[str, str] = {}
        self._load()

    def _load(self) -> None:
        if self.file_path.exists():
            try:
                with open(self.file_path, "r", encoding="utf-8") as f:
                    self.watermarks = json.load(f)
            except Exception as exc:
                logger.warning(f"Could not load watermarks: {exc}")

    def _save(self) -> None:
        with open(self.file_path, "w", encoding="utf-8") as f:
            json.dump(self.watermarks, f, indent=2)

    def get(self, prefix: str, default_hours_back: int = 24) -> datetime:
        val = self.watermarks.get(prefix)
        if val:
            return datetime.fromisoformat(val)
        # Default: look back N hours
        return datetime.now(timezone.utc) - timedelta(hours=default_hours_back)

    def set(self, prefix: str, ts: datetime) -> None:
        self.watermarks[prefix] = ts.isoformat()
        self._save()


# ============================================================================
# File parsers
# ============================================================================

def _prefix_to_topic_slug(prefix: str) -> str:
    """
    Convert an S3 prefix to a valid Kafka topic segment.
    "sales/"  → "sales"
    "data/2026/raw/" → "data_2026_raw"
    """
    slug = prefix.strip("/").replace("/", "_").replace(" ", "_")
    slug = re.sub(r"[^a-zA-Z0-9_.-]", "_", slug)
    return slug or "root"


def _parse_csv(content: bytes) -> List[Dict[str, Any]]:
    """Parse CSV bytes → list of row dicts."""
    text = content.decode("utf-8-sig", errors="replace")
    reader = csv.DictReader(io.StringIO(text))
    return [dict(row) for row in reader]


def _parse_json(content: bytes) -> List[Dict[str, Any]]:
    """Parse JSON bytes → list of records.
    Handles: single object, array of objects, newline-delimited JSON."""
    text = content.decode("utf-8", errors="replace").strip()
    if not text:
        return []
    # Try array first, then single object, then JSONL
    try:
        data = json.loads(text)
        if isinstance(data, list):
            return data
        elif isinstance(data, dict):
            return [data]
        return []
    except json.JSONDecodeError:
        # Try JSONL
        records = []
        for line in text.splitlines():
            line = line.strip()
            if line:
                try:
                    records.append(json.loads(line))
                except json.JSONDecodeError:
                    pass
        return records


def _parse_parquet(content: bytes) -> List[Dict[str, Any]]:
    """Parse Parquet bytes → list of row dicts (via pyarrow)."""
    if not _PARQUET_AVAILABLE:
        logger.warning("pyarrow not available – cannot parse parquet")
        return []
    table = pq.read_table(io.BytesIO(content))
    # Convert to list of dicts
    return table.to_pylist() if hasattr(table, "to_pylist") else [
        {col: table.column(col)[i].as_py() for col in table.column_names}
        for i in range(table.num_rows)
    ]


def _parse_file(key: str, content: bytes) -> List[Dict[str, Any]]:
    """Dispatch to the right parser based on file extension."""
    ext = Path(key).suffix.lower()
    try:
        if ext == ".csv":
            return _parse_csv(content)
        elif ext in (".json", ".jsonl"):
            return _parse_json(content)
        elif ext == ".parquet":
            return _parse_parquet(content)
    except Exception as exc:
        logger.error(f"Failed to parse {key}: {exc}")

    # Fallback: binary content as base64
    return [{
        "file_key":   key,
        "file_type":  ext.lstrip(".") or "binary",
        "content_b64": base64.b64encode(content).decode(),
        "size_bytes": len(content),
    }]


# ============================================================================
# Main extractor
# ============================================================================

class S3BatchExtractor:
    """
    AWS S3 → Kafka batch extractor.

    One instance per Airflow task run (or per manual invocation).
    Call :meth:`extract_prefix` for each S3 prefix you want to sync.
    """

    def __init__(self, config: S3ExtractorConfig) -> None:
        self.config = config
        self.s3_client = None
        self.kafka_producer = None
        self.watermark_store = WatermarkStore(config.watermark_file)

    # ------------------------------------------------------------------
    # Connection management
    # ------------------------------------------------------------------

    def connect(self) -> None:
        self._connect_s3()
        self._connect_kafka()

    def _connect_s3(self) -> None:
        if not _BOTO3_AVAILABLE:
            raise RuntimeError("boto3 not installed")
        logger.info("Connecting to S3 ...")
        self.s3_client = boto3.client(
            "s3",
            aws_access_key_id=self.config.aws_access_key,
            aws_secret_access_key=self.config.aws_secret_key,
            region_name=self.config.aws_region,
        )
        logger.info("S3 client ready")

    def _connect_kafka(self) -> None:
        if not _KAFKA_AVAILABLE:
            raise RuntimeError("kafka-python not installed")
        logger.info(f"Connecting to Kafka {self.config.bootstrap_servers} ...")
        self.kafka_producer = KafkaProducer(
            bootstrap_servers=self.config.bootstrap_servers,
            value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
            acks="all",
            retries=3,
            max_in_flight_requests_per_connection=1,
        )
        logger.info("Kafka producer ready")

    def close(self) -> None:
        if self.kafka_producer:
            self.kafka_producer.flush()
            self.kafka_producer.close()
        logger.info("Connections closed")

    # ------------------------------------------------------------------
    # Connection test  (called by Airflow task_check_connection)
    # ------------------------------------------------------------------

    def test_connection(self) -> Dict[str, Any]:
        """
        Verify S3 and Kafka are reachable.
        Returns dict with status for each service.
        """
        result: Dict[str, Any] = {"s3": {}, "kafka": {}}

        # Test S3
        try:
            if not _BOTO3_AVAILABLE:
                result["s3"] = {"status": "error", "error": "boto3 not installed"}
            else:
                self._connect_s3()
                # Try listing a single object to test credentials
                resp = self.s3_client.list_objects_v2(
                    Bucket=self.config.s3_bucket,
                    MaxKeys=1
                )
                result["s3"] = {
                    "status": "ok",
                    "bucket": self.config.s3_bucket,
                    "region": self.config.aws_region,
                }
        except NoCredentialsError:
            result["s3"] = {"status": "error", "error": "No AWS credentials"}
        except ClientError as exc:
            result["s3"] = {"status": "error", "error": str(exc)}
        except Exception as exc:
            result["s3"] = {"status": "error", "error": str(exc)}

        # Test Kafka
        try:
            if not _KAFKA_AVAILABLE:
                result["kafka"] = {"status": "error", "error": "kafka-python not installed"}
            else:
                admin = KafkaAdminClient(
                    bootstrap_servers=self.config.bootstrap_servers,
                    request_timeout_ms=5000,
                )
                topics = admin.list_topics()
                admin.close()
                result["kafka"] = {
                    "status": "ok",
                    "bootstrap_servers": self.config.bootstrap_servers,
                    "topics_count": len(topics),
                }
        except NoBrokersAvailable:
            result["kafka"] = {"status": "error", "error": "No Kafka brokers available"}
        except Exception as exc:
            result["kafka"] = {"status": "error", "error": str(exc)}

        return result

    # ------------------------------------------------------------------
    # Topic helpers
    # ------------------------------------------------------------------

    def _ensure_topic(self, topic: str) -> None:
        """Create Kafka topic if it doesn't exist."""
        try:
            admin = KafkaAdminClient(
                bootstrap_servers=self.config.bootstrap_servers,
                request_timeout_ms=5000,
            )
            existing = admin.list_topics()
            if topic not in existing:
                logger.info(f"Creating Kafka topic: {topic}")
                new_topic = NewTopic(
                    name=topic,
                    num_partitions=3,
                    replication_factor=1,
                )
                admin.create_topics([new_topic])
            admin.close()
        except TopicAlreadyExistsError:
            pass
        except Exception as exc:
            logger.warning(f"Could not ensure topic {topic}: {exc}")

    # ------------------------------------------------------------------
    # Main extraction
    # ------------------------------------------------------------------

    def extract_prefix(
        self,
        prefix: str,
        dry_run: bool = False,
    ) -> Dict[str, Any]:
        """
        Extract all new S3 objects from the given prefix and publish to Kafka.

        Returns stats dict:
            {
                "prefix": "sales/",
                "watermark_from": "2026-03-11T06:00:00+00:00",
                "watermark_to": "2026-03-11T08:00:00+00:00",
                "total_files": 5,
                "total_records": 2500,
                "total_batches": 5,
                "status": "success"
            }
        """
        logger.info(f"=== Extracting S3 prefix: {prefix} ===")
        start_time = time.time()

        # Read last watermark
        watermark_from = self.watermark_store.get(
            prefix,
            self.config.initial_lookback_hours
        )
        logger.info(f"Watermark from: {watermark_from}")

        # List new objects
        new_objects = self._list_new_objects(prefix, watermark_from)
        logger.info(f"Found {len(new_objects)} new objects")

        if not new_objects:
            return {
                "prefix": prefix,
                "watermark_from": watermark_from.isoformat(),
                "watermark_to": datetime.now(timezone.utc).isoformat(),
                "total_files": 0,
                "total_records": 0,
                "total_batches": 0,
                "status": "success",
                "message": "No new objects"
            }

        # Topic name
        topic_slug = _prefix_to_topic_slug(prefix)
        topic = f"{self.config.topic_prefix}.{topic_slug}"
        
        if not dry_run:
            self._ensure_topic(topic)

        # Process each object
        total_records = 0
        total_batches = 0
        batch_id = str(uuid.uuid4())
        watermark_to = datetime.now(timezone.utc)

        for obj_meta in new_objects:
            key = obj_meta["Key"]
            last_modified = obj_meta["LastModified"]
            
            # Read and parse file
            records = self._read_s3_object(key)
            if not records:
                continue

            # Update watermark_to to the latest object timestamp
            if last_modified > watermark_to:
                watermark_to = last_modified

            # Chunk records into batches
            batches = list(self._chunk_records(records, self.config.batch_size))
            
            for batch_num, batch_records in enumerate(batches, start=1):
                envelope = {
                    "source": "s3",
                    "bucket": self.config.s3_bucket,
                    "prefix": prefix,
                    "file_key": key,
                    "file_type": Path(key).suffix.lstrip(".") or "binary",
                    "batch_id": batch_id,
                    "batch_number": batch_num,
                    "total_batches": len(batches),
                    "extracted_at": datetime.now(timezone.utc).isoformat(),
                    "watermark_from": watermark_from.isoformat(),
                    "watermark_to": watermark_to.isoformat(),
                    "record_count": len(batch_records),
                    "records": batch_records,
                }

                if dry_run:
                    logger.info(
                        f"[DRY-RUN] Would publish batch {batch_num}/{len(batches)} "
                        f"({len(batch_records)} records) from {key} to {topic}"
                    )
                else:
                    self.kafka_producer.send(topic, value=envelope)
                    logger.info(
                        f"Published batch {batch_num}/{len(batches)} from {key} "
                        f"({len(batch_records)} records)"
                    )

                total_records += len(batch_records)
                total_batches += 1

        # Update watermark on success
        if not dry_run:
            self.watermark_store.set(prefix, watermark_to)
            logger.info(f"Watermark updated to: {watermark_to}")

        elapsed = time.time() - start_time
        logger.info(
            f"Extraction complete: {len(new_objects)} files, "
            f"{total_records} records, {total_batches} batches in {elapsed:.2f}s"
        )

        return {
            "prefix": prefix,
            "watermark_from": watermark_from.isoformat(),
            "watermark_to": watermark_to.isoformat(),
            "total_files": len(new_objects),
            "total_records": total_records,
            "total_batches": total_batches,
            "status": "success",
            "elapsed_seconds": round(elapsed, 2),
        }

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _list_new_objects(
        self, prefix: str, since: datetime
    ) -> List[Dict[str, Any]]:
        """List S3 objects with LastModified > since."""
        objects = []
        paginator = self.s3_client.get_paginator("list_objects_v2")
        
        for page in paginator.paginate(Bucket=self.config.s3_bucket, Prefix=prefix):
            if "Contents" not in page:
                continue
            for obj in page["Contents"]:
                # S3 returns LastModified as timezone-aware datetime
                if obj["LastModified"] > since:
                    objects.append(obj)
        
        return objects

    def _read_s3_object(self, key: str) -> List[Dict[str, Any]]:
        """Download and parse an S3 object into records."""
        try:
            logger.info(f"Reading S3 object: {key}")
            response = self.s3_client.get_object(
                Bucket=self.config.s3_bucket,
                Key=key
            )
            content = response["Body"].read()
            
            # Parse based on file extension
            records = _parse_file(key, content)
            logger.info(f"Parsed {len(records)} records from {key}")
            return records
            
        except Exception as exc:
            logger.error(f"Failed to read {key}: {exc}")
            return []

    @staticmethod
    def _chunk_records(records: List[Dict[str, Any]], chunk_size: int) -> Iterator[List[Dict[str, Any]]]:
        """Split records into chunks of chunk_size."""
        for i in range(0, len(records), chunk_size):
            yield records[i:i + chunk_size]

    @staticmethod
    def _estimate_message_size(records: List[Dict[str, Any]]) -> int:
        """Rough estimate of serialized JSON size in bytes."""
        return len(json.dumps(records, default=str).encode("utf-8"))


# ============================================================================
# CLI entry point
# ============================================================================

def _main() -> None:
    parser = argparse.ArgumentParser(
        description="SYINIQ S3 Batch Extractor – list new S3 objects and publish to Kafka"
    )
    parser.add_argument(
        "--prefix",
        nargs="+",
        help="S3 prefix(es) to extract (overrides S3_PREFIXES env var)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print what would be published without actually sending to Kafka",
    )
    parser.add_argument(
        "--reset-watermarks",
        action="store_true",
        help="Delete saved watermarks so the next run re-extracts ALL data",
    )
    args = parser.parse_args()

    # Reset watermarks if requested
    if args.reset_watermarks:
        config = S3ExtractorConfig.from_env()
        watermark_file = Path(config.watermark_file)
        if watermark_file.exists():
            watermark_file.unlink()
            logger.info(f"Deleted watermarks file: {watermark_file}")
        else:
            logger.info("No watermarks file found")
        return

    # Build config
    config = S3ExtractorConfig.from_env()
    
    # Override prefixes if provided via CLI
    if args.prefix:
        config.prefixes = args.prefix

    if not config.s3_bucket:
        logger.error("S3_BUCKET not configured")
        sys.exit(1)

    if not config.prefixes:
        logger.error("No S3 prefixes configured")
        sys.exit(1)

    # Create extractor and connect
    extractor = S3BatchExtractor(config)
    try:
        extractor.connect()
        
        # Extract each prefix
        for prefix in config.prefixes:
            stats = extractor.extract_prefix(prefix, dry_run=args.dry_run)
            print(f"\n{prefix}: {json.dumps(stats, indent=2, default=str)}")
    
    finally:
        extractor.close()


if __name__ == "__main__":
    _main()
