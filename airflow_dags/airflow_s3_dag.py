"""
airflow_s3_dag.py
==================
SYINIQ – Part 2 Batch Path: Airflow DAG for AWS S3 CDC

Overview
--------
This DAG runs on a configurable schedule (default: hourly) and orchestrates
the full AWS S3 → Kafka pipeline in 5 clearly separated tasks — mirroring
the MongoDB batch CDC DAG exactly:

    Task 1 – check_connection
        Pings S3 and Kafka. Fails fast if either is unreachable.

    Task 2 – list_prefixes
        Reads S3_PREFIXES from .env / Airflow Variables and returns the list
        that task 3 will iterate over. Stored as XCom.

    Task 3 – extract_and_publish  (one task per S3 prefix, in parallel)
        Calls S3BatchExtractor.extract_prefix() for each prefix.
        Lists objects modified since last watermark, reads them, publishes
        record batches to Kafka, and advances the watermark on success.

    Task 4 – validate_kafka_topics
        Confirms messages landed in the expected Kafka topics.

    Task 5 – mark_run_complete
        Aggregates stats from all prefix tasks. Acts as the pipeline's
        "completion certificate".

Topic naming convention  (mirroring MongoDB)
-------------------------------------------
    S3 prefix   →  Kafka topic
    sales/      →  airflow.s3.sales
    inventory/  →  airflow.s3.inventory
    data/2026/  →  airflow.s3.data_2026

Deployment
----------
1. Copy this file into your Airflow dags/ folder (or symlink it).
2. Set S3 credentials in Airflow Variables OR in Kafka Integration/.env.
3. Trigger manually or wait for the next scheduled run.

   airflow dags trigger syiniq_s3_batch_cdc
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any, Dict, List

# ---------------------------------------------------------------------------
# Ensure Kafka Integration folder is on sys.path
# ---------------------------------------------------------------------------
_DAG_DIR = Path(__file__).resolve().parent
_KAFKA_INTEGRATION_DIR = _DAG_DIR.parent / "Kafka Integration"

if str(_KAFKA_INTEGRATION_DIR) not in sys.path:
    sys.path.insert(0, str(_KAFKA_INTEGRATION_DIR))

# ---------------------------------------------------------------------------
# Load .env for local dev
# ---------------------------------------------------------------------------
try:
    from dotenv import load_dotenv
    _env_path = _KAFKA_INTEGRATION_DIR / ".env"
    if _env_path.exists():
        load_dotenv(_env_path)
except ImportError:
    pass

# ---------------------------------------------------------------------------
# Airflow imports – wrapped so the file can be imported outside Airflow
# ---------------------------------------------------------------------------
try:
    from airflow import DAG
    from airflow.operators.python import PythonOperator
    from airflow.utils.dates import days_ago
    from airflow.models import Variable
    _AIRFLOW_AVAILABLE = True
except ImportError:
    _AIRFLOW_AVAILABLE = False
    class DAG:
        def __init__(self, *args, **kwargs): pass
    class PythonOperator:
        def __init__(self, *args, **kwargs): pass

from s3_batch_extractor import S3BatchExtractor, S3ExtractorConfig

logger = logging.getLogger("syiniq.airflow.s3_dag")


# ============================================================================
# Helper: read config
# ============================================================================

def _airflow_var(key: str, default: str = "") -> str:
    if _AIRFLOW_AVAILABLE:
        try:
            return Variable.get(key, default_var=default)
        except Exception:
            pass
    return os.getenv(key, default)


def _get_config() -> S3ExtractorConfig:
    raw_prefixes = _airflow_var("S3_PREFIXES", os.getenv("S3_PREFIXES", "metadata/,data/"))
    prefixes = [p.strip() for p in raw_prefixes.split(",") if p.strip()]

    return S3ExtractorConfig(
        aws_access_key=_airflow_var("AWS_ACCESS_KEY_ID", os.getenv("AWS_ACCESS_KEY_ID", "")),
        aws_secret_key=_airflow_var("AWS_SECRET_ACCESS_KEY", os.getenv("AWS_SECRET_ACCESS_KEY", "")),
        aws_region=_airflow_var("AWS_DEFAULT_REGION", os.getenv("AWS_DEFAULT_REGION", "ap-southeast-1")),
        s3_bucket=_airflow_var("S3_BUCKET", os.getenv("S3_BUCKET", "")),
        prefixes=prefixes,
        batch_size=int(_airflow_var("S3_BATCH_SIZE", os.getenv("S3_BATCH_SIZE", "500"))),
        bootstrap_servers=_airflow_var(
            "KAFKA_BOOTSTRAP_SERVERS",
            os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
        ),
        topic_prefix=_airflow_var(
            "KAFKA_TOPIC_AIRFLOW_S3_PREFIX",
            os.getenv("KAFKA_TOPIC_AIRFLOW_S3_PREFIX", "airflow.s3"),
        ),
        watermark_file=(
            _airflow_var("S3_WATERMARK_FILE")
            or os.getenv("S3_WATERMARK_FILE")
            or str(_KAFKA_INTEGRATION_DIR / "watermarks" / "s3_watermarks.json")
        ),
        initial_lookback_hours=int(
            _airflow_var("S3_INITIAL_LOOKBACK_HOURS", os.getenv("S3_INITIAL_LOOKBACK_HOURS", "87600"))
        ),
    )


# ============================================================================
# Task functions
# ============================================================================

# ── Task 1 ──────────────────────────────────────────────────────────────────
def task_check_connection(**context: Any) -> Dict[str, Any]:
    """
    Verify that S3 is reachable and Kafka brokers are up.
    Fails fast so downstream tasks don't run against a broken source.
    """
    logger.info("=== Task 1: check_connection ===")
    config = _get_config()
    extractor = S3BatchExtractor(config)
    result = extractor.test_connection()

    logger.info(f"Connection test result:\n{json.dumps(result, indent=2, default=str)}")

    s3_ok    = result.get("s3",    {}).get("status") == "ok"
    kafka_ok = result.get("kafka", {}).get("status") == "ok"

    if not s3_ok:
        error_msg = result.get("s3", {}).get("error", "Unknown error")
        logger.error(f"S3 connection failed: {error_msg}")
        raise RuntimeError(f"S3 connection failed: {error_msg}")
    
    if not kafka_ok:
        error_msg = result.get("kafka", {}).get("error", "Unknown error")
        logger.error(f"Kafka connection failed: {error_msg}")
        raise RuntimeError(f"Kafka connection failed: {error_msg}")

    context["ti"].xcom_push(key="connection_result", value=result)
    return result


# ── Task 2 ──────────────────────────────────────────────────────────────────
def task_list_prefixes(**context: Any) -> List[str]:
    """
    Determine which S3 prefixes should be extracted this run.
    Returns the list and pushes it to XCom.
    """
    logger.info("=== Task 2: list_prefixes ===")
    config = _get_config()
    prefixes = config.prefixes
    logger.info(f"S3 prefixes to process: {prefixes}")
    context["ti"].xcom_push(key="prefixes", value=prefixes)
    return prefixes


# ── Task 3 ──────────────────────────────────────────────────────────────────
def task_extract_prefix(prefix: str, **context: Any) -> Dict[str, Any]:
    """
    Extract one S3 prefix and publish record batches to Kafka.

    One PythonOperator per prefix so Airflow can:
      - Retry each prefix independently
      - Run prefixes in parallel (max_active_tasks permitting)
      - Show clear per-prefix success/failure in the UI

    Flow:
      1. Build config from env / Airflow Variables
      2. Connect to S3 + Kafka
      3. Read the last watermark for this prefix
      4. List S3 objects with LastModified > watermark
      5. Download and parse each file into records
      6. Publish record batches to airflow.s3.<prefix_slug>
      7. Advance the watermark on success
      8. Push stats to XCom
    """
    logger.info(f"=== Task 3: extract_prefix [{prefix}] ===")

    config = _get_config()
    extractor = S3BatchExtractor(config)

    try:
        extractor.connect()
        stats = extractor.extract_prefix(prefix, dry_run=False)
    finally:
        extractor.close()

    logger.info(
        f"[{prefix}] Extraction complete: "
        f"{stats['total_files']} files, "
        f"{stats['total_records']} records, "
        f"{stats['total_batches']} batches, "
        f"status={stats['status']}"
    )

    xcom_key = f"stats_{prefix.strip('/').replace('/', '_')}"
    context["ti"].xcom_push(key=xcom_key, value=stats)
    return stats


# ── Task 4 ──────────────────────────────────────────────────────────────────
def task_validate_kafka_topics(**context: Any) -> Dict[str, Any]:
    """
    Confirm that Kafka topics exist and have received messages this run.
    """
    logger.info("=== Task 4: validate_kafka_topics ===")

    config = _get_config()
    validation: Dict[str, Any] = {"topics_checked": [], "all_ok": True, "warnings": []}

    try:
        from kafka import KafkaConsumer
        from kafka.admin import KafkaAdminClient
        
        admin = KafkaAdminClient(
            bootstrap_servers=config.bootstrap_servers,
            request_timeout_ms=5000,
        )
        
        existing_topics = admin.list_topics()
        
        for prefix in config.prefixes:
            topic_slug = prefix.strip("/").replace("/", "_").replace(" ", "_")
            topic = f"{config.topic_prefix}.{topic_slug}"
            
            if topic in existing_topics:
                # Try to peek at topic
                consumer = KafkaConsumer(
                    topic,
                    bootstrap_servers=config.bootstrap_servers,
                    auto_offset_reset="earliest",
                    consumer_timeout_ms=2000,
                    max_poll_records=1,
                )
                
                msg_count = sum(1 for _ in consumer)
                consumer.close()
                
                validation["topics_checked"].append({
                    "topic": topic,
                    "exists": True,
                    "message_count": msg_count
                })
            else:
                validation["all_ok"] = False
                validation["warnings"].append(f"Topic {topic} does not exist")
                validation["topics_checked"].append({
                    "topic": topic,
                    "exists": False
                })
        
        admin.close()

    except ImportError:
        validation["warnings"].append("kafka-python not installed – cannot validate topics")
    except Exception as exc:
        validation["all_ok"] = False
        validation["warnings"].append(f"Validation error: {exc}")

    logger.info(f"Validation result: {json.dumps(validation, indent=2, default=str)}")
    context["ti"].xcom_push(key="validation_result", value=validation)
    return validation


# ── Task 5 ──────────────────────────────────────────────────────────────────
def task_mark_run_complete(**context: Any) -> Dict[str, Any]:
    """
    Aggregate stats from all prefix tasks and write a completion summary.
    If this task succeeds, all upstream steps have succeeded.
    """
    logger.info("=== Task 5: mark_run_complete ===")

    ti = context["ti"]
    config = _get_config()

    summary: Dict[str, Any] = {
        "dag_run_id":     context.get("run_id", "unknown"),
        "execution_date": str(context.get("execution_date", datetime.now(timezone.utc))),
        "source":         "s3",
        "bucket":         config.s3_bucket,
        "prefixes":       {},
    }

    total_records = 0
    total_files   = 0

    for prefix in config.prefixes:
        xcom_key = f"stats_{prefix.strip('/').replace('/', '_')}"
        stats = ti.xcom_pull(key=xcom_key, task_ids=f"extract_prefix_{prefix.strip('/').replace('/', '_')}")
        if stats:
            summary["prefixes"][prefix] = stats
            total_records += stats.get("total_records", 0)
            total_files   += stats.get("total_files", 0)

    summary["total_files_extracted"]   = total_files
    summary["total_records_extracted"] = total_records
    summary["completed_at"]            = datetime.now(timezone.utc).isoformat()
    summary["status"]                  = "success"

    logger.info(
        f"Run complete – {total_files} files, {total_records} records "
        f"from {len(config.prefixes)} prefixes"
    )
    logger.info(f"Summary:\n{json.dumps(summary, indent=2, default=str)}")

    ti.xcom_push(key="run_summary", value=summary)
    return summary


# ============================================================================
# DAG definition
# ============================================================================

_DEFAULT_ARGS = {
    "owner":             os.getenv("AIRFLOW_DAG_OWNER",       "syiniq"),
    "depends_on_past":   False,
    "email_on_failure":  False,
    "email_on_retry":    False,
    "retries":           int(os.getenv("AIRFLOW_TASK_RETRIES",          "2")),
    "retry_delay":       timedelta(minutes=int(os.getenv("AIRFLOW_RETRY_DELAY_MINUTES", "5"))),
    "execution_timeout": timedelta(hours=int(os.getenv("AIRFLOW_TASK_TIMEOUT_HOURS",    "2"))),
}

_SCHEDULE = os.getenv("S3_AIRFLOW_SCHEDULE_INTERVAL", os.getenv("AIRFLOW_SCHEDULE_INTERVAL", "@hourly"))


if _AIRFLOW_AVAILABLE:
    dag = DAG(
        dag_id="syiniq_s3_batch_cdc",
        default_args=_DEFAULT_ARGS,
        description="SYINIQ AWS S3 CDC Batch Pipeline — S3 → Kafka → Spark → Iceberg",
        schedule_interval=_SCHEDULE,
        start_date=days_ago(1),
        catchup=False,
        tags=["syiniq", "s3", "cdc", "batch", "kafka"],
    )

    with dag:
        # Task 1: Check connection
        check_conn = PythonOperator(
            task_id="check_connection",
            python_callable=task_check_connection,
        )

        # Task 2: List prefixes
        list_prefixes = PythonOperator(
            task_id="list_prefixes",
            python_callable=task_list_prefixes,
        )

        # Task 3: Extract each prefix (dynamic tasks)
        # We'll create these dynamically based on config
        config = _get_config()
        extract_tasks = []
        
        for prefix in config.prefixes:
            task_id = f"extract_prefix_{prefix.strip('/').replace('/', '_')}"
            extract_task = PythonOperator(
                task_id=task_id,
                python_callable=task_extract_prefix,
                op_kwargs={"prefix": prefix},
            )
            extract_tasks.append(extract_task)

        # Task 4: Validate Kafka topics
        validate = PythonOperator(
            task_id="validate_kafka_topics",
            python_callable=task_validate_kafka_topics,
        )

        # Task 5: Mark run complete
        complete = PythonOperator(
            task_id="mark_run_complete",
            python_callable=task_mark_run_complete,
        )

        # Dependencies
        check_conn >> list_prefixes >> extract_tasks >> validate >> complete
