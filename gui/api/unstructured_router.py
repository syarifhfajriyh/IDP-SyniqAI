"""
Unstructured Silver Layer Router
=================================
FastAPI router for unstructured-media transformation jobs.

Endpoints
---------
POST /api/silver/unstructured/process          – start a processing job
PUT  /api/silver/unstructured/rules/{type}     – update transformation rules
GET  /api/silver/unstructured/jobs             – list all jobs
GET  /api/silver/unstructured/jobs/{job_id}    – get one job's status
GET  /api/silver/unstructured/stats            – source connector stats
GET  /api/silver/unstructured/tables           – list Silver Iceberg tables
GET  /api/silver/unstructured/preview/{type}   – preview processed records
POST /api/silver/unstructured/validate         – validate source connectivity

Wired into backend.py via:

    from unstructured_router import router as unstructured_router, setup_unstructured_router
    setup_unstructured_router(silver_job_tracker, config, gold_layer_path, _find_java_17)
    app.include_router(unstructured_router, prefix="/api/silver/unstructured",
                       tags=["Unstructured Silver"])
"""

from __future__ import annotations

import logging
import os
import sys
import threading
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, BackgroundTasks, HTTPException
from pydantic import BaseModel

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Shared state injected by backend.py at startup
# ---------------------------------------------------------------------------
_silver_job_tracker = None   # SilverJobTracker instance
_config = None               # AppConfig instance (has .minio, .postgres …)
_gold_layer_path: Optional[Path] = None  # path to data lakehouse/syniq_project
_find_java_17_fn = None      # callable() → java_home str | None

# Persistent transformation rules store (in-memory; survives restarts via
# optional disk backing if needed)
_transformation_rules: Dict[str, Dict[str, Any]] = {
    "image": {
        "resize": False, "resizeWidth": 224, "resizeHeight": 224,
        "normalizePixels": False, "grayscale": False,
        "edgeDetection": False, "objectDetection": False,
        "formatConversion": None,
    },
    "video": {
        "normalizeFPS": False, "targetFPS": 30, "compression": False,
        "formatConversion": None,
    },
    "audio": {
        "normalizeVolume": False, "channelConfig": None,
        "formatConversion": None,
    },
    "document": {
        "extractText": True, "detectLanguage": True,
        "sentimentAnalysis": False, "piiRedaction": False,
        "chunkSize": 1024,
    },
    "text": {
        "extractText": True, "detectLanguage": True,
        "sentimentAnalysis": False, "piiRedaction": False,
    },
}

_rules_lock = threading.Lock()


# ---------------------------------------------------------------------------
# Setup function called by backend.py
# ---------------------------------------------------------------------------

def setup_unstructured_router(
    silver_job_tracker,
    config,
    gold_layer_path: Path,
    find_java_17_fn,
) -> None:
    """Inject shared dependencies from backend.py."""
    global _silver_job_tracker, _config, _gold_layer_path, _find_java_17_fn
    _silver_job_tracker = silver_job_tracker
    _config = config
    _gold_layer_path = gold_layer_path
    _find_java_17_fn = find_java_17_fn
    logger.info("✅ UnstructuredRouter dependencies injected")


# ---------------------------------------------------------------------------
# Router
# ---------------------------------------------------------------------------
router = APIRouter(tags=["Unstructured Silver"])


# ---------------------------------------------------------------------------
# Pydantic models
# ---------------------------------------------------------------------------

class UnstructuredProcessRequest(BaseModel):
    """Request body for POST /process"""
    media_type: str                       # "image" | "video" | "audio" | "document"
    domain: str = "media"
    entity: str = "assets"
    execution_mode: str = "full"          # "full" | "incremental"
    # Source connector configs (optional – merged with server-side env config)
    mongodb_config: Optional[Dict[str, Any]] = None
    s3_config: Optional[Dict[str, Any]] = None
    # Override transform rules for this single run (optional)
    transforms: Optional[Dict[str, Any]] = None
    # Quality rules
    rules: List[Dict[str, Any]] = []
    # If files are already in Bronze, skip staging
    stage_to_bronze: bool = True
    # Max assets to fetch from source connectors
    limit: int = 10_000


class TransformRulesUpdate(BaseModel):
    """Request body for PUT /rules/{type}"""
    rules: Dict[str, Any]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _get_minio_config() -> Dict[str, Any]:
    """Build minio config from injected AppConfig or fall back to env vars."""
    if _config and hasattr(_config, "minio"):
        m = _config.minio
        return {
            "endpoint": m.endpoint,
            "access_key": m.access_key,
            "secret_key": m.secret_key,
            "secure": getattr(m, "secure", False),
        }
    return {
        "endpoint": os.getenv("MINIO_ENDPOINT", "http://localhost:9000"),
        "access_key": os.getenv("MINIO_ACCESS_KEY", "admin"),
        "secret_key": os.getenv("MINIO_SECRET_KEY", "password123"),
        "secure": False,
    }


def _get_processor_config(
    mongodb_config: Optional[Dict[str, Any]],
    s3_config: Optional[Dict[str, Any]],
) -> Dict[str, Any]:
    """
    Merge request-provided connector configs with environment-sourced defaults.
    Request values take precedence (allowing per-call overrides from the UI).
    """
    default_mongo = {
        "host": os.getenv("MONGODB_HOST", "localhost"),
        "port": int(os.getenv("MONGODB_PORT", "27017")),
        "database": os.getenv("MONGODB_DATABASE", "media"),
        "user": os.getenv("MONGODB_USER", ""),
        "password": os.getenv("MONGODB_PASSWORD", ""),
    }

    default_s3 = {
        "bucket": os.getenv("S3_RAW_MEDIA_BUCKET", "syniq-raw-media"),
        "aws_access_key_id": os.getenv("AWS_ACCESS_KEY_ID", ""),
        "aws_secret_access_key": os.getenv("AWS_SECRET_ACCESS_KEY", ""),
        "region_name": os.getenv("AWS_DEFAULT_REGION", "us-east-1"),
    }

    merged: Dict[str, Any] = {"minio": _get_minio_config()}

    # Use request-provided config if present, else fall back to env ONLY when
    # the env vars are explicitly set (not just the hardcoded "localhost" fallback).
    # This prevents the pipeline from blindly trying localhost:27017 when no
    # MongoDB was configured in the UI.
    if mongodb_config:
        merged["mongodb"] = {**default_mongo, **mongodb_config}
    elif os.getenv("MONGODB_HOST"):
        # Only use the default if MONGODB_HOST was explicitly set in the environment
        merged["mongodb"] = default_mongo

    if s3_config:
        # Normalize short-form key names used by the UI to boto3-style names
        normalized_s3 = dict(s3_config)
        if "aws_access_key" in normalized_s3 and "aws_access_key_id" not in normalized_s3:
            normalized_s3["aws_access_key_id"] = normalized_s3.pop("aws_access_key")
        if "aws_secret_key" in normalized_s3 and "aws_secret_access_key" not in normalized_s3:
            normalized_s3["aws_secret_access_key"] = normalized_s3.pop("aws_secret_key")
        if "aws_region" in normalized_s3 and "region_name" not in normalized_s3:
            normalized_s3["region_name"] = normalized_s3.pop("aws_region").strip()
        if "s3_bucket" in normalized_s3 and "bucket" not in normalized_s3:
            normalized_s3["bucket"] = normalized_s3.pop("s3_bucket")
        if "s3_prefix" in normalized_s3 and "prefix" not in normalized_s3:
            normalized_s3["prefix"] = normalized_s3.pop("s3_prefix")
        merged["s3"] = {**default_s3, **normalized_s3}
    elif default_s3.get("aws_access_key_id"):
        merged["s3"] = default_s3

    return merged


def _configure_java() -> None:
    """Set JAVA_HOME to Java 17 if needed (required for Spark 3.5+)."""
    if _find_java_17_fn is None:
        return
    java_home = _find_java_17_fn()
    if java_home:
        os.environ["JAVA_HOME"] = java_home
        os.environ["PATH"] = f"{java_home}\\bin;{os.environ.get('PATH', '')}"
        logger.info(f"✓ Java 17 configured: {java_home}")
    else:
        raise RuntimeError(
            "Java 17 is required for Spark 3.5+. "
            "Install it from https://adoptium.net/temurin/releases/?version=17"
        )


def _bootstrap_processor_path() -> None:
    """Add syniq_project + data ingestion connectors to sys.path."""
    if _gold_layer_path and str(_gold_layer_path) not in sys.path:
        sys.path.insert(0, str(_gold_layer_path))
        sys.path.insert(0, str(_gold_layer_path / "ingestion"))
        sys.path.insert(0, str(_gold_layer_path / "connectors"))
    # Also add the data ingestion Connector directory as a fallback
    data_ingestion_path = _gold_layer_path.parent.parent / "data ingestion" / "Connector" if _gold_layer_path else None
    if data_ingestion_path and data_ingestion_path.exists() and str(data_ingestion_path) not in sys.path:
        sys.path.insert(0, str(data_ingestion_path))


def _run_lightweight_job(
    job_id: str,
    media_type: str,
    domain: str,
    entity: str,
    processor_config: Dict[str, Any],
    stage_to_bronze: bool,
    limit: int,
    transforms: Optional[Dict[str, Any]] = None,
    rules: Optional[List[Dict[str, Any]]] = None,
) -> None:
    """
    No-Spark fallback pipeline: lists assets from S3 and/or MongoDB,
    applies transform metadata, and stages to MinIO Bronze.
    Used automatically when Java 17 / PySpark is unavailable.
    """
    try:
        import json
        from io import BytesIO

        _silver_job_tracker.update_status(
            job_id=job_id, status="running",
            message="Listing assets (lightweight mode – no Spark)…", progress=20
        )

        s3_cfg = processor_config.get("s3") or {}
        minio_cfg = processor_config.get("minio") or {}
        assets: List[Dict[str, Any]] = []

        if s3_cfg and s3_cfg.get("aws_access_key_id") and s3_cfg.get("bucket"):
            try:
                import boto3
                s3_client = boto3.client(
                    "s3",
                    aws_access_key_id=s3_cfg["aws_access_key_id"],
                    aws_secret_access_key=s3_cfg.get("aws_secret_access_key", ""),
                    region_name=s3_cfg.get("region_name", "us-east-1").strip(),
                )
                bucket = s3_cfg["bucket"]
                prefix = s3_cfg.get("prefix", "")
                paginator = s3_client.get_paginator("list_objects_v2")
                for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
                    for obj in page.get("Contents", []):
                        key = obj["Key"]
                        ext = key.rsplit(".", 1)[-1].lower() if "." in key else ""
                        assets.append({
                            "source": "s3",
                            "bucket": bucket,
                            "key": key,
                            "size_bytes": obj.get("Size", 0),
                            "last_modified": obj["LastModified"].isoformat(),
                            "media_type": media_type,
                            "ext": ext,
                            "domain": domain,
                            "entity": entity,
                            "ingested_at": datetime.utcnow().isoformat(),
                        })
                        if len(assets) >= limit:
                            break
                    if len(assets) >= limit:
                        break
                logger.info(f"Lightweight job {job_id}: discovered {len(assets)} S3 assets")
            except Exception as s3_exc:
                logger.warning(f"S3 listing failed for job {job_id}: {s3_exc}")

        # ---- MongoDB source ----
        mongo_cfg = processor_config.get("mongodb") or {}
        if mongo_cfg and (mongo_cfg.get("host") or mongo_cfg.get("uri")):
            try:
                from pymongo import MongoClient  # type: ignore
                uri = mongo_cfg.get("uri") or "mongodb://{}:{}".format(
                    mongo_cfg["host"], mongo_cfg.get("port", 27017)
                )
                username = mongo_cfg.get("username") or mongo_cfg.get("user", "")
                password = mongo_cfg.get("password", "")
                if username and password and "://" in uri and "@" not in uri:
                    proto, rest = uri.split("://", 1)
                    uri = f"{proto}://{username}:{password}@{rest}"
                db_name = mongo_cfg.get("database", domain)
                collection_name = mongo_cfg.get("collection", entity)
                client = MongoClient(uri, serverSelectionTimeoutMS=5000)
                col = client[db_name][collection_name]
                projection = {
                    "_id": 1, "file_path": 1, "s3_path": 1, "url": 1,
                    "filename": 1, "file_name": 1, "size": 1,
                    "created_at": 1, "updated_at": 1,
                    "media_type": 1, "content_type": 1,
                }
                remaining = limit - len(assets)
                for doc in col.find({}, projection).limit(remaining):
                    fp = (
                        doc.get("file_path") or doc.get("s3_path") or
                        doc.get("url") or doc.get("filename") or
                        doc.get("file_name") or str(doc["_id"])
                    )
                    ext = fp.rsplit(".", 1)[-1].lower() if "." in fp else ""
                    assets.append({
                        "source": "mongodb",
                        "collection": f"{db_name}.{collection_name}",
                        "doc_id": str(doc["_id"]),
                        "file_path": fp,
                        "size_bytes": doc.get("size", 0),
                        "last_modified": str(
                            doc.get("updated_at") or doc.get("created_at") or ""
                        ),
                        "media_type": media_type,
                        "ext": ext,
                        "domain": domain,
                        "entity": entity,
                        "ingested_at": datetime.utcnow().isoformat(),
                    })
                    if len(assets) >= limit:
                        break
                client.close()
                logger.info(
                    f"Lightweight job {job_id}: discovered {len(assets)} MongoDB assets "
                    f"from {db_name}.{collection_name}"
                )
            except Exception as mongo_exc:
                logger.warning(f"MongoDB listing failed for job {job_id}: {mongo_exc}")

        _silver_job_tracker.update_status(
            job_id=job_id, status="running",
            message=f"Staging {len(assets)} asset records to Bronze…", progress=60
        )

        # Stage metadata JSON to MinIO Bronze
        if minio_cfg:
            try:
                from minio import Minio  # type: ignore
                endpoint = minio_cfg["endpoint"].replace("http://", "").replace("https://", "")
                mc = Minio(
                    endpoint,
                    access_key=minio_cfg.get("access_key", ""),
                    secret_key=minio_cfg.get("secret_key", ""),
                    secure=minio_cfg.get("secure", False),
                )
                bronze_bucket = "syniqai-bronze"
                try:
                    if not mc.bucket_exists(bronze_bucket):
                        mc.make_bucket(bronze_bucket)
                except Exception:
                    pass
                meta_payload = {
                    "assets": assets,
                    "transforms_applied": transforms or {},
                    "rules_applied": rules or [],
                    "job_id": job_id,
                    "generated_at": datetime.utcnow().isoformat(),
                }
                meta_bytes = json.dumps(meta_payload, indent=2, default=str).encode("utf-8")
                ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
                object_name = f"{domain}/{entity}/{media_type}/metadata_{ts}.json"
                mc.put_object(
                    bronze_bucket, object_name,
                    BytesIO(meta_bytes), len(meta_bytes),
                    content_type="application/json",
                )
                logger.info(f"Lightweight job {job_id}: staged metadata to minio://{bronze_bucket}/{object_name}")
            except Exception as minio_exc:
                logger.warning(f"MinIO staging failed for job {job_id}: {minio_exc}")

        _silver_job_tracker.update_status(
            job_id=job_id, status="completed",
            message=f"Lightweight pipeline completed – {len(assets)} assets indexed", progress=100
        )
        _silver_job_tracker.update_results(
            job_id=job_id,
            silver_path=f"minio://syniqai-bronze/{domain}/{entity}/{media_type}/",
            row_count=len(assets),
            cleaning_summary={
                "assets_discovered": len(assets),
                "assets_staged": len(assets),
                "mode": "lightweight",
                "media_type": media_type,
                "transforms_applied": list((transforms or {}).keys()),
                "sources": list({a["source"] for a in assets}),
            },
        )

    except Exception as exc:
        import traceback
        logger.error(f"Lightweight job {job_id} failed: {exc}\n{traceback.format_exc()}")
        _silver_job_tracker.update_status(
            job_id=job_id, status="failed",
            message="Lightweight pipeline error",
            error_message=f"{type(exc).__name__}: {exc}",
        )


# ---------------------------------------------------------------------------
# Background task runner
# ---------------------------------------------------------------------------

def _run_unstructured_job(
    job_id: str,
    media_type: str,
    domain: str,
    entity: str,
    execution_mode: str,
    processor_config: Dict[str, Any],
    transforms: Dict[str, Any],
    rules: List[Dict[str, Any]],
    stage_to_bronze: bool,
    limit: int,
) -> None:
    """Background task: runs the full unstructured pipeline and updates tracker."""

    if _silver_job_tracker is None:
        logger.error("silver_job_tracker not initialised – cannot run job")
        return

    spark_available = True
    try:
        _configure_java()
    except RuntimeError as exc:
        spark_available = False
        logger.warning(f"Java 17 / Spark unavailable – using lightweight pipeline for job {job_id}: {exc}")

    if not spark_available:
        _run_lightweight_job(
            job_id=job_id,
            media_type=media_type,
            domain=domain,
            entity=entity,
            processor_config=processor_config,
            stage_to_bronze=stage_to_bronze,
            limit=limit,
            transforms=transforms,
            rules=rules,
        )
        return

    _bootstrap_processor_path()

    _silver_job_tracker.update_status(
        job_id=job_id, status="running",
        message=f"Initialising {media_type} pipeline…", progress=5
    )

    try:
        from spark_unstructured_processor import UnstructuredProcessor  # type: ignore
    except ImportError as exc:
        _silver_job_tracker.update_status(
            job_id=job_id, status="failed",
            message="Cannot import UnstructuredProcessor",
            error_message=str(exc)
        )
        return

    _silver_job_tracker.update_status(
        job_id=job_id, status="running",
        message="Discovering assets from source connectors…", progress=10
    )

    try:
        processor = UnstructuredProcessor(processor_config)
        result = processor.run(
            media_type=media_type,
            domain=domain,
            entity=entity,
            transforms=transforms,
            execution_mode=execution_mode,
            rules=rules,
            limit=limit,
            stage_to_bronze=stage_to_bronze,
        )

        if result.get("status") == "completed":
            _silver_job_tracker.update_status(
                job_id=job_id, status="completed",
                message="Unstructured pipeline completed", progress=100
            )
            _silver_job_tracker.update_results(
                job_id=job_id,
                silver_path=result.get("silver_table"),
                row_count=result.get("row_count") or result.get("metrics", {}).get("processed_files"),
                cleaning_summary={
                    "assets_discovered": result.get("assets_discovered", 0),
                    "assets_staged": result.get("assets_staged", 0),
                    "duration_seconds": result.get("total_duration_seconds", 0),
                    "media_type": media_type,
                },
            )
        else:
            _silver_job_tracker.update_status(
                job_id=job_id, status="failed",
                message="Pipeline returned failed status",
                error_message=result.get("error", "Unknown error"),
            )

    except Exception as exc:
        import traceback
        logger.error(f"Unstructured job {job_id} failed: {exc}\n{traceback.format_exc()}")
        _silver_job_tracker.update_status(
            job_id=job_id, status="failed",
            message="Pipeline exception",
            error_message=f"{type(exc).__name__}: {exc}",
        )


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

@router.post("/process")
async def start_unstructured_processing(
    request: UnstructuredProcessRequest,
    background_tasks: BackgroundTasks,
):
    """
    Start an unstructured-media silver transformation job.

    The job runs in the background.  Poll ``GET /jobs/{job_id}`` for status.
    """
    if _silver_job_tracker is None:
        raise HTTPException(status_code=503, detail="Service not yet initialised")

    valid_types = {"image", "video", "audio", "document", "text", "pdf"}
    if request.media_type not in valid_types:
        raise HTTPException(
            status_code=422,
            detail=f"media_type must be one of {sorted(valid_types)}"
        )

    # Build the job ID
    job_id = (
        f"unstructured_{request.domain}_{request.entity}_"
        f"{request.media_type}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    )

    # Register in tracker
    bronze_path = (
        f"s3a://syniqai-bronze/{request.domain}/"
        f"{request.s3_config.get('bucket', 's3') if request.s3_config else 'mongodb'}/"
        f"{request.entity}/"
    )
    _silver_job_tracker.create_job(
        job_id=job_id,
        table_name=f"{request.domain}.unstructured.{request.media_type}.{request.entity}",
        source=request.s3_config.get("bucket", "unstructured") if request.s3_config else "mongodb",
        entity=request.entity,
        bronze_path=bronze_path,
    )

    # Resolve transform rules: explicit request overrides > stored server-side rules > defaults
    with _rules_lock:
        base_rules = dict(_transformation_rules.get(request.media_type, {}))
    transforms = {**base_rules, **(request.transforms or {})}

    # Build processor config
    processor_config = _get_processor_config(request.mongodb_config, request.s3_config)

    # Queue background task
    background_tasks.add_task(
        _run_unstructured_job,
        job_id=job_id,
        media_type=request.media_type,
        domain=request.domain,
        entity=request.entity,
        execution_mode=request.execution_mode,
        processor_config=processor_config,
        transforms=transforms,
        rules=request.rules,
        stage_to_bronze=request.stage_to_bronze,
        limit=request.limit,
    )

    return {
        "job_id": job_id,
        "status": "queued",
        "media_type": request.media_type,
        "domain": request.domain,
        "entity": request.entity,
        "message": f"Unstructured {request.media_type} pipeline queued for {request.domain}.{request.entity}",
        "silver_table": (
            f"syniq_iceberg.{request.domain}."
            f"unstructured_{request.media_type}_{request.entity}"
        ),
    }


@router.get("/jobs")
def list_unstructured_jobs(status: Optional[str] = None, limit: int = 50):
    """List unstructured processing jobs."""
    if _silver_job_tracker is None:
        return {"jobs": [], "total": 0}

    all_jobs = _silver_job_tracker.list_jobs(limit=limit * 3, status=status)

    # Filter to only unstructured jobs
    jobs = [j for j in all_jobs if j.get("job_id", "").startswith("unstructured_")]
    return {"jobs": jobs[:limit], "total": len(jobs)}


@router.get("/jobs/{job_id}")
def get_unstructured_job(job_id: str):
    """Get the status of a single unstructured pipeline job."""
    if _silver_job_tracker is None:
        raise HTTPException(status_code=503, detail="Service not yet initialised")

    job = _silver_job_tracker.get_job(job_id)
    if job is None:
        raise HTTPException(status_code=404, detail=f"Job '{job_id}' not found")
    return job


@router.put("/rules/{media_type}")
def update_transformation_rules(media_type: str, body: TransformRulesUpdate):
    """
    Persist transformation rules for a given media type server-side.

    These rules are used as the baseline for all subsequent processing jobs
    unless explicitly overridden in the job request.
    """
    valid_types = {"image", "video", "audio", "document", "text", "pdf"}
    if media_type not in valid_types:
        raise HTTPException(
            status_code=422,
            detail=f"media_type must be one of {sorted(valid_types)}"
        )

    with _rules_lock:
        current = _transformation_rules.setdefault(media_type, {})
        current.update(body.rules)
        snapshot = dict(current)

    return {
        "media_type": media_type,
        "rules": snapshot,
        "updated_at": datetime.now().isoformat(),
    }


@router.get("/rules/{media_type}")
def get_transformation_rules(media_type: str):
    """Return current server-side transformation rules for a media type."""
    with _rules_lock:
        rules = dict(_transformation_rules.get(media_type, {}))
    return {"media_type": media_type, "rules": rules}


@router.get("/rules")
def list_all_transformation_rules():
    """Return all transformation rules for all media types."""
    with _rules_lock:
        snapshot = {k: dict(v) for k, v in _transformation_rules.items()}
    return {"rules": snapshot}


@router.get("/stats")
def get_source_stats(
    mongodb_host: str = "localhost",
    mongodb_port: int = 27017,
    mongodb_database: str = "media",
    s3_bucket: str = "",
    aws_access_key_id: str = "",
    aws_secret_access_key: str = "",
    aws_region: str = "us-east-1",
):
    """
    Return asset counts from configured source connectors (MongoDB + S3).
    Query params allow a quick per-request connection override.
    """
    _bootstrap_processor_path()

    mongodb_cfg = None
    s3_cfg = None

    # Use env / query-param values only when they look configured
    mongo_host = os.getenv("MONGODB_HOST", mongodb_host)
    if mongo_host and mongo_host not in ("localhost", "127.0.0.1"):
        mongodb_cfg = {
            "host": mongo_host,
            "port": int(os.getenv("MONGODB_PORT", str(mongodb_port))),
            "database": os.getenv("MONGODB_DATABASE", mongodb_database),
            "user": os.getenv("MONGODB_USER", ""),
            "password": os.getenv("MONGODB_PASSWORD", ""),
        }

    real_bucket = os.getenv("S3_RAW_MEDIA_BUCKET", s3_bucket)
    real_key = os.getenv("AWS_ACCESS_KEY_ID", aws_access_key_id)
    if real_bucket and real_key:
        s3_cfg = {
            "bucket": real_bucket,
            "aws_access_key_id": real_key,
            "aws_secret_access_key": os.getenv("AWS_SECRET_ACCESS_KEY", aws_secret_access_key),
            "region_name": os.getenv("AWS_DEFAULT_REGION", aws_region),
        }

    processor_config = _get_processor_config(mongodb_cfg, s3_cfg)
    processor_config["minio"] = _get_minio_config()

    try:
        from spark_unstructured_processor import UnstructuredProcessor  # type: ignore
        processor = UnstructuredProcessor(processor_config)
        stats = processor.get_source_stats()
        return {"status": "ok", "sources": stats}
    except Exception as exc:
        logger.warning(f"get_source_stats failed: {exc}")
        return {
            "status": "unavailable",
            "sources": {},
            "error": str(exc),
        }


@router.get("/models")
def list_ml_models():
    """Return the ML model registry used by the unstructured pipeline."""
    # Try to enrich with job history (counts, last_run)
    job_counts: Dict[str, int] = {}
    if _silver_job_tracker:
        try:
            all_jobs = _silver_job_tracker.list_jobs()
            for job in all_jobs:
                mt = job.get("cleaning_summary", {}).get("media_type") or job.get("entity", "")
                if mt:
                    job_counts[mt] = job_counts.get(mt, 0) + 1
        except Exception:
            pass

    registry = [
        {
            "id": "img_clf",
            "name": "Image Classifier",
            "type": "Image Classification",
            "framework": "PyTorch",
            "version": "v1.0",
            "status": "deployed",
            "thumbnail": "🏷️",
            "inputs": "Images (any resolution)",
            "outputs": "class labels + confidence",
            "predictions": job_counts.get("image", 0),
        },
        {
            "id": "obj_det",
            "name": "Object Detector (YOLOv8)",
            "type": "Object Detection",
            "framework": "ONNX",
            "version": "v1.0",
            "status": "deployed",
            "thumbnail": "🎯",
            "inputs": "Images / Video frames",
            "outputs": "bounding boxes + labels",
            "predictions": job_counts.get("image", 0) + job_counts.get("video", 0),
        },
        {
            "id": "asr",
            "name": "Speech-to-Text (Whisper)",
            "type": "ASR / NLP",
            "framework": "OpenAI Whisper",
            "version": "v1.0",
            "status": "deployed",
            "thumbnail": "🎙️",
            "inputs": "Audio files (mp3/wav/flac)",
            "outputs": "transcript + language + segments",
            "predictions": job_counts.get("audio", 0),
        },
        {
            "id": "doc_ocr",
            "name": "Document OCR & Extractor",
            "type": "Document AI",
            "framework": "Tesseract / pdfplumber",
            "version": "v1.0",
            "status": "deployed",
            "thumbnail": "📄",
            "inputs": "PDF / DOCX / scanned images",
            "outputs": "extracted text + pages + language",
            "predictions": job_counts.get("document", 0) + job_counts.get("pdf", 0),
        },
        {
            "id": "vid_proc",
            "name": "Video Frame Processor",
            "type": "Video Analysis",
            "framework": "OpenCV",
            "version": "v1.0",
            "status": "deployed",
            "thumbnail": "🎬",
            "inputs": "Video files (mp4/avi/mov)",
            "outputs": "keyframes + metadata + quality score",
            "predictions": job_counts.get("video", 0),
        },
    ]
    return {"models": registry, "total": len(registry)}


@router.get("/health")
def get_service_health():
    """Quick health probe for MinIO and pipeline components."""
    services = []

    # MinIO probe
    try:
        from minio import Minio
        minio_cfg = _get_minio_config()
        ep = minio_cfg["endpoint"].replace("http://", "").replace("https://", "")
        mc = Minio(ep, access_key=minio_cfg["access_key"],
                   secret_key=minio_cfg["secret_key"],
                   secure=minio_cfg.get("secure", False))
        mc.list_buckets()
        services.append({"service": "Object Storage (MinIO)", "status": "healthy", "uptime": "—"})
    except Exception as exc:
        services.append({"service": "Object Storage (MinIO)", "status": "unavailable", "uptime": "—", "error": str(exc)})

    # Processor path probe
    try:
        _bootstrap_processor_path()
        services.append({"service": "Unstructured Processor", "status": "healthy", "uptime": "—"})
    except Exception:
        services.append({"service": "Unstructured Processor", "status": "unavailable", "uptime": "—"})

    # Job tracker probe
    if _silver_job_tracker is not None:
        services.append({"service": "Job Tracker", "status": "healthy", "uptime": "—"})
    else:
        services.append({"service": "Job Tracker", "status": "starting", "uptime": "—"})

    overall = "ok" if all(s["status"] == "healthy" for s in services) else "degraded"
    return {"status": overall, "services": services}


@router.get("/tables")
def list_unstructured_silver_tables():
    """
    List Silver Iceberg tables that belong to the unstructured namespace.
    Falls back to MinIO Bronze bucket scan if Spark/Iceberg is unavailable.
    """
    _bootstrap_processor_path()

    # Attempt Spark / Iceberg table listing
    try:
        if _gold_layer_path:
            sys.path.insert(0, str(_gold_layer_path))
            from utils.spark_iceberg_config import create_spark_session_with_iceberg  # type: ignore

            _configure_java()
            spark = create_spark_session_with_iceberg(
                app_name="ListUnstructuredTables",
                minio_config=_get_minio_config(),
            )
            rows = spark.sql(
                "SHOW TABLES IN syniq_iceberg LIKE '*unstructured*'"
            ).collect()
            spark.stop()
            # Normalize to always have table_name key
            tables = [
                {
                    "table_name": f"{r.asDict().get('namespace', '')}.{r.asDict().get('tableName', r.asDict().get('table_name', ''))}",
                    **r.asDict()
                }
                for r in rows
            ]
            return {"catalog": "syniq_iceberg", "tables": tables, "total": len(tables)}
    except Exception as exc:
        logger.warning(f"Iceberg listing unavailable: {exc}; falling back to MinIO scan")

    # MinIO Bronze scan fallback — returns normalized table_name
    try:
        from minio import Minio
        minio_cfg = _get_minio_config()
        ep = minio_cfg["endpoint"].replace("http://", "").replace("https://", "")
        mc = Minio(ep, access_key=minio_cfg["access_key"],
                   secret_key=minio_cfg["secret_key"],
                   secure=minio_cfg.get("secure", False))
        objs = list(mc.list_objects("syniqai-bronze", recursive=True))
        media_prefixes: dict = {}
        for obj in objs:
            parts = obj.object_name.split("/")
            if len(parts) >= 2:
                key = "/".join(parts[:3]) if len(parts) >= 3 else "/".join(parts[:2])
                if key not in media_prefixes:
                    media_prefixes[key] = {"count": 0, "size": 0}
                media_prefixes[key]["count"] += 1
                media_prefixes[key]["size"] += obj.size or 0
        tables = [
            {
                "table_name": prefix.replace("/", "."),
                "prefix": prefix,
                "catalog": "bronze",
                "row_count": info["count"],
                "size_bytes": info["size"],
            }
            for prefix, info in sorted(media_prefixes.items())
        ]
        return {"catalog": "bronze_fallback", "tables": tables, "total": len(tables)}
    except Exception as exc:
        logger.error(f"MinIO fallback also failed: {exc}")
        return {"catalog": "syniq_iceberg", "tables": [], "total": 0, "error": str(exc)}


@router.get("/preview/{media_type}")
def preview_processed_records(
    media_type: str,
    domain: str = "media",
    entity: str = "assets",
    limit: int = 20,
):
    """
    Return a sample of processed records from the Silver Iceberg table
    for the given media type.  Used by the React front-end to populate
    FileBrowser, ObjectDetection, AudioAnalysis, and TextExtraction views.
    """
    _bootstrap_processor_path()

    valid_types = {"image", "video", "audio", "document", "text", "pdf"}
    if media_type not in valid_types:
        raise HTTPException(
            status_code=422,
            detail=f"media_type must be one of {sorted(valid_types)}"
        )

    # Try Spark Iceberg Silver table first
    try:
        if _gold_layer_path is None:
            raise RuntimeError("gold_layer_path not configured")

        _configure_java()
        sys.path.insert(0, str(_gold_layer_path))
        from utils.spark_iceberg_config import create_spark_session_with_iceberg  # type: ignore

        spark = create_spark_session_with_iceberg(
            app_name="PreviewUnstructured",
            minio_config=_get_minio_config(),
        )

        table = f"syniq_iceberg.{domain}.{entity}_files"
        rows = (
            spark.table(table)
            .filter(f"_data_type = '{media_type}'")
            .limit(limit)
            .collect()
        )
        spark.stop()

        records = [row.asDict() for row in rows]
        for rec in records:
            for k, v in rec.items():
                if hasattr(v, "isoformat"):
                    rec[k] = v.isoformat()
        return {"media_type": media_type, "records": records, "total": len(records), "source": "silver_iceberg"}

    except Exception as exc:
        logger.warning(f"Silver Iceberg preview unavailable ({exc}); falling back to Bronze scan")

    # Fallback: scan MinIO Bronze bucket for matching files
    try:
        from minio import Minio
        from pathlib import Path as _Path

        _EXT_MAP = {
            "image":    {".jpg", ".jpeg", ".png", ".gif", ".bmp", ".tiff", ".webp"},
            "video":    {".mp4", ".avi", ".mov", ".wmv", ".mkv", ".webm"},
            "audio":    {".mp3", ".wav", ".flac", ".aac", ".ogg", ".m4a"},
            "document": {".pdf", ".docx", ".doc"},
            "text":     {".txt", ".csv", ".json", ".log"},
            "pdf":      {".pdf"},
        }
        valid_exts = _EXT_MAP.get(media_type, set())

        minio_cfg = _get_minio_config()
        ep = minio_cfg["endpoint"].replace("http://", "").replace("https://", "")
        mc = Minio(ep, access_key=minio_cfg["access_key"],
                   secret_key=minio_cfg["secret_key"],
                   secure=minio_cfg.get("secure", False))

        # Scan bronze bucket under domain prefix
        prefix = f"{domain}/"
        objs = list(mc.list_objects("syniqai-bronze", prefix=prefix, recursive=True))

        records = []
        for obj in objs:
            name = obj.object_name.split("/")[-1]
            ext = _Path(name).suffix.lower() if "." in name else ""
            # Include if extension matches, OR if no extension filter set,
            # OR if it's a parquet file (treat as structured→unstructured metadata)
            if valid_exts and ext not in valid_exts and ext != ".parquet":
                continue
            records.append({
                "file_name": name,
                "file_extension": ext.lstrip("."),
                "file_size_bytes": obj.size,
                "last_modified": obj.last_modified.isoformat() if obj.last_modified else None,
                "s3_path": f"s3a://syniqai-bronze/{obj.object_name}",
                "processing_status": "in_bronze",
            })
            if len(records) >= limit:
                break

        return {
            "media_type": media_type,
            "records": records,
            "total": len(records),
            "source": "bronze_fallback",
            "note": "Silver Iceberg tables not yet created. Run a pipeline from Feature Pipeline to generate Silver data.",
        }
    except Exception as fallback_exc:
        logger.error(f"Bronze fallback preview also failed: {fallback_exc}")
        return {
            "media_type": media_type,
            "records": [],
            "total": 0,
            "source": "error",
            "error": str(fallback_exc),
        }


@router.post("/validate")
def validate_source_connectivity(
    mongodb_config: Optional[Dict[str, Any]] = None,
    s3_config: Optional[Dict[str, Any]] = None,
):
    """
    Validate that the configured source connectors (MongoDB / S3) are reachable.
    """
    _bootstrap_processor_path()
    results: Dict[str, Any] = {}

    if mongodb_config:
        try:
            from connectors.mongodb_connector import MongoDBConnector  # type: ignore
            conn = MongoDBConnector(mongodb_config)
            conn.connect()
            results["mongodb"] = {"status": "ok", "database": mongodb_config.get("database")}
            conn.client.close()
        except Exception as exc:
            results["mongodb"] = {"status": "error", "error": str(exc)}

    if s3_config:
        try:
            from connectors.s3_connector import S3Connector  # type: ignore
            conn = S3Connector(s3_config)
            ok = conn.connect()
            if ok:
                stats = conn.get_bucket_stats()
                results["s3"] = {"status": "ok", **stats}
                conn.disconnect()
            else:
                results["s3"] = {"status": "error", "error": "connect() returned False"}
        except Exception as exc:
            results["s3"] = {"status": "error", "error": str(exc)}

    if not results:
        return {"status": "no_connectors_provided", "results": {}}

    overall = "ok" if all(v.get("status") == "ok" for v in results.values()) else "partial"
    return {"status": overall, "results": results}

