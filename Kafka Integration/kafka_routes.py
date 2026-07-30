"""
kafka_routes.py
===============
FastAPI router that exposes all Kafka management endpoints to the SYINIQ GUI.

Mounted in backend.py with:
    from kafka_routes import router as kafka_router
    app.include_router(kafka_router, prefix="/api/kafka", tags=["Kafka"])

All endpoints are prefixed /api/kafka/...
"""

from __future__ import annotations

import logging
from typing import Any, Dict, Optional

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Import KafkaService and KafkaBridge
# ---------------------------------------------------------------------------
try:
    from kafka_service import KafkaService
    from kafka_bridge import get_kafka_bridge
except ImportError:
    # If running from a different location, try relative import
    try:
        from .kafka_service import KafkaService
        from .kafka_bridge import get_kafka_bridge
    except ImportError:
        logger.error("Failed to import Kafka components")
        raise

_kafka_service = KafkaService()

# ---------------------------------------------------------------------------
# Router
# ---------------------------------------------------------------------------
router = APIRouter(tags=["Kafka"])


# ---------------------------------------------------------------------------
# Pydantic Models
# ---------------------------------------------------------------------------
class CreateTopicRequest(BaseModel):
    name: str
    num_partitions: int = 3
    replication_factor: int = 1


class RegisterConnectorRequest(BaseModel):
    name: str
    config: Dict[str, Any]


# ---------------------------------------------------------------------------
# ---- STATUS / DASHBOARD ----
# ---------------------------------------------------------------------------
@router.get("/status")
def get_kafka_status():
    """
    Dashboard summary: broker health, topic count, connector count, message totals.
    """
    try:
        summary = _kafka_service.dashboard_summary()
        return summary
    except Exception as exc:
        logger.error(f"Failed to get Kafka status: {exc}")
        raise HTTPException(status_code=500, detail=str(exc))


@router.get("/health")
def check_kafka_health():
    """
    Lightweight health check: ping Kafka broker and Kafka Connect.
    """
    return _kafka_service.cluster_health()


@router.get("/bridge")
def get_kafka_bridge_status():
    """
    Return the status of the in-process KafkaBridge producer.
    """
    bridge = get_kafka_bridge()
    return bridge.health_check()


# ---------------------------------------------------------------------------
# ---- TOPICS ----
# ---------------------------------------------------------------------------
@router.get("/topics")
def list_kafka_topics():
    """
    List all managed Kafka topics with partition and offset information.
    """
    result = _kafka_service.list_topics()
    if result.get("error"):
        raise HTTPException(status_code=500, detail=result["error"])
    return result


@router.get("/topics/{topic_name}/sample")
def sample_topic_messages(
    topic_name: str,
    max_messages: int = Query(10, gt=0, le=100),
):
    """
    Return up to `max_messages` recent messages from a topic.
    """
    result = _kafka_service.topic_sample(topic_name, max_messages)
    if result.get("error"):
        raise HTTPException(status_code=404, detail=result["error"])
    return result


@router.post("/topics")
def create_kafka_topic(request: CreateTopicRequest):
    """
    Create a new Kafka topic.
    """
    result = _kafka_service.create_topic(
        request.name,
        request.num_partitions,
        request.replication_factor,
    )
    if not result.get("success", False):
        raise HTTPException(status_code=500, detail=result.get("error", "Unknown error"))
    return result


# ---------------------------------------------------------------------------
# ---- CONNECTORS (Debezium / Kafka Connect) ----
# ---------------------------------------------------------------------------
@router.get("/connectors")
def list_kafka_connectors():
    """
    List all registered Kafka Connect connectors with status.
    """
    result = _kafka_service.list_connectors()
    if result.get("error"):
        raise HTTPException(status_code=500, detail=result["error"])
    return result


@router.get("/connectors/{connector_name}")
def get_connector_detail(connector_name: str):
    """
    Get status and config of a single connector.
    """
    result = _kafka_service.get_connector(connector_name)
    if result.get("error"):
        raise HTTPException(status_code=404, detail=result["error"])
    return result


@router.post("/connectors/{connector_name}/restart")
def restart_kafka_connector(connector_name: str):
    """
    Restart a Kafka Connect connector.
    """
    result = _kafka_service.restart_connector(connector_name)
    if not result.get("success", False):
        raise HTTPException(status_code=500, detail=result.get("error", "Unknown error"))
    return result


@router.put("/connectors/{connector_name}/pause")
def pause_kafka_connector(connector_name: str):
    """
    Pause a Kafka Connect connector.
    """
    result = _kafka_service.pause_connector(connector_name)
    if not result.get("success", False):
        raise HTTPException(status_code=500, detail=result.get("error", "Unknown error"))
    return {"success": True, "message": f"Connector '{connector_name}' paused"}


@router.put("/connectors/{connector_name}/resume")
def resume_kafka_connector(connector_name: str):
    """
    Resume a paused Kafka Connect connector.
    """
    result = _kafka_service.resume_connector(connector_name)
    if not result.get("success", False):
        raise HTTPException(status_code=500, detail=result.get("error", "Unknown error"))
    return {"success": True, "message": f"Connector '{connector_name}' resumed"}


@router.delete("/connectors/{connector_name}")
def delete_kafka_connector(connector_name: str):
    """
    Delete (unregister) a Kafka Connect connector.
    """
    result = _kafka_service.delete_connector(connector_name)
    if not result.get("success", False):
        raise HTTPException(status_code=500, detail=result.get("error", "Unknown error"))
    return {"success": True, "message": f"Connector '{connector_name}' deleted"}


@router.post("/connectors")
def register_kafka_connector(request: RegisterConnectorRequest):
    """
    Register a new connector (e.g., Debezium) via Kafka Connect REST API.
    
    Example payload:
    {
        "name": "syiniq-debezium-postgres",
        "config": {
            "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
            "database.hostname": "localhost",
            "database.port": "5432",
            ...
        }
    }
    """
    result = _kafka_service.register_debezium_connector(request.name, request.config)
    if not result.get("success", False):
        raise HTTPException(status_code=500, detail=result.get("error", "Unknown error"))
    return result


# ---------------------------------------------------------------------------
# ---- MONGODB BATCH PIPELINE (Airflow → Kafka → Spark → MinIO Iceberg) ----
# ---------------------------------------------------------------------------

import os
import requests
from pathlib import Path
from datetime import datetime, timezone
from fastapi import BackgroundTasks

# Resolve paths relative to this file
_THIS_DIR = Path(__file__).resolve().parent
# Inside Docker, call Airflow via host.docker.internal; on bare metal use localhost.
# Set AIRFLOW_BASE_URL env var to override.
_is_in_docker = Path("/.dockerenv").exists()
_AIRFLOW_BASE_URL = os.getenv(
    "AIRFLOW_BASE_URL",
    "http://host.docker.internal:8085" if _is_in_docker else "http://localhost:8085",
)
# Kafka bootstrap: inside Docker use the PLAINTEXT_INTERNAL listener (kafka:29092)
# which correctly advertises an internal hostname; outside Docker use localhost:9092.
_KAFKA_BOOTSTRAP_INTERNAL = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS_INTERNAL",
    "kafka:29092" if _is_in_docker else os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
)
_AIRFLOW_DAG_ID = "syiniq_mongodb_batch_cdc"
_AIRFLOW_AUTH = (
    os.getenv("AIRFLOW_ADMIN_USER", "admin"),
    os.getenv("AIRFLOW_ADMIN_PASSWORD", "admin"),
)
_WATERMARK_PATHS = [
    _THIS_DIR / "watermarks" / "mongodb_watermarks.json",
    _THIS_DIR.parent / "airflow" / "watermarks" / "mongodb_watermarks.json",
]


def _read_watermarks() -> Dict[str, str]:
    """Read watermarks from the first file that exists."""
    import json
    for path in _WATERMARK_PATHS:
        if path.exists():
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)
    return {}


@router.get("/airflow/status")
def get_airflow_status():
    """Check if Airflow is reachable."""
    try:
        response = requests.get(
            f"{_AIRFLOW_BASE_URL}/api/v1/health",
            auth=_AIRFLOW_AUTH,
            timeout=5,
        )
        if response.status_code == 200:
            return {"status": "ok", "airflow_url": _AIRFLOW_BASE_URL}
        return {"status": "error", "message": f"HTTP {response.status_code}"}
    except Exception as exc:
        return {"status": "error", "message": str(exc)}


def _airflow_get(path: str, timeout: int = 15) -> Dict[str, Any]:
    """GET from Airflow REST API, return parsed JSON."""
    url = f"{_AIRFLOW_BASE_URL}/api/v1{path}"
    try:
        resp = requests.get(url, auth=_AIRFLOW_AUTH, timeout=timeout)
        resp.raise_for_status()
        return resp.json()
    except requests.exceptions.ConnectionError:
        return {"error": f"Cannot connect to Airflow at {_AIRFLOW_BASE_URL}"}
    except requests.exceptions.Timeout:
        return {"error": "Airflow request timeout"}
    except Exception as exc:
        return {"error": str(exc)}


def _airflow_post(path: str, body: Dict[str, Any], timeout: int = 30) -> Dict[str, Any]:
    """POST to Airflow REST API, return parsed JSON."""
    url = f"{_AIRFLOW_BASE_URL}/api/v1{path}"
    try:
        resp = requests.post(url, auth=_AIRFLOW_AUTH, json=body, timeout=timeout)
        resp.raise_for_status()
        return resp.json()
    except requests.exceptions.ConnectionError:
        return {"error": f"Cannot connect to Airflow at {_AIRFLOW_BASE_URL}"}
    except requests.exceptions.Timeout:
        return {"error": "Airflow request timeout"}
    except Exception as exc:
        return {"error": str(exc)}


def _airflow_patch(path: str, body: Dict[str, Any], timeout: int = 15) -> Dict[str, Any]:
    """PATCH Airflow REST API."""
    url = f"{_AIRFLOW_BASE_URL}/api/v1{path}"
    try:
        resp = requests.patch(url, auth=_AIRFLOW_AUTH, json=body, timeout=timeout)
        resp.raise_for_status()
        return resp.json()
    except requests.exceptions.ConnectionError:
        return {"error": f"Cannot connect to Airflow at {_AIRFLOW_BASE_URL}"}
    except requests.exceptions.Timeout:
        return {"error": "Airflow request timeout"}
    except Exception as exc:
        return {"error": str(exc)}


@router.get("/mongodb/pipeline")
def get_mongodb_pipeline_status():
    """Get status of the MongoDB batch CDC pipeline."""
    result = _airflow_get(f"/dags/{_AIRFLOW_DAG_ID}")
    if "error" in result:
        return {"status": "error", "message": result["error"]}
    return {
        "status": "ok",
        "dag_id": _AIRFLOW_DAG_ID,
        "is_paused": result.get("is_paused", True),
        "last_parsed_time": result.get("last_parsed_time"),
        "tags": result.get("tags", []),
    }


@router.post("/mongodb/pipeline/trigger")
def trigger_mongodb_dag():
    """Trigger a new run of the MongoDB CDC DAG."""
    body = {"conf": {}}
    result = _airflow_post(f"/dags/{_AIRFLOW_DAG_ID}/dagRuns", body)
    if "error" in result:
        raise HTTPException(status_code=500, detail=result["error"])
    return {"success": True, "dag_run_id": result.get("dag_run_id")}


@router.patch("/mongodb/pipeline/pause")
def toggle_mongodb_dag_pause(paused: bool = True):
    """Pause or unpause the MongoDB CDC DAG."""
    body = {"is_paused": paused}
    result = _airflow_patch(f"/dags/{_AIRFLOW_DAG_ID}", body)
    if "error" in result:
        raise HTTPException(status_code=500, detail=result["error"])
    return {"success": True, "is_paused": paused}


@router.get("/mongodb/health")
def check_mongodb_health(uri: str = Query("mongodb://localhost:27017")):
    """Test MongoDB connection."""
    try:
        from pymongo import MongoClient
        client = MongoClient(uri, serverSelectionTimeoutMS=5000)
        client.admin.command("ping")
        return {"status": "ok", "uri": uri}
    except Exception as exc:
        return {"status": "error", "error": str(exc)}


@router.post("/mongodb/watermarks/reset")
def reset_mongodb_watermarks():
    """Delete MongoDB watermarks file to force full re-extraction."""
    import json
    for path in _WATERMARK_PATHS:
        if path.exists():
            path.unlink()
            return {"success": True, "message": f"Deleted {path}"}
    return {"success": False, "message": "No watermarks file found"}


@router.get("/mongodb/watermarks")
def get_mongodb_watermarks():
    """Get current MongoDB watermarks."""
    watermarks = _read_watermarks()
    return {"watermarks": watermarks}


@router.get("/mongodb/dag/runs")
def get_mongodb_dag_runs(limit: int = Query(10, ge=1, le=50)):
    """Get recent MongoDB DAG runs."""
    result = _airflow_get(f"/dags/{_AIRFLOW_DAG_ID}/dagRuns?limit={limit}")
    if "error" in result:
        return {"error": result["error"]}
    return result


@router.post("/mongodb/pipeline/spark")
def run_spark_consumer(background_tasks: BackgroundTasks, dry_run: bool = False):
    """Run Spark consumer to process MongoDB data from Kafka."""
    def _run_spark():
        try:
            from spark_mongodb_batch_consumer import run_pipeline
            run_pipeline(dry_run=dry_run)
        except Exception as exc:
            logger.error(f"Spark consumer failed: {exc}")
    
    background_tasks.add_task(_run_spark)
    return {"success": True, "message": "Spark consumer started in background"}


# ============================================================================
# AWS S3 Batch CDC routes
# Mirrors the MongoDB routes above, but for the syiniq_s3_batch_cdc DAG.
# ============================================================================

_S3_DAG_ID = "syiniq_s3_batch_cdc"
_S3_WATERMARK_PATHS = [
    _THIS_DIR / "watermarks" / "s3_watermarks.json",
    _THIS_DIR.parent / "airflow" / "watermarks" / "s3_watermarks.json",
]


def _read_s3_watermarks() -> Dict[str, str]:
    """Read S3 per-prefix watermarks from the first file that exists."""
    import json
    for path in _S3_WATERMARK_PATHS:
        if path.exists():
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)
    return {}


@router.get("/s3/pipeline")
def get_s3_pipeline_status():
    """Get status of the S3 batch CDC pipeline."""
    result = _airflow_get(f"/dags/{_S3_DAG_ID}")
    if "error" in result:
        return {"status": "error", "message": result["error"]}
    return {
        "status": "ok",
        "dag_id": _S3_DAG_ID,
        "is_paused": result.get("is_paused", True),
        "last_parsed_time": result.get("last_parsed_time"),
        "tags": result.get("tags", []),
    }


@router.post("/s3/pipeline/trigger")
def trigger_s3_dag():
    """Trigger a new run of the S3 CDC DAG."""
    body = {"conf": {}}
    result = _airflow_post(f"/dags/{_S3_DAG_ID}/dagRuns", body)
    if "error" in result:
        raise HTTPException(status_code=500, detail=result["error"])
    return {"success": True, "dag_run_id": result.get("dag_run_id")}


@router.patch("/s3/pipeline/pause")
def toggle_s3_dag_pause(paused: bool = True):
    """Pause or unpause the S3 CDC DAG."""
    body = {"is_paused": paused}
    result = _airflow_patch(f"/dags/{_S3_DAG_ID}", body)
    if "error" in result:
        raise HTTPException(status_code=500, detail=result["error"])
    return {"success": True, "is_paused": paused}


@router.get("/s3/watermarks")
def get_s3_watermarks():
    """Get current S3 watermarks."""
    watermarks = _read_s3_watermarks()
    return {"watermarks": watermarks}


@router.get("/s3/dag/runs")
def get_s3_dag_runs(limit: int = Query(10, ge=1, le=50)):
    """Get recent S3 DAG runs."""
    result = _airflow_get(f"/dags/{_S3_DAG_ID}/dagRuns?limit={limit}")
    if "error" in result:
        return {"error": result["error"]}
    return result


@router.post("/s3/pipeline/spark")
def run_s3_spark_consumer(background_tasks: BackgroundTasks, dry_run: bool = False):
    """Run Spark consumer to process S3 data from Kafka."""
    def _run_spark():
        try:
            from spark_s3_batch_consumer import run_pipeline
            run_pipeline(dry_run=dry_run)
        except Exception as exc:
            logger.error(f"Spark S3 consumer failed: {exc}")
    
    background_tasks.add_task(_run_spark)
    return {"success": True, "message": "Spark S3 consumer started in background"}
