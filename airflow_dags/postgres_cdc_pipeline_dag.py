"""
postgres_cdc_pipeline_dag.py
=============================
SYNIQAI - PostgreSQL CDC Path: Bronze health-check -> Silver -> Quality Gate -> Gold

Overview
--------
The PostgreSQL CDC path (Debezium -> Kafka -> spark_cdc_consumer.py -> Iceberg
Bronze) is the platform's proven end-to-end source, but everything downstream
of Bronze -- Silver transformation, the quality gate, and Gold aggregation --
currently runs as independently-invoked scripts, not one orchestrated
pipeline (see documentation/SYSTEM_ARCHITECTURE.md, sections 4 and 7). This
DAG closes that gap by calling the existing FastAPI backend routes for each
stage, in order, with retries and a hard quality gate -- mirroring the
structure of airflow_s3_dag.py (PythonOperator-only, XCom for inter-task
data, the same default_args shape).

    Task 1 - check_cdc_health
        Calls GET /api/cdc/consumer/health. Fails fast if the Spark CDC
        consumer, Kafka, or MinIO aren't up -- no point running downstream
        stages against a Bronze layer that isn't being fed.

    Task 2 - run_silver_transform  (one task per configured table)
        Calls POST /silver/transform-cdc for each Bronze CDC table, using the
        primary key(s) already registered for that table (mirrors
        SparkCDCConsumer.PK_REGISTRY in spark_cdc_consumer.py).

    Task 3 - run_quality_gate  (one task per configured table)
        Calls POST /api/quality-rules/tables/{table}/execute against the
        Silver output and FAILS the task (blocking Gold promotion) if the
        quality score drops below QUALITY_GATE_MIN_SCORE, or if no active
        rules are registered for the table (fail closed, not open).

    Task 4 - run_gold_aggregation  (one task per configured Gold target)
        Calls POST /gold/transform/aggregation for each configured
        Silver -> Gold aggregation.

    Task 5 - mark_run_complete
        Aggregates stats from all table tasks into a single completion
        summary, same pattern as airflow_s3_dag.py's mark_run_complete.

Configuration
-------------
The set of tables this DAG processes is configurable via the Airflow
Variable `POSTGRES_CDC_PIPELINE_CONFIG` (JSON) or the env var of the same
name for local/dev use -- see _default_pipeline_config() below for the
schema and a working single-table demo default (the "customers" table).
Edit that Variable to match your actual Bronze/Silver/Gold table names
rather than editing this file per-deployment.

Deployment
----------
1. Copy this file into your Airflow dags/ folder (or symlink it), same as
   airflow_s3_dag.py.
2. Ensure the FastAPI backend (gui/api/backend.py) is reachable at
   SYNIQAI_API_BASE_URL (default http://localhost:8000).
3. Trigger manually or wait for the next scheduled run:
   airflow dags trigger syniqai_postgres_cdc_pipeline
"""

from __future__ import annotations

import json
import logging
import os
from datetime import timedelta
from typing import Any, Dict, List

try:
    from airflow import DAG
    from airflow.operators.python import PythonOperator
    from airflow.utils.dates import days_ago
    from airflow.models import Variable
    _AIRFLOW_AVAILABLE = True
except ImportError:
    _AIRFLOW_AVAILABLE = False

    class DAG:
        def __init__(self, *args, **kwargs):
            pass

    class PythonOperator:
        def __init__(self, *args, **kwargs):
            pass

logger = logging.getLogger("syniqai.airflow.postgres_cdc_pipeline")

QUALITY_GATE_MIN_SCORE = float(os.getenv("QUALITY_GATE_MIN_SCORE", "80"))


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


def _default_pipeline_config() -> Dict[str, Any]:
    """
    One working demo entry (the 'customers' table) so this DAG runs
    out-of-the-box against a freshly started platform. Override via the
    POSTGRES_CDC_PIPELINE_CONFIG Airflow Variable to match real tables.
    """
    return {
        "api_base_url": os.getenv("SYNIQAI_API_BASE_URL", "http://localhost:8000"),
        "domain": "finance",
        "source": "postgres",
        "tables": [
            {
                "bronze_cdc_table": "local.bronze.postgres_customers",
                "silver_table": "syniqai_silver.finance.customers",
                "quality_gate_table_name": "customers",
                "primary_keys": ["id"],
                "gold": {
                    "gold_table": "syniqai_gold.finance.customer_summary",
                    "group_by": ["id"],
                    "aggregations": [
                        {"column": "*", "function": "count", "alias": "record_count"}
                    ],
                },
            }
        ],
    }


def _get_config() -> Dict[str, Any]:
    raw = _airflow_var("POSTGRES_CDC_PIPELINE_CONFIG", "")
    if not raw:
        return _default_pipeline_config()
    try:
        return json.loads(raw)
    except json.JSONDecodeError as e:
        logger.error(f"Invalid POSTGRES_CDC_PIPELINE_CONFIG JSON, falling back to default: {e}")
        return _default_pipeline_config()


def _table_task_id(prefix: str, table_entry: Dict[str, Any]) -> str:
    slug = table_entry["quality_gate_table_name"].strip("/").replace("/", "_").replace(".", "_")
    return f"{prefix}_{slug}"


def _generate_eda(config: Dict[str, Any], table_entry: Dict[str, Any], stage: str) -> None:
    """
    Regenerate the EDA report for a table so Silver/Gold EDA dashboards are
    pre-populated after each stage instead of requiring a manual "Generate"
    click. Non-fatal: EDA is a nice-to-have, not a pipeline correctness gate,
    so a failure here is logged and swallowed rather than failing the DAG.
    """
    import requests

    name = table_entry["quality_gate_table_name"]
    url = f"{config['api_base_url']}/gold/eda/generate"
    try:
        resp = requests.post(url, params={"source": config["source"], "entity": name}, timeout=300)
        resp.raise_for_status()
        logger.info(f"[{name}] EDA regenerated after {stage}")
    except Exception as e:
        logger.warning(f"[{name}] EDA auto-generation failed after {stage} (non-fatal): {e}")


# ============================================================================
# Task functions
# ============================================================================

# -- Task 1 -------------------------------------------------------------------
def task_check_cdc_health(**context: Any) -> Dict[str, Any]:
    """Fail fast if the CDC path (Kafka / Spark consumer / MinIO) isn't up."""
    logger.info("=== Task 1: check_cdc_health ===")
    import requests

    config = _get_config()
    url = f"{config['api_base_url']}/api/cdc/consumer/health"
    resp = requests.get(url, timeout=15)
    resp.raise_for_status()
    result = resp.json()

    logger.info(f"CDC health check result:\n{json.dumps(result, indent=2, default=str)}")

    health = result.get("health", {})
    if not result.get("success") or health.get("overall_status") != "healthy":
        raise RuntimeError(
            f"CDC path is not healthy (status={health.get('overall_status')!r}); "
            f"refusing to run downstream Silver/Gold stages: {json.dumps(health.get('checks', {}))}"
        )

    context["ti"].xcom_push(key="health_result", value=result)
    return result


# -- Task 2 (per table) --------------------------------------------------------
def task_run_silver_transform(table_entry: Dict[str, Any], **context: Any) -> Dict[str, Any]:
    """Transform one Bronze CDC table into Silver via /silver/transform-cdc."""
    name = table_entry["quality_gate_table_name"]
    logger.info(f"=== Task 2: run_silver_transform [{name}] ===")
    import requests

    config = _get_config()
    url = f"{config['api_base_url']}/silver/transform-cdc"
    payload = {
        "bronze_cdc_table": table_entry["bronze_cdc_table"],
        "silver_table": table_entry["silver_table"],
        "primary_keys": table_entry["primary_keys"],
        "quality_rules": table_entry.get("quality_rules", []),
    }
    resp = requests.post(url, json=payload, timeout=300)
    resp.raise_for_status()
    result = resp.json()

    if result.get("success") is False:
        raise RuntimeError(f"Silver transform failed for {name}: {result.get('error', result)}")

    logger.info(f"[{name}] Silver transform complete: {json.dumps(result, default=str)[:500]}")
    _generate_eda(config, table_entry, stage="Silver transform")
    context["ti"].xcom_push(key=f"silver_{name}", value=result)
    return result


# -- Task 3 (per table) --------------------------------------------------------
def task_run_quality_gate(table_entry: Dict[str, Any], **context: Any) -> Dict[str, Any]:
    """
    Run the quality rule engine against Silver output and BLOCK promotion to
    Gold if the score is below threshold, or if no active rules exist for
    the table (fail closed, matching the CDC merge fix's fail-closed intent).
    """
    name = table_entry["quality_gate_table_name"]
    logger.info(f"=== Task 3: run_quality_gate [{name}] ===")
    import requests

    config = _get_config()
    url = f"{config['api_base_url']}/api/quality-rules/tables/{name}/execute"
    payload = {"domain": config["domain"], "source": config["source"], "limit": None}
    resp = requests.post(url, json=payload, timeout=300)
    resp.raise_for_status()
    result = resp.json()

    if result.get("success") is False:
        raise RuntimeError(
            f"Quality gate blocked for {name}: {result.get('error', 'no active rules found')}"
        )

    score = result.get("quality_score", 0)
    if score < QUALITY_GATE_MIN_SCORE:
        raise RuntimeError(
            f"Quality gate blocked for {name}: score {score} < required {QUALITY_GATE_MIN_SCORE} "
            f"({result.get('rows_failed', '?')} rows failed / "
            f"{result.get('rows_quarantined', '?')} quarantined)"
        )

    logger.info(f"[{name}] Quality gate passed: score={score}")
    context["ti"].xcom_push(key=f"quality_{name}", value=result)
    return result


# -- Task 4 (per table) --------------------------------------------------------
def task_run_gold_aggregation(table_entry: Dict[str, Any], **context: Any) -> Dict[str, Any]:
    """Promote Silver to Gold via /gold/transform/aggregation."""
    name = table_entry["quality_gate_table_name"]
    logger.info(f"=== Task 4: run_gold_aggregation [{name}] ===")
    import requests

    config = _get_config()
    gold = table_entry["gold"]
    url = f"{config['api_base_url']}/gold/transform/aggregation"
    payload = {
        "silver_table": table_entry["silver_table"],
        "gold_table": gold["gold_table"],
        "group_by": gold["group_by"],
        "aggregations": gold["aggregations"],
    }
    resp = requests.post(url, json=payload, timeout=600)
    resp.raise_for_status()
    result = resp.json()

    if result.get("success") is False:
        raise RuntimeError(f"Gold aggregation failed for {name}: {result.get('error', result)}")

    logger.info(f"[{name}] Gold aggregation complete: {json.dumps(result, default=str)[:500]}")
    _generate_eda(config, table_entry, stage="Gold aggregation")
    context["ti"].xcom_push(key=f"gold_{name}", value=result)
    return result


# -- Task 5 -------------------------------------------------------------------
def task_mark_run_complete(**context: Any) -> Dict[str, Any]:
    """Aggregate stats from all table tasks into one completion summary."""
    logger.info("=== Task 5: mark_run_complete ===")
    ti = context["ti"]
    config = _get_config()

    summary: Dict[str, Any] = {
        "dag_run_id": context.get("run_id", "unknown"),
        "domain": config["domain"],
        "source": config["source"],
        "tables": {},
        "status": "success",
    }

    for table_entry in config["tables"]:
        name = table_entry["quality_gate_table_name"]
        summary["tables"][name] = {
            "silver": ti.xcom_pull(key=f"silver_{name}", task_ids=_table_task_id("run_silver_transform", table_entry)),
            "quality": ti.xcom_pull(key=f"quality_{name}", task_ids=_table_task_id("run_quality_gate", table_entry)),
            "gold": ti.xcom_pull(key=f"gold_{name}", task_ids=_table_task_id("run_gold_aggregation", table_entry)),
        }

    logger.info(f"Run complete:\n{json.dumps(summary, indent=2, default=str)}")
    ti.xcom_push(key="run_summary", value=summary)
    return summary


# ============================================================================
# DAG definition
# ============================================================================

_DEFAULT_ARGS = {
    "owner": os.getenv("AIRFLOW_DAG_OWNER", "syniqai"),
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": int(os.getenv("AIRFLOW_TASK_RETRIES", "2")),
    "retry_delay": timedelta(minutes=int(os.getenv("AIRFLOW_RETRY_DELAY_MINUTES", "5"))),
    "execution_timeout": timedelta(hours=int(os.getenv("AIRFLOW_TASK_TIMEOUT_HOURS", "2"))),
}

_SCHEDULE = os.getenv("POSTGRES_CDC_AIRFLOW_SCHEDULE_INTERVAL", os.getenv("AIRFLOW_SCHEDULE_INTERVAL", None))


if _AIRFLOW_AVAILABLE:
    dag = DAG(
        dag_id="syniqai_postgres_cdc_pipeline",
        default_args=_DEFAULT_ARGS,
        description="SyniqAI PostgreSQL CDC Pipeline — Bronze health-check → Silver → Quality Gate → Gold",
        schedule_interval=_SCHEDULE,  # None = manual trigger only by default
        start_date=days_ago(1),
        catchup=False,
        tags=["syniqai", "postgres", "cdc", "medallion"],
    )

    with dag:
        check_health = PythonOperator(
            task_id="check_cdc_health",
            python_callable=task_check_cdc_health,
        )

        complete = PythonOperator(
            task_id="mark_run_complete",
            python_callable=task_mark_run_complete,
        )

        config = _get_config()
        for table_entry in config["tables"]:
            silver_task = PythonOperator(
                task_id=_table_task_id("run_silver_transform", table_entry),
                python_callable=task_run_silver_transform,
                op_kwargs={"table_entry": table_entry},
            )
            quality_task = PythonOperator(
                task_id=_table_task_id("run_quality_gate", table_entry),
                python_callable=task_run_quality_gate,
                op_kwargs={"table_entry": table_entry},
            )
            gold_task = PythonOperator(
                task_id=_table_task_id("run_gold_aggregation", table_entry),
                python_callable=task_run_gold_aggregation,
                op_kwargs={"table_entry": table_entry},
            )

            check_health >> silver_task >> quality_task >> gold_task >> complete
