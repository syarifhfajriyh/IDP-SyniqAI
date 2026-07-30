"""
Quality Rules API Routes
Provides endpoints for data quality rule management, execution, and monitoring
"""
from fastapi import APIRouter, HTTPException, BackgroundTasks
from pydantic import BaseModel, Field
from typing import List, Dict, Any, Optional
from datetime import datetime
import logging
import uuid

# Import lineage store for tracking quality checks
try:
    from lineage_routes import lineage_store
    LINEAGE_AVAILABLE = True
except ImportError:
    LINEAGE_AVAILABLE = False
    logging.warning("LineageStore not available for quality rules")

from database import rules_repo, quarantine_repo, execution_log_repo, db_manager
from quality_check_executor import QualityCheckExecutor

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/quality-rules", tags=["quality-rules"])

# Initialize executor
quality_executor = QualityCheckExecutor()


# Pydantic Models
class QualityRule(BaseModel):
    rule_name: str
    domain: str
    category: str  # validation, transformation, masking, referential_integrity, anomaly_detection, compliance, data_quality, schema_validation
    rule_type: str  # not_null, range_check, regex_format, enum_validation, unique, foreign_key, cross_column_logic, sql_expression, data_type_check, anomaly_detection, masking_rule
    description: Optional[str] = None
    target_table: str
    target_columns: List[str] = []  # Array of column names
    condition_expression: str
    severity: str  # CRITICAL, HIGH, WARNING, INFO (uppercase)
    action: str  # quarantine_row, log, block_table
    execution_priority: int = 5  # 1-10 (lower = higher priority)
    created_by: str = "system"


class RuleUpdate(BaseModel):
    rule_name: Optional[str] = None
    description: Optional[str] = None
    condition_expression: Optional[str] = None
    severity: Optional[str] = None
    action: Optional[str] = None
    status: Optional[str] = None
    is_active: Optional[bool] = None


class ExecuteQualityCheckRequest(BaseModel):
    table_name: str
    domain: str = "finance"
    source: str = "postgres"
    limit: Optional[int] = None


# ========== GET ROUTES ==========

@router.get("/tables/{table_name}")
async def get_rules_for_table(table_name: str, domain: str = "finance"):
    """
    Get all active quality rules for a specific table
    """
    try:
        rules = rules_repo.get_rules_by_table(domain, table_name)
        
        return {
            "success": True,
            "table_name": table_name,
            "domain": domain,
            "rule_count": len(rules),
            "rules": rules
        }
    except Exception as e:
        logger.error(f"Error fetching rules for {table_name}: {e}")
        return {
            "success": False,
            "error": str(e),
            "rules": []
        }


@router.get("/tables/{table_name}/execution-history")
async def get_execution_history(table_name: str, domain: str = "finance", limit: int = 10):
    """
    Get quality check execution history for a table
    """
    try:
        logger.info(f"Fetching execution history for table={table_name}, domain={domain}")
        
        # Get execution history directly from the table
        history = execution_log_repo.get_execution_history_by_table(domain, table_name, limit)
        
        logger.info(f"✓ Found {len(history)} execution records for {table_name}")
        if history:
            logger.info(f"Sample record: {history[0]}")
        
        return {
            "success": True,
            "table_name": table_name,
            "domain": domain,
            "history_count": len(history),
            "history": history
        }
    except Exception as e:
        logger.error(f"Error fetching execution history: {e}")
        return {
            "success": False,
            "error": str(e),
            "history": []
        }


@router.get("/tables/{table_name}/quarantine")
async def get_quarantine_records(
    table_name: str,
    domain: str = "finance",
    status: str = "pending",
    limit: int = 100
):
    """
    Get quarantine records for a table
    """
    try:
        records = quarantine_repo.get_quarantine_records(domain, status, limit)
        
        # Filter by table_name
        filtered_records = [r for r in records if r.get('source_table') == table_name]
        
        return {
            "success": True,
            "table_name": table_name,
            "domain": domain,
            "status": status,
            "record_count": len(filtered_records),
            "records": filtered_records
        }
    except Exception as e:
        logger.error(f"Error fetching quarantine records: {e}")
        return {
            "success": False,
            "error": str(e),
            "records": []
        }


@router.get("/quarantine/summary")
async def get_quarantine_summary(domain: str = "finance"):
    """
    Get quarantine summary statistics for a domain
    """
    try:
        summary = quarantine_repo.get_quarantine_summary(domain)
        
        return {
            "success": True,
            "domain": domain,
            "summary": summary
        }
    except Exception as e:
        logger.error(f"Error fetching quarantine summary: {e}")
        return {
            "success": False,
            "error": str(e),
            "summary": {}
        }


# ========== POST ROUTES ==========

@router.post("/tables/{table_name}/execute")
async def execute_quality_check(
    table_name: str,
    request: ExecuteQualityCheckRequest,
    background_tasks: BackgroundTasks
):
    """
    Execute quality checks on a table
    Returns execution results and creates quarantine records for failures
    """
    try:
        logger.info(f"Executing quality check on {table_name}")
        
        # Get rules for this table
        all_rules = rules_repo.get_rules_by_table(request.domain, table_name)
        
        # Filter only active rules for execution
        rules = [rule for rule in all_rules if rule.get('is_active', False)]
        
        if not rules:
            return {
                "success": False,
                "error": f"No active rules found for {table_name}",
                "execution_id": None
            }
        
        # Execute quality checks (synchronous for now, can be async later)
        execution_id = str(uuid.uuid4())
        results = await quality_executor.execute_checks(
            table_name=table_name,
            domain=request.domain,
            source=request.source,
            rules=rules,
            limit=request.limit,
            execution_id=execution_id
        )
        
        # Record lineage for quality check transformation
        if LINEAGE_AVAILABLE:
            try:
                # Determine layers based on source
                source_layer = 'bronze' if 'bronze' in request.source.lower() else 'silver'
                target_layer = 'silver' if source_layer == 'bronze' else 'gold'
                
                # Calculate quality metrics
                total_checks = results.get('total_checks', 0)
                passed_checks = results.get('passed_checks', 0)
                failed_checks = results.get('failed_checks', 0)
                quality_score = (passed_checks / total_checks * 100) if total_checks > 0 else 0.0
                
                lineage_entry = {
                    "timestamp": datetime.utcnow().isoformat(),
                    "source": {
                        "layer": source_layer,
                        "location": f"{table_name}_raw" if source_layer == 'bronze' else table_name
                    },
                    "target": {
                        "layer": target_layer,
                        "location": table_name
                    },
                    "transformation": "quality_validation",
                    "metrics": {
                        "row_count": results.get('total_rows', 0),
                        "columns_used": [rule.get('target_columns', []) for rule in rules]
                    },
                    "metadata": {
                        "domain": request.domain,
                        "execution_id": execution_id,
                        "total_checks": total_checks,
                        "passed_checks": passed_checks,
                        "failed_checks": failed_checks,
                        "quality_score": quality_score,
                        "rules_applied": len(rules)
                    }
                }
                
                lineage_store.record_transformation(lineage_entry)
                logger.info(f"📊 Quality check lineage recorded for {table_name}")
            except Exception as e:
                logger.warning(f"Failed to record quality check lineage: {e}")
        
        return {
            "success": True,
            "execution_id": execution_id,
            "table_name": table_name,
            "results": results
        }
        
    except Exception as e:
        logger.error(f"Error executing quality check: {e}")
        return {
            "success": False,
            "error": str(e),
            "execution_id": None
        }


@router.post("/rules")
async def create_rule(rule: QualityRule):
    """
    Create a new quality rule
    """
    try:
        rule_data = rule.dict()
        rule_id = rules_repo.create_rule(rule_data)
        
        return {
            "success": True,
            "rule_id": rule_id,
            "message": f"Rule '{rule.rule_name}' created successfully"
        }
    except Exception as e:
        logger.error(f"Error creating rule: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.put("/rules/{rule_id}")
async def update_rule(rule_id: str, updates: RuleUpdate):
    """
    Update an existing rule
    """
    try:
        update_dict = {k: v for k, v in updates.dict().items() if v is not None}
        
        if not update_dict:
            raise HTTPException(status_code=400, detail="No updates provided")
        
        success = rules_repo.update_rule(rule_id, update_dict)
        
        return {
            "success": success,
            "rule_id": rule_id,
            "message": "Rule updated successfully"
        }
    except Exception as e:
        logger.error(f"Error updating rule: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/rules/{rule_id}")
async def delete_rule(rule_id: str):
    """
    Soft delete a rule (archives it)
    """
    try:
        success = rules_repo.delete_rule(rule_id)
        
        return {
            "success": success,
            "rule_id": rule_id,
            "message": "Rule archived successfully"
        }
    except Exception as e:
        logger.error(f"Error deleting rule: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/quarantine/{quarantine_id}/resolve")
async def resolve_quarantine(
    quarantine_id: str,
    resolution: str,
    resolved_by: str = "system"
):
    """
    Mark a quarantine record as resolved
    """
    try:
        success = quarantine_repo.resolve_quarantine(
            quarantine_id,
            resolution,
            resolved_by
        )
        
        return {
            "success": success,
            "quarantine_id": quarantine_id,
            "message": "Quarantine record resolved"
        }
    except Exception as e:
        logger.error(f"Error resolving quarantine: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/quarantine/download/{execution_id}")
async def download_quarantine_data(
    execution_id: str,
    domain: str = "finance",
    source: str = "postgres",
    table_name: str = None
):
    """
    Download quarantined records as CSV from MinIO
    """
    try:
        from minio_utils import MinIOClient
        import io
        import pandas as pd
        from fastapi.responses import StreamingResponse
        
        minio_client = MinIOClient()
        quarantine_bucket = "syniqai-quarantine"
        
        # List all files in the execution path
        prefix = f"{domain}/{source}/{table_name}/{execution_id}" if table_name else f"{domain}/{source}"
        
        logger.info(f"Downloading quarantine data from: {quarantine_bucket}/{prefix}")
        
        objects = minio_client.client.list_objects(quarantine_bucket, prefix=prefix, recursive=True)
        
        all_data = []
        for obj in objects:
            if obj.object_name.endswith('.parquet'):
                # Download parquet file
                response = minio_client.client.get_object(quarantine_bucket, obj.object_name)
                data = response.read()
                
                # Read parquet and append to list
                df = pd.read_parquet(io.BytesIO(data))
                df['quarantine_file'] = obj.object_name
                all_data.append(df)
        
        if not all_data:
            return {
                "success": False,
                "error": "No quarantine data found"
            }
        
        # Combine all dataframes
        combined_df = pd.concat(all_data, ignore_index=True)
        
        # Convert to CSV
        csv_buffer = io.StringIO()
        combined_df.to_csv(csv_buffer, index=False)
        csv_buffer.seek(0)
        
        return StreamingResponse(
            iter([csv_buffer.getvalue()]),
            media_type="text/csv",
            headers={
                "Content-Disposition": f"attachment; filename=quarantine_{execution_id}.csv"
            }
        )

    except Exception as e:
        logger.error(f"Error downloading quarantine data: {e}")
        return {
            "success": False,
            "error": str(e)
        }


# ============================================================================
# Quality trend history — server-side persistence
# Replaces the frontend's localStorage-based trend (fragile: lost on cache
# clear, not shared across users/sessions) with a real time series backed by
# Postgres, using the same connection pool as the rest of the rule engine.
# ============================================================================

def _ensure_quality_history_table():
    try:
        with db_manager.get_cursor() as cursor:
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS quality_history_snapshots (
                    id SERIAL PRIMARY KEY,
                    domain VARCHAR(50) NOT NULL,
                    week_label VARCHAR(20),
                    quality_score DECIMAL(5,2) NOT NULL,
                    completeness DECIMAL(5,2) NOT NULL,
                    missing_values BIGINT NOT NULL DEFAULT 0,
                    table_count INT NOT NULL DEFAULT 0,
                    recorded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_quality_history_domain_time
                ON quality_history_snapshots(domain, recorded_at)
            """)
        logger.info("✓ quality_history_snapshots table ready")
    except Exception as e:
        logger.warning(f"Could not ensure quality_history_snapshots table: {e}")


_ensure_quality_history_table()


class QualityHistorySnapshotRequest(BaseModel):
    domain: str
    week_label: Optional[str] = None
    quality_score: float
    completeness: float
    missing_values: int = 0
    table_count: int = 0


@router.post("/history")
def save_quality_history_snapshot(snapshot: QualityHistorySnapshotRequest):
    """Persist a quality trend snapshot server-side."""
    try:
        with db_manager.get_cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO quality_history_snapshots
                    (domain, week_label, quality_score, completeness, missing_values, table_count)
                VALUES (%s, %s, %s, %s, %s, %s)
                RETURNING id, recorded_at
                """,
                (
                    snapshot.domain, snapshot.week_label, snapshot.quality_score,
                    snapshot.completeness, snapshot.missing_values, snapshot.table_count,
                ),
            )
            row = cursor.fetchone()
        return {"success": True, "id": row[0], "recorded_at": row[1].isoformat()}
    except Exception as e:
        logger.error(f"Error saving quality history snapshot: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/history")
def get_quality_history(domain: str = "finance", limit: int = 12):
    """Return the most recent quality trend snapshots for a domain, oldest first."""
    try:
        rows = db_manager.execute_query_dict(
            """
            SELECT domain, week_label, quality_score, completeness, missing_values,
                   table_count, recorded_at
            FROM quality_history_snapshots
            WHERE domain = %s
            ORDER BY recorded_at DESC
            LIMIT %s
            """,
            (domain, limit),
        )
        rows = list(reversed(rows))  # oldest first, for chart rendering
        for r in rows:
            r["recorded_at"] = r["recorded_at"].isoformat()
            r["quality_score"] = float(r["quality_score"])
            r["completeness"] = float(r["completeness"])
        return {"success": True, "history": rows, "count": len(rows)}
    except Exception as e:
        logger.error(f"Error fetching quality history: {e}")
        raise HTTPException(status_code=500, detail=str(e))
