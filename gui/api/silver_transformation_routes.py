"""
Silver Transformation API Routes
REST endpoints for Silver layer transformations
"""

from fastapi import APIRouter, HTTPException, BackgroundTasks
from pydantic import BaseModel
from typing import Dict, List, Optional, Any
from datetime import datetime
import logging
import uuid

from silver_transformation_service import get_silver_service

# Import lineage store for tracking transformations
try:
    from lineage_routes import lineage_store
    LINEAGE_AVAILABLE = True
except ImportError:
    LINEAGE_AVAILABLE = False
    logging.warning("LineageStore not available for silver transformations")

logger = logging.getLogger(__name__)

router = APIRouter()

# Request/Response models
class QualityRuleRequest(BaseModel):
    name: str
    type: str  # not_null, email, phone, range, length, regex, enum
    column: str
    params: Optional[Dict] = {}
    severity: str = "error"  # error, warning, info


class TransformationRequest(BaseModel):
    bronze_table: str
    silver_table: str
    quality_rules: List[Dict]
    normalization: Optional[Dict] = None
    type_mapping: Optional[Dict] = None
    dedup_columns: Optional[List[str]] = None
    partition_by: Optional[List[str]] = None


class CDCTransformationRequest(BaseModel):
    bronze_cdc_table: str
    silver_table: str
    primary_keys: List[str]
    quality_rules: List[Dict]
    normalization: Optional[Dict] = None


@router.get("/silver/bronze-tables")
async def list_bronze_tables():
    """List all available Bronze tables"""
    try:
        service = get_silver_service()
        tables = service.get_available_bronze_tables()
        
        return {
            "success": True,
            "tables": tables,
            "count": len(tables)
        }
    except Exception as e:
        logger.error(f"Failed to list Bronze tables: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/silver/table-schema/{table_name:path}")
async def get_table_schema(table_name: str):
    """Get schema of a table"""
    try:
        service = get_silver_service()
        schema = service.get_table_schema(table_name)
        
        if "error" in schema:
            raise HTTPException(status_code=404, detail=schema["error"])
        
        return {
            "success": True,
            "schema": schema
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get schema: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/silver/preview/{table_name:path}")
async def preview_table(table_name: str, limit: int = 10):
    """Preview data from a table"""
    try:
        service = get_silver_service()
        preview = service.preview_data(table_name, limit)
        
        if "error" in preview:
            raise HTTPException(status_code=404, detail=preview["error"])
        
        return {
            "success": True,
            "preview": preview
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to preview table: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/silver/validate-rules")
async def validate_quality_rules(request: Dict):
    """Validate quality rules without applying transformation"""
    try:
        service = get_silver_service()
        
        result = service.validate_quality_rules(
            table_name=request.get("table_name"),
            rules_config=request.get("quality_rules", [])
        )
        
        return result
    except Exception as e:
        logger.error(f"Failed to validate rules: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/silver/transform")
async def transform_table(request: TransformationRequest, 
                         background_tasks: BackgroundTasks):
    """Execute Silver transformation"""
    try:
        service = get_silver_service()
        
        transformation_config = {
            "quality_rules": request.quality_rules,
            "normalization": request.normalization,
            "type_mapping": request.type_mapping,
            "dedup_columns": request.dedup_columns,
            "partition_by": request.partition_by
        }
        
        result = service.transform_table(
            bronze_table=request.bronze_table,
            silver_table=request.silver_table,
            transformation_config=transformation_config
        )
        
        # Record lineage for transformation
        if LINEAGE_AVAILABLE and result.get('success'):
            try:
                lineage_entry = {
                    "timestamp": datetime.utcnow().isoformat(),
                    "source": {
                        "layer": "bronze",
                        "location": request.bronze_table
                    },
                    "target": {
                        "layer": "silver",
                        "location": request.silver_table
                    },
                    "transformation": "silver_transformation",
                    "metrics": {
                        "row_count": result.get('rows_processed', 0),
                        "columns_used": []
                    },
                    "metadata": {
                        "job_id": str(uuid.uuid4()),
                        "quality_rules_count": len(request.quality_rules),
                        "normalization_applied": bool(request.normalization),
                        "dedup_applied": bool(request.dedup_columns)
                    }
                }
                
                lineage_store.record_transformation(lineage_entry)
                logger.info(f"📊 Transformation lineage recorded: {request.bronze_table} → {request.silver_table}")
            except Exception as e:
                logger.warning(f"Failed to record transformation lineage: {e}")
        
        return result
    except Exception as e:
        logger.error(f"Transformation failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/silver/transform-cdc")
async def transform_cdc_table(request: CDCTransformationRequest):
    """Execute CDC transformation"""
    try:
        service = get_silver_service()
        
        transformation_config = {
            "quality_rules": request.quality_rules,
            "normalization": request.normalization
        }
        
        result = service.transform_cdc_table(
            bronze_cdc_table=request.bronze_cdc_table,
            silver_table=request.silver_table,
            primary_keys=request.primary_keys,
            transformation_config=transformation_config
        )
        
        return result
    except Exception as e:
        logger.error(f"CDC transformation failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/silver/templates")
async def get_transformation_templates():
    """Get pre-defined transformation templates"""
    try:
        service = get_silver_service()
        templates = service.get_transformation_templates()
        
        return {
            "success": True,
            "templates": templates
        }
    except Exception as e:
        logger.error(f"Failed to get templates: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/silver/apply-template")
async def apply_template(request: Dict):
    """Apply a pre-defined template"""
    try:
        service = get_silver_service()
        
        result = service.apply_template(
            template_name=request.get("template_name"),
            bronze_table=request.get("bronze_table"),
            silver_table=request.get("silver_table")
        )
        
        return result
    except Exception as e:
        logger.error(f"Failed to apply template: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/silver/tables")
async def list_silver_tables():
    """List all Silver tables"""
    try:
        service = get_silver_service()
        tables = service.get_silver_tables()
        
        return {
            "success": True,
            "tables": tables,
            "count": len(tables)
        }
    except Exception as e:
        logger.error(f"Failed to list Silver tables: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ========================================
# Pipeline-Based Transformation (New UI)
# ========================================

class PipelineTransformationRequest(BaseModel):
    source_table: str
    target_table: str
    transformations: List[Dict[str, Any]]
    output_config: Dict[str, Any]
    glue_config: Optional[Dict[str, Any]] = None


@router.post("/silver/execute-transformation")
async def execute_transformation_pipeline(request: PipelineTransformationRequest):
    """
    Execute transformation pipeline from the new UI
    Supports 50+ transformation operations across 11 categories
    Uses PySpark for distributed processing and full JOIN support
    """
    # Import job tracker and lineage store
    from silver_job_tracker import job_tracker
    from lineage_routes import lineage_store
    
    # Create tracking job
    job_id = job_tracker.create_job(
        table_name=request.target_table,
        source=request.source_table.split('.')[0] if '.' in request.source_table else 'bronze',
        entity=request.source_table.split('.')[-1] if '.' in request.source_table else request.source_table
    )
    
    try:
        from spark_transformation_executor import SparkTransformationExecutor
        
        logger.info(f"🚀 Executing Spark transformation: {request.source_table} → {request.target_table}")
        logger.info(f"📋 Pipeline steps: {len(request.transformations)}")
        logger.info(f"🆔 Job ID: {job_id}")
        
        # Update job to running
        job_tracker.update_status(job_id, 'running', message='Starting transformation pipeline', progress=5)
        
        # List transformation operations
        for idx, t in enumerate(request.transformations, 1):
            logger.info(f"   DEBUG: transform type = {type(t)}, content = {t}")
            status = "✅" if t.get('enabled', True) else "⏭️"
            logger.info(f"   {status} Step {idx}: {t.get('operation', 'Unknown')}")
        
        # Update progress
        job_tracker.update_status(job_id, 'running', message='Initializing Spark executor', progress=10)
        
        # Convert request to dict to ensure proper serialization
        logger.info("Converting request to dict...")
        transformations_list = [dict(t) if hasattr(t, 'dict') else t for t in request.transformations]
        output_config_dict = dict(request.output_config) if hasattr(request.output_config, 'dict') else request.output_config
        
        job_tracker.update_status(job_id, 'running', message='Executing transformations', progress=30)
        
        executor = SparkTransformationExecutor()
        
        result = executor.execute_pipeline(
            source_table=request.source_table,
            target_table=request.target_table,
            transformations=transformations_list,
            output_config=output_config_dict
        )
        
        # Update job with results
        job_tracker.update_status(job_id, 'running', message='Finalizing results', progress=90)
        
        # Extract metrics from result
        input_rows = result.get('input_rows', 0)
        output_rows = result.get('output_rows', 0)
        
        # Calculate quality score (simple metric based on row retention)
        quality_score = (output_rows / input_rows) if input_rows > 0 else 0.0
        
        # Build cleaning summary
        cleaning_summary = {
            'rows_quarantined': 0,
            'rows_cleaned': input_rows - output_rows if input_rows >= output_rows else 0,
            'rows_validated': output_rows
        }
        
        # Record lineage
        try:
            source_layer = 'bronze' if 'bronze' in request.source_table.lower() else 'source'
            target_layer = 'silver' if 'silver' in request.target_table.lower() else 'gold'
            
            transformation_desc = ', '.join([t.get('operation', 'unknown') for t in transformations_list[:5]])
            if len(transformations_list) > 5:
                transformation_desc += f' + {len(transformations_list) - 5} more'
            
            lineage_entry = {
                "timestamp": datetime.utcnow().isoformat(),
                "source": {
                    "layer": source_layer,
                    "location": request.source_table
                },
                "target": {
                    "layer": target_layer,
                    "location": request.target_table
                },
                "transformation": transformation_desc,
                "metrics": {
                    "row_count": output_rows,
                    "columns_used": []
                },
                "metadata": {
                    "job_id": job_id,
                    "transformation_count": len(transformations_list),
                    "quality_score": quality_score
                }
            }
            
            lineage_store.record_transformation(lineage_entry)
            logger.info(f"📊 Lineage recorded for {request.target_table}")
        except Exception as e:
            logger.warning(f"Failed to record lineage: {e}")
        
        # Mark job as completed
        job_tracker.update_results(
            job_id=job_id,
            status='completed',
            row_count=output_rows,
            quality_score=quality_score,
            cleaning_summary=cleaning_summary,
            message=f"Transformation complete: {input_rows} → {output_rows} rows"
        )
        
        logger.info(f"✅ Job {job_id} completed successfully")
        
        return {
            "success": True,
            "result": result,
            "job_id": job_id,
            "message": f"✅ Transformation complete: {result.get('input_rows', 0)} → {result.get('output_rows', 0)} rows"
        }
        
    except Exception as e:
        logger.error(f"❌ Pipeline execution failed: {e}", exc_info=True)
        logger.error(f"Exception type: {type(e).__name__}")
        logger.error(f"Exception args: {e.args}")
        import traceback
        logger.error(f"Full traceback:\n{traceback.format_exc()}")
        
        # Mark job as failed
        job_tracker.update_results(
            job_id=job_id,
            status='failed',
            error_message=str(e)
        )
        
        logger.error(f"❌ Job {job_id} failed")
        
        raise HTTPException(status_code=500, detail=str(e))
