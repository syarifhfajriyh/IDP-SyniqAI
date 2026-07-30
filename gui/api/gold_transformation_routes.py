"""
Gold Transformation API Routes
REST endpoints for Gold layer transformations (Silver ? Gold)
Supports aggregations, joins, rollups, and dimensional modeling
"""

from fastapi import APIRouter, HTTPException, BackgroundTasks
from pydantic import BaseModel
from typing import Dict, List, Optional, Any
from datetime import datetime
import logging

from silver_job_tracker import job_tracker
from lineage_routes import lineage_store
from gold_service import get_gold_service

logger = logging.getLogger(__name__)

router = APIRouter()

# -- Request/Response Models --------------------------------------------------

class AggregationRequest(BaseModel):
    """Request for aggregation transformation"""
    silver_table: str  # e.g., "syniqai_silver.finance.finance_transactions"
    gold_table: str  # e.g., "syniqai_gold.finance.product_performance"
    group_by: List[str]  # ["PRODUCT_ID", "PRODUCT_NAME", "CATEGORY"]
    aggregations: List[Dict[str, Any]]  # [{"column": "AMOUNT", "function": "sum", "alias": "total_revenue"}]
    filters: Optional[List[Dict[str, Any]]] = None
    description: Optional[str] = None


class JoinRequest(BaseModel):
    """Request for multi-table join"""
    tables: List[Dict[str, str]]  # [{"table": "silver.customers", "alias": "c"}]
    join_conditions: List[Dict[str, Any]]  # [{"left": "c.customer_id", "right": "o.customer_id", "type": "INNER"}]
    select_columns: List[str]  # ["c.customer_id", "o.order_date", "o.amount"]
    gold_table: str
    description: Optional[str] = None


class CustomerSegmentationRequest(BaseModel):
    """Request for customer segmentation (RFM analysis)"""
    silver_table: str  # Transaction table with USER_ID, TRANSACTION_DATE, AMOUNT
    gold_table: str  # Target customer segment table
    description: Optional[str] = None


# -- API Endpoints -------------------------------------------------------------

@router.post("/gold/transform/aggregation")
async def execute_aggregation(request: AggregationRequest):
    """
    Execute aggregation transformation: Silver ? Gold
    
    Example:
    ```
    POST /api/gold/transform/aggregation
    {
        "silver_table": "syniqai_silver.finance.finance_transactions",
        "gold_table": "syniqai_gold.finance.product_performance", 
        "group_by": ["PRODUCT_ID", "PRODUCT_NAME", "CATEGORY"],
        "aggregations": [
            {"column": "AMOUNT", "function": "sum", "alias": "total_revenue"},
            {"column": "*", "function": "count", "alias": "transaction_count"},
            {"column": "AMOUNT", "function": "avg", "alias": "avg_price"},
            {"column": "USER_ID", "function": "count_distinct", "alias": "unique_buyers"}
        ]
    }
    ```
    """
    # Create tracking job
    job_id = job_tracker.create_job(
        table_name=request.gold_table,
        source=request.silver_table.split('.')[0] if '.' in request.silver_table else 'silver',
        entity=request.gold_table.split('.')[-1] if '.' in request.gold_table else request.gold_table
    )
    
    try:
        logger.info(f"?? Starting Gold aggregation (Job: {job_id})")
        job_tracker.update_status(job_id, 'running', message='Initializing Gold transformation', progress=10)
        
        # Get Gold service (singleton with persistent Spark session)
        try:
            service = get_gold_service()
            job_tracker.update_status(job_id, 'running', message='Executing aggregation', progress=30)
        except Exception as e:
            logger.error(f"Failed to get Gold service: {e}")
            raise HTTPException(status_code=500, detail=f"Spark initialization failed: {str(e)}")
        
        # Execute transformation
        result = service.execute_aggregation(
            silver_table=request.silver_table,
            gold_table=request.gold_table,
            group_by_cols=request.group_by,
            aggregations=request.aggregations,
            filters=request.filters
        )
        
        job_tracker.update_status(job_id, 'running', message='Recording lineage', progress=90)
        
        # Record lineage to PostgreSQL
        try:
            lineage_entry = {
                "timestamp": datetime.utcnow().isoformat(),
                "source": {
                    "layer": "silver",
                    "location": request.silver_table
                },
                "target": {
                    "layer": "gold",
                    "location": request.gold_table
                },
                "transformation": "aggregation",
                "metrics": {
                    "row_count": result.get('output_rows', 0),
                    "input_rows": result.get('input_rows', 0),
                    "aggregations_count": len(request.aggregations)
                },
                "metadata": {
                    "job_id": job_id,
                    "domain": request.gold_table.split('.')[1] if '.' in request.gold_table else 'default',
                    "group_by_columns": request.group_by,
                    "description": request.description or "Aggregation transformation"
                }
            }
            
            lineage_store.record_transformation(lineage_entry)
            logger.info(f"?? Lineage recorded: {request.silver_table} ? {request.gold_table}")
        except Exception as e:
            logger.warning(f"Failed to record lineage: {e}")
        
        # Mark job as completed
        job_tracker.update_results(
            job_id=job_id,
            status='completed',
            row_count=result.get('output_rows', 0),
            message=f"Aggregation complete: {result.get('input_rows', 0)} ? {result.get('output_rows', 0)} rows"
        )
        
        logger.info(f"? Gold aggregation completed (Job: {job_id})")
        
        return {
            "success": True,
            "job_id": job_id,
            "result": result,
            "message": f"? Gold table created: {result.get('output_rows', 0)} rows"
        }
        
    except Exception as e:
        logger.error(f"? Gold aggregation failed: {e}", exc_info=True)
        
        job_tracker.update_results(
            job_id=job_id,
            status='failed',
            error_message=str(e)
        )
        
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/gold/transform/join")
async def execute_join(request: JoinRequest):
    """
    Execute multi-table join transformation
    
    Example:
    ```
    POST /api/gold/transform/join
    {
        "tables": [
            {"table": "syniqai_silver.finance.finance_transactions", "alias": "txn"},
            {"table": "syniqai_gold.finance.product_performance", "alias": "prod"}
        ],
        "join_conditions": [
            {"left": "txn.PRODUCT_ID", "right": "prod.PRODUCT_ID", "type": "INNER"}
        ],
        "select_columns": [
            "txn.TRANSACTION_ID",
            "txn.TRANSACTION_DATE",
            "prod.PRODUCT_NAME",
            "txn.AMOUNT",
            "prod.total_revenue"
        ],
        "gold_table": "syniqai_gold.finance.transaction_insights"
    }
    ```
    """
    # Create tracking job
    job_id = job_tracker.create_job(
        table_name=request.gold_table,
        source=request.tables[0]["table"].split('.')[0] if '.' in request.tables[0]["table"] else 'silver',
        entity=request.gold_table.split('.')[-1] if '.' in request.gold_table else request.gold_table
    )
    
    try:
        logger.info(f"?? Starting Gold join (Job: {job_id})")
        job_tracker.update_status(job_id, 'running', message='Initializing join transformation', progress=10)
        
        # Get Gold service (singleton with persistent Spark session)
        try:
            service = get_gold_service()
            job_tracker.update_status(job_id, 'running', message='Executing join', progress=30)
        except Exception as e:
            logger.error(f"Failed to get Gold service: {e}")
            raise HTTPException(status_code=500, detail=f"Spark initialization failed: {str(e)}")
        
        # Execute transformation
        result = service.execute_join(
            tables=request.tables,
            join_conditions=request.join_conditions,
            select_columns=request.select_columns,
            gold_table=request.gold_table
        )
        
        job_tracker.update_status(job_id, 'running', message='Recording lineage', progress=90)
        
        # Record lineage to PostgreSQL
        try:
            lineage_entry = {
                "timestamp": datetime.utcnow().isoformat(),
                "source": {
                    "layer": "silver",
                    "location": ", ".join([t["table"] for t in request.tables])
                },
                "target": {
                    "layer": "gold",
                    "location": request.gold_table
                },
                "transformation": "join",
                "metrics": {
                    "row_count": result.get('output_rows', 0),
                    "tables_joined": len(request.tables)
                },
                "metadata": {
                    "job_id": job_id,
                    "domain": request.gold_table.split('.')[1] if '.' in request.gold_table else 'default',
                    "join_conditions_count": len(request.join_conditions),
                    "description": request.description or "Join transformation"
                }
            }
            
            lineage_store.record_transformation(lineage_entry)
            logger.info(f"?? Lineage recorded for join transformation")
        except Exception as e:
            logger.warning(f"Failed to record lineage: {e}")
        
        # Mark job as completed
        job_tracker.update_results(
            job_id=job_id,
            status='completed',
            row_count=result.get('output_rows', 0),
            message=f"Join complete: {len(request.tables)} tables ? {result.get('output_rows', 0)} rows"
        )
        
        logger.info(f"? Gold join completed (Job: {job_id})")
        
        return {
            "success": True,
            "job_id": job_id,
            "result": result,
            "message": f"? Gold table created: {result.get('output_rows', 0)} rows"
        }
        
    except Exception as e:
        logger.error(f"? Gold join failed: {e}", exc_info=True)
        
        job_tracker.update_results(
            job_id=job_id,
            status='failed',
            error_message=str(e)
        )
        
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/gold/transform/customer-segmentation")
async def execute_customer_segmentation(request: CustomerSegmentationRequest):
    """
    Execute RFM (Recency, Frequency, Monetary) customer segmentation
    
    Creates customer segments: VIP, Premium, Regular, New
    Based on total_spent and purchase_frequency metrics
    """
    # Create tracking job
    job_id = job_tracker.create_job(
        table_name=request.gold_table,
        source=request.silver_table.split('.')[0] if '.' in request.silver_table else 'silver',
        entity=request.gold_table.split('.')[-1] if '.' in request.gold_table else request.gold_table
    )
    
    try:
        logger.info(f"?? Starting customer segmentation (Job: {job_id})")
        job_tracker.update_status(job_id, 'running', message='Initializing segmentation', progress=10)
        
        # Get Gold service (singleton with persistent Spark session)
        try:
            service = get_gold_service()
            job_tracker.update_status(job_id, 'running', message='Executing RFM analysis', progress=30)
        except Exception as e:
            logger.error(f"Failed to get Gold service: {e}")
            raise HTTPException(status_code=500, detail=f"Spark initialization failed: {str(e)}")
        
        # Execute transformation
        result = service.execute_customer_segmentation(
            silver_table=request.silver_table,
            gold_table=request.gold_table
        )
        
        job_tracker.update_status(job_id, 'running', message='Recording lineage', progress=90)
        
        # Record lineage to PostgreSQL
        try:
            lineage_entry = {
                "timestamp": datetime.utcnow().isoformat(),
                "source": {
                    "layer": "silver",
                    "location": request.silver_table
                },
                "target": {
                    "layer": "gold",
                    "location": request.gold_table
                },
                "transformation": "customer_segmentation",
                "metrics": {
                    "row_count": result.get('output_rows', 0),
                    "input_rows": result.get('input_rows', 0),
                    "segments": result.get('segments', [])
                },
                "metadata": {
                    "job_id": job_id,
                    "domain": request.gold_table.split('.')[1] if '.' in request.gold_table else 'default',
                    "description": request.description or "RFM customer segmentation"
                }
            }
            
            lineage_store.record_transformation(lineage_entry)
            logger.info(f"?? Lineage recorded for customer segmentation")
        except Exception as e:
            logger.warning(f"Failed to record lineage: {e}")
        
        # Mark job as completed
        job_tracker.update_results(
            job_id=job_id,
            status='completed',
            row_count=result.get('output_rows', 0),
            message=f"Segmentation complete: {result.get('output_rows', 0)} customers"
        )
        
        logger.info(f"? Customer segmentation completed (Job: {job_id})")
        
        return {
            "success": True,
            "job_id": job_id,
            "result": result,
            "message": f"? Customer segments created: {result.get('output_rows', 0)} customers"
        }
        
    except Exception as e:
        logger.error(f"? Customer segmentation failed: {e}", exc_info=True)
        
        job_tracker.update_results(
            job_id=job_id,
            status='failed',
            error_message=str(e)
        )
        
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/gold/tables")
async def list_gold_tables():
    """List all Gold tables with metadata"""
    try:
        # For now, return mock data - can be enhanced to query Iceberg catalog
        tables = [
            {
                "name": "product_performance",
                "full_name": "syniqai_gold.finance.product_performance",
                "row_count": 0,
                "quality_score": 95.0,
                "last_updated": datetime.utcnow().isoformat(),
                "transformation_type": "aggregation"
            },
            {
                "name": "customer_segments",
                "full_name": "syniqai_gold.finance.customer_segments",
                "row_count": 0,
                "quality_score": 97.0,
                "last_updated": datetime.utcnow().isoformat(),
                "transformation_type": "customer_segmentation"
            },
            {
                "name": "transaction_insights",
                "full_name": "syniqai_gold.finance.transaction_insights",
                "row_count": 0,
                "quality_score": 93.0,
                "last_updated": datetime.utcnow().isoformat(),
                "transformation_type": "join"
            }
        ]
        
        return {
            "success": True,
            "tables": tables,
            "total": len(tables)
        }
        
    except Exception as e:
        logger.error(f"Failed to list Gold tables: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/gold/silver-tables")
async def list_silver_tables_for_gold():
    """
    List all available Silver tables from MinIO/Iceberg for Gold transformation
    Includes table schema (columns) for building transformations
    """
    try:
        # Get Gold service (which has access to Silver catalog)
        service = get_gold_service()
        tables = service.get_silver_tables()
        
        return {
            "success": True,
            "tables": tables,
            "total": len(tables),
            "message": f"Found {len(tables)} Silver tables available for Gold transformation"
        }
    except Exception as e:
        logger.error(f"Failed to list Silver tables: {e}")
        # Return fallback tables
        tables = [
            {
                "name": "march_user_transactions",
                "full_name": "syniqai_silver.finance.march_user_transactions", 
                "domain": "finance",
                "row_count": 1064
            },
            {
                "name": "finance_transactions",
                "full_name": "syniqai_silver.finance.finance_transactions",
                "domain": "finance", 
                "row_count": 850
            }
        ]
        
        return {
            "success": True,
            "tables": tables,
            "total": len(tables),
            "message": "Using fallback table list",
            "warning": str(e)
        }
