"""
Test Lineage Recording
Quick utility to test Reports functionality by inserting sample lineage data
"""

from fastapi import APIRouter, HTTPException
from typing import Dict
import logging
from datetime import datetime
import uuid

logger = logging.getLogger(__name__)

router = APIRouter()

# Import lineage store
try:
    from lineage_routes import lineage_store
    LINEAGE_AVAILABLE = True
except ImportError:
    LINEAGE_AVAILABLE = False
    logger.warning("LineageStore not available")


@router.post("/test/lineage/record-sample")
async def record_sample_lineage():
    """Record sample lineage data for testing Reports"""
    if not LINEAGE_AVAILABLE:
        raise HTTPException(status_code=503, detail="Lineage system not available")
    
    try:
        # Record Bronze -> Silver transformation (using correct format)
        entry1 = {
            "timestamp": datetime.utcnow().isoformat(),
            "source": {
                "layer": "bronze",
                "location": "finance_transactions_raw"
            },
            "target": {
                "layer": "silver",
                "location": "finance_transactions"
            },
            "transformation": "cleaning_validation",
            "metrics": {
                "row_count": 10000,
                "columns_used": ["transaction_id", "amount", "timestamp", "customer_id"]
            },
            "metadata": {
                "domain": "finance",
                "job_id": str(uuid.uuid4()),
                "quality_score": 98.5,
                "execution_time_seconds": 45
            }
        }
        lineage_store.record_transformation(entry1)
        
        # Record Silver -> Gold transformation
        entry2 = {
            "timestamp": datetime.utcnow().isoformat(),
            "source": {
                "layer": "silver",
                "location": "finance_transactions"
            },
            "target": {
                "layer": "gold",
                "location": "finance_transactions_aggregated"
            },
            "transformation": "aggregation",
            "metrics": {
                "row_count": 9850,
                "columns_used": ["customer_id", "amount", "timestamp"]
            },
            "metadata": {
                "domain": "finance",
                "job_id": str(uuid.uuid4()),
                "quality_score": 99.2,
                "execution_time_seconds": 12
            }
        }
        lineage_store.record_transformation(entry2)
        
        logger.info("✓ Sample lineage data recorded successfully")
        
        return {
            "success": True,
            "message": "Sample lineage data recorded successfully",
            "transformations_created": 2,
            "tables": ["finance_transactions_raw", "finance_transactions", "finance_transactions_aggregated"],
            "next_steps": "Refresh the Reports page and check pgAdmin: SELECT * FROM data_lineage ORDER BY created_at DESC;"
        }
        
    except Exception as e:
        logger.error(f"Failed to record sample lineage: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/test/lineage/record-transformation")
async def record_custom_transformation(
    source_layer: str,
    source_table: str,
    target_layer: str,
    target_table: str,
    transformation_type: str = "transformation",
    domain: str = "finance"
):
    """Record custom lineage transformation"""
    if not LINEAGE_AVAILABLE:
        raise HTTPException(status_code=503, detail="Lineage system not available")
    
    try:
        lineage_store.record_transformation(
            source_layer=source_layer,
            source_table=source_table,
            target_layer=target_layer,
            target_table=target_table,
            transformation_type=transformation_type,
            transformation_logic={
                "type": transformation_type,
                "timestamp": datetime.utcnow().isoformat(),
                "metadata": {"manual_entry": True}
            },
            domain=domain,
            batch_id=str(uuid.uuid4())
        )
        
        return {
            "success": True,
            "message": "Transformation lineage recorded",
            "source": f"{source_layer}.{source_table}",
            "target": f"{target_layer}.{target_table}"
        }
        
    except Exception as e:
        logger.error(f"Failed to record lineage: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/test/lineage/verify")
async def verify_lineage_system():
    """Verify lineage system is working"""
    if not LINEAGE_AVAILABLE:
        return {"success": False, "message": "LineageStore not imported"}
    
    try:
        # Check database connection
        from database import DatabaseManager
        db = DatabaseManager()
        
        # Try to query data_lineage table
        query = "SELECT COUNT(*) as count FROM data_lineage"
        result = db.execute_query(query, fetch=True)
        lineage_count = result[0][0] if result else 0
        
        # Try to query audit_log table
        query2 = "SELECT COUNT(*) as count FROM audit_log"
        result2 = db.execute_query(query2, fetch=True)
        audit_count = result2[0][0] if result2 else 0
        
        # Get all tables from lineage store
        tables = lineage_store.get_all_tables()
        
        # Get lineage graph
        graph = lineage_store.get_full_graph()
        
        return {
            "success": True,
            "lineage_available": True,
            "database_connected": True,
            "data_lineage_rows": lineage_count,
            "audit_log_rows": audit_count,
            "total_tables_in_graph": len(tables) if tables else 0,
            "graph_nodes": len(graph.get("nodes", [])),
            "graph_edges": len(graph.get("edges", [])),
            "message": "Lineage system operational" if lineage_count > 0 else "Database connected but no lineage data yet"
        }
        
    except Exception as e:
        logger.error(f"Lineage verification failed: {e}", exc_info=True)
        return {
            "success": False,
            "error": str(e),
            "message": "Lineage system check failed"
        }
