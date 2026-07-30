"""
Reports API Routes
Comprehensive reporting from PostgreSQL lineage and audit data
Provides end-to-end visibility: Bronze → Silver → Gold with CDC tracking
"""

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel
from typing import List, Dict, Optional, Any
import logging
from datetime import datetime, timedelta
import json

logger = logging.getLogger(__name__)

router = APIRouter()

# Try to initialize database connection
try:
    from database import DatabaseManager
    db = DatabaseManager()
    db.initialize()
    DB_AVAILABLE = True
    logger.info("✓ Reports routes connected to PostgreSQL")
except Exception as e:
    logger.warning(f"⚠ PostgreSQL not available for reports: {e}")
    db = None
    DB_AVAILABLE = False


def check_tables_exist() -> Dict[str, bool]:
    """Check if required PostgreSQL tables exist"""
    if not DB_AVAILABLE or not db:
        return {"data_lineage": False, "audit_log": False}
    
    try:
        query = """
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public' 
                AND table_name IN ('data_lineage', 'audit_log')
        """
        results = db.execute_query_dict(query)
        results = results if results is not None else []
        existing_tables = {r['table_name'] for r in results}
        
        return {
            "data_lineage": "data_lineage" in existing_tables,
            "audit_log": "audit_log" in existing_tables
        }
    except Exception as e:
        logger.error(f"Failed to check table existence: {e}")
        return {"data_lineage": False, "audit_log": False}


@router.get("/reports/lineage/summary")
async def get_lineage_summary():
    """Get high-level lineage summary across all layers"""
    if not DB_AVAILABLE:
        return {
            "success": False,
            "message": "PostgreSQL not available",
            "summary": {"total_transformations": 0, "total_tables": 0, "by_layer": []}
        }
    
    tables = check_tables_exist()
    if not tables["data_lineage"]:
        return {
            "success": True,
            "message": "data_lineage table not yet created. Run setup_postgres.sql to initialize.",
            "summary": {"total_transformations": 0, "total_tables": 0, "by_layer": []}
        }
    
    try:
        query = """
            SELECT 
                source_layer,
                target_layer,
                COUNT(*) as transformation_count,
                COUNT(DISTINCT source_table) as source_tables,
                COUNT(DISTINCT target_table) as target_tables,
                MIN(created_at) as first_transformation,
                MAX(created_at) as last_transformation
            FROM data_lineage
            WHERE is_valid = TRUE
            GROUP BY source_layer, target_layer
            ORDER BY source_layer, target_layer
        """
        
        results = db.execute_query_dict(query)
        results = results if results is not None else []
        
        # Calculate totals
        total_transformations = sum(r['transformation_count'] for r in results)
        total_tables = len(set(
            [r['source_tables'] for r in results] + 
            [r['target_tables'] for r in results]
        ))
        
        return {
            "success": True,
            "summary": {
                "total_transformations": total_transformations,
                "total_tables": total_tables,
                "by_layer": results
            }
        }
    
    except Exception as e:
        logger.error(f"Failed to get lineage summary: {e}")
        # Return empty data instead of 500 error
        return {
            "success": False,
            "error": str(e),
            "summary": {"total_transformations": 0, "total_tables": 0, "by_layer": []}
        }


@router.get("/reports/lineage/flow")
async def get_lineage_flow(
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    domain: Optional[str] = None
):
    """Get complete data flow from Bronze → Silver → Gold"""
    if not DB_AVAILABLE:
        return {"success": False, "flow": [], "count": 0}
    
    tables = check_tables_exist()
    if not tables["data_lineage"]:
        return {"success": True, "flow": [], "count": 0, "message": "data_lineage table not initialized"}
    
    try:
        # Build query with optional filters
        conditions = ["is_valid = TRUE"]
        params = []
        
        if start_date:
            conditions.append("created_at >= %s")
            params.append(start_date)
        
        if end_date:
            conditions.append("created_at <= %s")
            params.append(end_date)
        
        if domain:
            conditions.append("domain = %s")
            params.append(domain)
        
        where_clause = " AND ".join(conditions)
        
        query = f"""
            SELECT 
                lineage_id,
                source_layer, source_table,
                target_layer, target_table,
                transformation_type,
                transformation_logic,
                domain,
                batch_id,
                created_at
            FROM data_lineage
            WHERE {where_clause}
            ORDER BY created_at DESC
            LIMIT 1000
        """
        
        results = db.execute_query_dict(query, tuple(params) if params else None)
        
        # Handle None or empty results
        if results is None:
            results = []
        
        # Parse transformation logic
        for row in results:
            if row['transformation_logic']:
                try:
                    row['transformation_details'] = json.loads(row['transformation_logic'])
                except:
                    row['transformation_details'] = {}
        
        return {
            "success": True,
            "flow": results,
            "count": len(results),
            "filters": {
                "start_date": start_date,
                "end_date": end_date,
                "domain": domain
            }
        }
    
    except Exception as e:
        logger.error(f"Failed to get lineage flow: {e}")
        return {
            "success": False,
            "error": str(e),
            "flow": [],
            "count": 0
        }


@router.get("/reports/audit/events")
async def get_audit_events(
    event_category: Optional[str] = None,
    start_date: Optional[str] = None,
    limit: int = Query(default=100, ge=1, le=1000)
):
    """Get audit log events for comprehensive tracking"""
    if not DB_AVAILABLE:
        return {"success": False, "events": [], "count": 0}
    
    tables = check_tables_exist()
    if not tables["audit_log"]:
        return {"success": True, "events": [], "count": 0, "message": "audit_log table not initialized"}
    
    try:
        conditions = []
        params = []
        
        if event_category:
            conditions.append("event_category = %s")
            params.append(event_category)
        
        if start_date:
            conditions.append("event_timestamp >= %s")
            params.append(start_date)
        
        where_clause = "WHERE " + " AND ".join(conditions) if conditions else ""
        
        query = f"""
            SELECT 
                audit_id,
                event_type, event_category,
                user_id, user_role,
                resource_type, resource_name,
                action, action_details,
                status, error_message,
                event_timestamp
            FROM audit_log
            {where_clause}
            ORDER BY event_timestamp DESC
            LIMIT %s
        """
        
        params.append(limit)
        results = db.execute_query_dict(query, tuple(params))
        
        # Handle None or empty results
        if results is None:
            results = []
        
        # Parse action_details JSON
        for row in results:
            if row['action_details']:
                try:
                    row['action_details'] = json.loads(row['action_details'])
                except:
                    pass
        
        return {
            "success": True,
            "events": results,
            "count": len(results)
        }
    
    except Exception as e:
        logger.error(f"Failed to get audit events: {e}")
        return {
            "success": False,
            "error": str(e),
            "events": [],
            "count": 0
        }


@router.get("/reports/pipeline/history")
async def get_pipeline_history(
    days: int = Query(default=7, ge=1, le=90)
):
    """Get complete pipeline execution history for reporting"""
    if not DB_AVAILABLE:
        return {"success": False, "history": {"lineage_by_day": [], "audit_by_day": []}}
    
    tables = check_tables_exist()
    if not tables["data_lineage"] or not tables["audit_log"]:
        return {"success": True, "history": {"lineage_by_day": [], "audit_by_day": []}, "message": "Tables not initialized"}
    
    try:
        start_date = datetime.utcnow() - timedelta(days=days)
        
        # Get lineage transformations
        lineage_query = """
            SELECT 
                DATE(created_at) as date,
                source_layer, target_layer,
                COUNT(*) as transformation_count,
                COUNT(DISTINCT batch_id) as batch_count
            FROM data_lineage
            WHERE created_at >= %s AND is_valid = TRUE
            GROUP BY DATE(created_at), source_layer, target_layer
            ORDER BY date DESC
        """
        
        lineage_results = db.execute_query_dict(lineage_query, (start_date,))
        lineage_results = lineage_results if lineage_results is not None else []
        
        # Get audit events by category
        audit_query = """
            SELECT 
                DATE(event_timestamp) as date,
                event_category,
                COUNT(*) as event_count,
                COUNT(CASE WHEN status = 'success' THEN 1 END) as success_count,
                COUNT(CASE WHEN status = 'failure' THEN 1 END) as failure_count
            FROM audit_log
            WHERE event_timestamp >= %s
            GROUP BY DATE(event_timestamp), event_category
            ORDER BY date DESC
        """
        
        audit_results = db.execute_query_dict(audit_query, (start_date,))
        audit_results = audit_results if audit_results is not None else []
        
        return {
            "success": True,
            "history": {
                "lineage_by_day": lineage_results,
                "audit_by_day": audit_results,
                "period_days": days,
                "start_date": start_date.isoformat()
            }
        }
    
    except Exception as e:
        logger.error(f"Failed to get pipeline history: {e}")
        return {
            "success": False,
            "error": str(e),
            "history": {"lineage_by_day": [], "audit_by_day": []}
        }


@router.get("/reports/data-quality/trends")
async def get_quality_trends(
    days: int = Query(default=30, ge=1, le=90)
):
    """Get data quality trends over time from lineage metadata"""
    if not DB_AVAILABLE:
        return {"success": False, "trends": [], "count": 0}
    
    tables = check_tables_exist()
    if not tables["data_lineage"]:
        return {"success": True, "trends": [], "count": 0, "message": "data_lineage table not initialized"}
    
    try:
        start_date = datetime.utcnow() - timedelta(days=days)
        
        # Extract quality scores from transformation_logic metadata
        query = """
            SELECT 
                target_table,
                target_layer,
                transformation_logic,
                created_at
            FROM data_lineage
            WHERE created_at >= %s 
                AND is_valid = TRUE
                AND transformation_logic IS NOT NULL
            ORDER BY target_table, created_at
        """
        
        results = db.execute_query_dict(query, (start_date,))
        
        # Parse quality scores from metadata
        quality_trends = []
        for row in results:
            try:
                logic = json.loads(row['transformation_logic'])
                metadata = logic.get('metadata', {})
                if 'quality_score' in metadata:
                    quality_trends.append({
                        'table': row['target_table'],
                        'layer': row['target_layer'],
                        'quality_score': metadata['quality_score'],
                        'timestamp': row['created_at'].isoformat()
                    })
            except:
                pass
        
        return {
            "success": True,
            "trends": quality_trends,
            "count": len(quality_trends),
            "period_days": days
        }
    
    except Exception as e:
        logger.error(f"Failed to get quality trends: {e}")
        return {
            "success": False,
            "error": str(e),
            "trends": [],
            "count": 0
        }


@router.get("/reports/cdc/changes")
async def get_cdc_changes(
    table: Optional[str] = None,
    days: int = Query(default=7, ge=1, le=90)
):
    """Get CDC change events from audit log and lineage"""
    if not DB_AVAILABLE:
        return {"success": False, "cdc_changes": [], "count": 0}
    
    tables = check_tables_exist()
    if not tables["audit_log"]:
        return {"success": True, "cdc_changes": [], "count": 0, "message": "audit_log table not initialized"}
    
    try:
        start_date = datetime.utcnow() - timedelta(days=days)
        
        # Query audit log for CDC events
        conditions = [
            "event_category = 'data_processing'",
            "event_type LIKE '%cdc%'",
            "event_timestamp >= %s"
        ]
        params = [start_date]
        
        if table:
            conditions.append("resource_name LIKE %s")
            params.append(f"%{table}%")
        
        where_clause = " AND ".join(conditions)
        
        query = f"""
            SELECT 
                audit_id,
                event_type,
                resource_name,
                action,
                action_details,
                status,
                event_timestamp
            FROM audit_log
            WHERE {where_clause}
            ORDER BY event_timestamp DESC
            LIMIT 500
        """
        
        results = db.execute_query_dict(query, tuple(params))
        results = results if results is not None else []
        
        # Parse action_details
        for row in results:
            if row['action_details']:
                try:
                    row['details'] = json.loads(row['action_details'])
                except:
                    row['details'] = {}
        
        return {
            "success": True,
            "cdc_changes": results,
            "count": len(results),
            "table_filter": table,
            "period_days": days
        }
    
    except Exception as e:
        logger.error(f"Failed to get CDC changes: {e}")
        return {
            "success": False,
            "error": str(e),
            "cdc_changes": [],
            "count": 0
        }


@router.get("/reports/comprehensive")
async def get_comprehensive_report(
    domain: Optional[str] = None,
    days: int = Query(default=7, ge=1, le=90)
):
    """
    Get comprehensive report combining lineage, audit, and quality data
    This is the main reporting endpoint that aggregates everything
    """
    if not DB_AVAILABLE:
        return {"success": False, "report": {"lineage": {}, "audit": {}, "tables": {}}}
    
    tables = check_tables_exist()
    if not tables["data_lineage"] or not tables["audit_log"]:
        return {"success": True, "report": {"lineage": {}, "audit": {}, "tables": {}}, "message": "Tables not initialized"}
    
    try:
        start_date = datetime.utcnow() - timedelta(days=days)
        
        # 1. Lineage summary
        lineage_summary_query = """
            SELECT 
                source_layer, target_layer,
                COUNT(*) as transformation_count,
                COUNT(DISTINCT source_table) as unique_sources,
                COUNT(DISTINCT target_table) as unique_targets
            FROM data_lineage
            WHERE created_at >= %s AND is_valid = TRUE
            GROUP BY source_layer, target_layer
        """
        lineage_summary = db.execute_query_dict(lineage_summary_query, (start_date,))
        lineage_summary = lineage_summary if lineage_summary is not None else []
        
        # 2. Audit events breakdown
        audit_summary_query = """
            SELECT 
                event_category,
                COUNT(*) as total_events,
                COUNT(CASE WHEN status = 'success' THEN 1 END) as success_count,
                COUNT(CASE WHEN status = 'failure' THEN 1 END) as failure_count
            FROM audit_log
            WHERE event_timestamp >= %s
            GROUP BY event_category
        """
        audit_summary = db.execute_query_dict(audit_summary_query, (start_date,))
        audit_summary = audit_summary if audit_summary is not None else []
        
        # 3. Recent transformations
        recent_transformations_query = """
            SELECT 
                source_table, target_table,
                transformation_type,
                created_at
            FROM data_lineage
            WHERE created_at >= %s AND is_valid = TRUE
            ORDER BY created_at DESC
            LIMIT 50
        """
        recent_transformations = db.execute_query_dict(recent_transformations_query, (start_date,))
        recent_transformations = recent_transformations if recent_transformations is not None else []
        
        # 4. Tables by layer
        tables_by_layer_query = """
            SELECT 
                layer,
                COUNT(DISTINCT table_name) as table_count
            FROM (
                SELECT source_layer as layer, source_table as table_name FROM data_lineage
                UNION
                SELECT target_layer as layer, target_table as table_name FROM data_lineage
            ) AS all_tables
            GROUP BY layer
        """
        tables_by_layer = db.execute_query_dict(tables_by_layer_query)
        tables_by_layer = tables_by_layer if tables_by_layer is not None else []
        
        return {
            "success": True,
            "report": {
                "period": {
                    "days": days,
                    "start_date": start_date.isoformat(),
                    "end_date": datetime.utcnow().isoformat()
                },
                "lineage": {
                    "summary": lineage_summary,
                    "recent_transformations": recent_transformations
                },
                "audit": {
                    "summary": audit_summary
                },
                "tables": {
                    "by_layer": tables_by_layer
                }
            },
            "generated_at": datetime.utcnow().isoformat()
        }
    
    except Exception as e:
        logger.error(f"Failed to generate comprehensive report: {e}")
        return {
            "success": False,
            "error": str(e),
            "report": {"lineage": {}, "audit": {}, "tables": {}}
        }


@router.get("/reports/table/{table_name:path}")
async def get_table_report(table_name: str):
    """Get detailed report for a specific table including lineage and audit trail"""
    if not DB_AVAILABLE:
        return {"success": False, "table": table_name, "report": {"lineage": [], "audit_trail": []}}
    
    tables = check_tables_exist()
    if not tables["data_lineage"] or not tables["audit_log"]:
        return {"success": True, "table": table_name, "report": {"lineage": [], "audit_trail": []}, "message": "Tables not initialized"}
    
    try:
        # 1. Get lineage for this table
        lineage_query = """
            SELECT 
                lineage_id,
                source_layer, source_table,
                target_layer, target_table,
                transformation_type,
                transformation_logic,
                batch_id,
                created_at
            FROM data_lineage
            WHERE source_table = %s OR target_table = %s
            ORDER BY created_at DESC
            LIMIT 100
        """
        
        lineage_results = db.execute_query_dict(lineage_query, (table_name, table_name))
        lineage_results = lineage_results if lineage_results is not None else []
        
        # 2. Get audit events for this table
        audit_query = """
            SELECT 
                audit_id,
                event_type, event_category,
                action, action_details,
                status, error_message,
                event_timestamp
            FROM audit_log
            WHERE resource_name LIKE %s
            ORDER BY event_timestamp DESC
            LIMIT 100
        """
        
        audit_results = db.execute_query_dict(audit_query, (f"%{table_name}%",))
        audit_results = audit_results if audit_results is not None else []
        
        # Parse JSON fields
        for row in lineage_results:
            if row['transformation_logic']:
                try:
                    row['transformation_details'] = json.loads(row['transformation_logic'])
                except:
                    row['transformation_details'] = {}
        
        for row in audit_results:
            if row['action_details']:
                try:
                    row['details'] = json.loads(row['action_details'])
                except:
                    row['details'] = {}
        
        return {
            "success": True,
            "table": table_name,
            "report": {
                "lineage": lineage_results,
                "audit_trail": audit_results,
                "upstream_count": len([r for r in lineage_results if r['target_table'] == table_name]),
                "downstream_count": len([r for r in lineage_results if r['source_table'] == table_name])
            }
        }
    
    except Exception as e:
        logger.error(f"Failed to generate table report: {e}")
        return {
            "success": False,
            "error": str(e),
            "table": table_name,
            "report": {"lineage": [], "audit_trail": [], "upstream_count": 0, "downstream_count": 0}
        }


@router.get("/reports/transformations/recent")
async def get_recent_transformations(
    layer: Optional[str] = None,
    limit: int = Query(default=50, ge=1, le=500)
):
    """Get recent transformations across all layers or specific layer"""
    if not DB_AVAILABLE:
        return {"success": False, "transformations": [], "count": 0}
    
    tables = check_tables_exist()
    if not tables["data_lineage"]:
        return {"success": True, "transformations": [], "count": 0, "message": "data_lineage table not initialized"}
    
    try:
        conditions = ["is_valid = TRUE"]
        params = []
        
        if layer:
            conditions.append("(source_layer = %s OR target_layer = %s)")
            params.extend([layer, layer])
        
        where_clause = " AND ".join(conditions)
        
        query = f"""
            SELECT 
                lineage_id,
                source_layer, source_table,
                target_layer, target_table,
                transformation_type,
                transformation_logic,
                domain,
                created_at
            FROM data_lineage
            WHERE {where_clause}
            ORDER BY created_at DESC
            LIMIT %s
        """
        
        params.append(limit)
        results = db.execute_query_dict(query, tuple(params))
        
        # Handle None or empty results
        if results is None:
            results = []
        
        # Parse transformation logic
        for row in results:
            if row['transformation_logic']:
                try:
                    logic = json.loads(row['transformation_logic'])
                    row['metrics'] = logic.get('metrics', {})
                except:
                    row['metrics'] = {}
        
        return {
            "success": True,
            "transformations": results,
            "count": len(results),
            "layer_filter": layer
        }
    
    except Exception as e:
        logger.error(f"Failed to get recent transformations: {e}")
        return {
            "success": False,
            "error": str(e),
            "transformations": [],
            "count": 0
        }


@router.get("/reports/quality/validation-history")
async def get_quality_validation_history(
    days: int = Query(default=30, ge=1, le=90)
):
    """Get quality validation history from audit log"""
    if not DB_AVAILABLE:
        return {"success": False, "validation_history": [], "count": 0}
    
    tables = check_tables_exist()
    if not tables["audit_log"]:
        return {"success": True, "validation_history": [], "count": 0, "message": "audit_log table not initialized"}
    
    try:
        start_date = datetime.utcnow() - timedelta(days=days)
        
        query = """
            SELECT 
                audit_id,
                event_type,
                resource_name,
                action_details,
                status,
                event_timestamp
            FROM audit_log
            WHERE event_category = 'data_processing'
                AND (event_type LIKE '%quality%' OR event_type LIKE '%validation%')
                AND event_timestamp >= %s
            ORDER BY event_timestamp DESC
            LIMIT 500
        """
        
        results = db.execute_query_dict(query, (start_date,))
        
        # Handle None or empty results
        if results is None:
            results = []
        
        # Parse quality scores from action_details
        for row in results:
            if row.get('action_details'):
                try:
                    details = json.loads(row['action_details']) if isinstance(row['action_details'], str) else row['action_details']
                    row['quality_score'] = details.get('quality_score')
                    row['validation_passed'] = details.get('validation_passed')
                except:
                    pass
        
        return {
            "success": True,
            "validation_history": results,
            "count": len(results),
            "period_days": days
        }
    
    except Exception as e:
        logger.error(f"Failed to get quality validation history: {e}")
        return {
            "success": False,
            "error": str(e),
            "validation_history": [],
            "count": 0
        }
