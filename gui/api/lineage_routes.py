"""
Data Lineage API Routes
Endpoints for tracking and retrieving data lineage information
Now persists to PostgreSQL for comprehensive reporting
"""

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel
from typing import List, Dict, Optional
import logging
from datetime import datetime
import json
import uuid

logger = logging.getLogger(__name__)

router = APIRouter()

# Try to initialize PostgreSQL connection
try:
    from database import DatabaseManager
    db_manager = DatabaseManager()
    db_manager.initialize()
    DB_AVAILABLE = True
    logger.info("✓ Lineage routes connected to PostgreSQL")
except Exception as e:
    logger.warning(f"⚠ PostgreSQL not available for lineage - using in-memory fallback: {e}")
    db_manager = None
    DB_AVAILABLE = False

# PostgreSQL-backed lineage storage with in-memory fallback
class LineageStore:
    def __init__(self):
        self.db = db_manager
        # In-memory fallback if PostgreSQL unavailable
        self.fallback_transformations = []
        self.fallback_table_lineage = {}
        logger.info(f"LineageStore initialized (PostgreSQL: {DB_AVAILABLE})")
    
    def record_transformation(self, entry: Dict):
        """Record a transformation in PostgreSQL data_lineage table (or fallback to memory)"""
        
        # If PostgreSQL not available, use in-memory fallback
        if not DB_AVAILABLE or not self.db:
            logger.warning("PostgreSQL unavailable - storing lineage in memory (will be lost on restart)")
            return self._record_in_memory(entry)
        
        try:
            source_table = entry['source']['location']
            target_table = entry['target']['location']
            source_layer = entry['source']['layer']
            target_layer = entry['target']['layer']
            transformation = entry['transformation']
            metadata = entry.get('metadata', {})
            
            # Extract domain from metadata or use 'default'
            domain = metadata.get('domain', 'finance')
            batch_id = metadata.get('job_id') or metadata.get('batch_id')
            
            # Insert into data_lineage table
            query = """
                INSERT INTO data_lineage (
                    source_layer, source_table, source_column,
                    target_layer, target_table, target_column,
                    transformation_type, transformation_logic,
                    domain, batch_id, created_at
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING lineage_id
            """
            
            # Store transformation logic as JSON
            transformation_logic = json.dumps({
                'type': transformation,
                'metrics': entry['metrics'],
                'timestamp': entry['timestamp']
            })
            
            with self.db.get_cursor() as cursor:
                cursor.execute(query, (
                    source_layer,
                    source_table,
                    None,  # source_column (table-level lineage for now)
                    target_layer,
                    target_table,
                    None,  # target_column (table-level lineage for now)
                    transformation,
                    transformation_logic,
                    domain,
                    batch_id,
                    datetime.utcnow()
                ))
                lineage_id = cursor.fetchone()[0]
            
            logger.info(f"📊 Lineage persisted to Postgres: {source_table} → {target_table} (ID: {lineage_id})")
            
            # Also log to audit_log for comprehensive tracking
            self._log_audit(
                event_type='lineage_recorded',
                event_category='data_processing',
                resource_type='transformation',
                resource_name=f"{source_table} → {target_table}",
                details={
                    'source_layer': source_layer,
                    'target_layer': target_layer,
                    'transformation': transformation,
                    'lineage_id': str(lineage_id)
                }
            )
            
            return lineage_id
            
        except Exception as e:
            logger.error(f"Failed to record lineage in Postgres: {e}")
            # Fallback to in-memory storage
            logger.warning("Falling back to in-memory lineage storage")
            return self._record_in_memory(entry)
    
    def _record_in_memory(self, entry: Dict):
        """In-memory fallback when PostgreSQL is unavailable"""
        self.fallback_transformations.append(entry)
        
        source_table = entry['source']['location']
        target_table = entry['target']['location']
        
        if target_table not in self.fallback_table_lineage:
            self.fallback_table_lineage[target_table] = {
                'upstream': [],
                'downstream': [],
                'transformations': []
            }
        
        if source_table not in self.fallback_table_lineage[target_table]['upstream']:
            self.fallback_table_lineage[target_table]['upstream'].append(source_table)
        
        self.fallback_table_lineage[target_table]['transformations'].append({
            'type': entry['transformation'],
            'timestamp': entry['timestamp'],
            'row_count': entry['metrics']['row_count'],
            'columns_used': entry['metrics'].get('columns_used', [])
        })
        
        if source_table not in self.fallback_table_lineage:
            self.fallback_table_lineage[source_table] = {
                'upstream': [],
                'downstream': [],
                'transformations': []
            }
        
        if target_table not in self.fallback_table_lineage[source_table]['downstream']:
            self.fallback_table_lineage[source_table]['downstream'].append(target_table)
        
        logger.info(f"📊 Lineage stored in memory: {source_table} → {target_table}")
        return None
    
    def _log_audit(self, event_type: str, event_category: str, 
                   resource_type: str, resource_name: str, details: Dict):
        """Log audit entry"""
        try:
            query = """
                INSERT INTO audit_log (
                    event_type, event_category,
                    user_id, user_role,
                    resource_type, resource_name,
                    action, action_details, status,
                    event_timestamp
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            
            with self.db.get_cursor() as cursor:
                cursor.execute(query, (
                    event_type,
                    event_category,
                    'system',  # System-generated lineage
                    'lakehouse_pipeline',
                    resource_type,
                    resource_name,
                    'create',  # Action: 'create' lineage record
                    json.dumps(details),
                    'success',
                    datetime.utcnow()
                ))
        except Exception as e:
            logger.warning(f"Failed to log audit entry: {e}")
            # Don't fail the main operation if audit logging fails
    
    def get_table_lineage(self, table_name: str) -> Dict:
        """Get lineage for a specific table from PostgreSQL (or fallback to memory)"""
        
        # Use in-memory fallback if PostgreSQL unavailable
        if not DB_AVAILABLE or not self.db:
            return self.fallback_table_lineage.get(table_name, {
                'upstream': [],
                'downstream': [],
                'transformations': []
            })
        
        try:
            # Get upstream sources
            upstream_query = """
                SELECT DISTINCT source_table, source_layer, transformation_type, transformation_logic
                FROM data_lineage
                WHERE target_table = %s
                ORDER BY created_at DESC
            """
            
            # Get downstream dependencies
            downstream_query = """
                SELECT DISTINCT target_table, target_layer, transformation_type, transformation_logic
                FROM data_lineage
                WHERE source_table = %s
                ORDER BY created_at DESC
            """
            
            upstream_results = self.db.execute_query_dict(upstream_query, (table_name,))
            downstream_results = self.db.execute_query_dict(downstream_query, (table_name,))
            
            # Parse transformation data
            transformations = []
            for row in upstream_results:
                try:
                    logic = json.loads(row['transformation_logic']) if row['transformation_logic'] else {}
                    transformations.append({
                        'type': row['transformation_type'],
                        'timestamp': logic.get('timestamp'),
                        'row_count': logic.get('metrics', {}).get('row_count', 0),
                        'columns_used': logic.get('metrics', {}).get('columns_used', [])
                    })
                except:
                    pass
            
            return {
                'upstream': [r['source_table'] for r in upstream_results],
                'downstream': [r['target_table'] for r in downstream_results],
                'transformations': transformations
            }
            
        except Exception as e:
            logger.error(f"Failed to get table lineage from Postgres: {e}")
            return {'upstream': [], 'downstream': [], 'transformations': []}
    
    def get_all_tables(self) -> List[str]:
        """Get all tables in lineage graph from PostgreSQL (or fallback to memory)"""
        
        # Use in-memory fallback if PostgreSQL unavailable
        if not DB_AVAILABLE or not self.db:
            return list(self.fallback_table_lineage.keys())
        
        try:
            query = """
                SELECT DISTINCT table_name FROM (
                    SELECT source_table AS table_name FROM data_lineage
                    UNION
                    SELECT target_table AS table_name FROM data_lineage
                ) AS all_tables
                ORDER BY table_name
            """
            results = self.db.execute_query_dict(query)
            return [r['table_name'] for r in results]
        except Exception as e:
            logger.error(f"Failed to get all tables from Postgres: {e}")
            # Fallback to in-memory
            return list(self.fallback_table_lineage.keys())
    
    def get_full_graph(self) -> Dict:
        """Get complete lineage graph from PostgreSQL (or fallback to memory)"""
        
        # Use in-memory fallback if PostgreSQL unavailable
        if not DB_AVAILABLE or not self.db:
            return self._get_graph_from_memory()
        
        try:
            # Get all lineage relationships
            query = """
                SELECT 
                    source_layer, source_table,
                    target_layer, target_table,
                    transformation_type,
                    COUNT(*) as transformation_count
                FROM data_lineage
                WHERE is_valid = TRUE
                GROUP BY source_layer, source_table, target_layer, target_table, transformation_type
                ORDER BY target_table
            """
            
            results = self.db.execute_query_dict(query)
            
            # Build nodes (unique tables)
            tables_set = set()
            table_info = {}
            
            for row in results:
                source = row['source_table']
                target = row['target_table']
                tables_set.add(source)
                tables_set.add(target)
                
                # Store layer info
                if source not in table_info:
                    table_info[source] = {'layer': row['source_layer'], 'transform_count': 0}
                if target not in table_info:
                    table_info[target] = {'layer': row['target_layer'], 'transform_count': 0}
                
                table_info[target]['transform_count'] += row['transformation_count']
            
            # Build nodes list
            nodes = [
                {
                    'id': table,
                    'name': table.split('.')[-1] if '.' in table else table,
                    'layer': table_info[table]['layer'],
                    'full_name': table,
                    'transformation_count': table_info[table]['transform_count']
                }
                for table in sorted(tables_set)
            ]
            
            # Build edges list
            edges = [
                {
                    'from': row['source_table'],
                    'to': row['target_table'],
                    'transformations': [row['transformation_type']]
                }
                for row in results
            ]
            
            return {
                'nodes': nodes,
                'edges': edges
            }
            
        except Exception as e:
            logger.error(f"Failed to get full graph from Postgres: {e}")
            # Fallback to in-memory
            return self._get_graph_from_memory()
    
    def _get_graph_from_memory(self) -> Dict:
        """Build lineage graph from in-memory fallback data"""
        nodes = []
        edges = []
        
        for table, lineage in self.fallback_table_lineage.items():
            # Determine layer from table name
            if 'bronze' in table.lower():
                layer = 'bronze'
            elif 'silver' in table.lower():
                layer = 'silver'
            elif 'gold' in table.lower():
                layer = 'gold'
            else:
                layer = 'source'
            
            nodes.append({
                'id': table,
                'name': table.split('.')[-1] if '.' in table else table,
                'layer': layer,
                'full_name': table,
                'transformation_count': len(lineage.get('transformations', []))
            })
            
            # Add edges for upstream connections
            for upstream_table in lineage.get('upstream', []):
                edges.append({
                    'from': upstream_table,
                    'to': table,
                    'transformations': [t['type'] for t in lineage.get('transformations', [])[:3]]
                })
        
        return {
            'nodes': nodes,
            'edges': edges
        }

# Global lineage store
lineage_store = LineageStore()


# Request/Response Models
class TransformationRecord(BaseModel):
    source_layer: str
    source_location: str
    target_layer: str
    target_location: str
    transformation: str
    row_count: int
    columns_used: Optional[List[str]] = []
    metadata: Optional[Dict] = {}


@router.post("/lineage/record")
async def record_transformation(record: TransformationRecord):
    """Record a data transformation in lineage"""
    try:
        entry = {
            "timestamp": datetime.utcnow().isoformat(),
            "source": {
                "layer": record.source_layer,
                "location": record.source_location
            },
            "target": {
                "layer": record.target_layer,
                "location": record.target_location
            },
            "transformation": record.transformation,
            "metrics": {
                "row_count": record.row_count,
                "columns_used": record.columns_used
            },
            "metadata": record.metadata
        }
        
        lineage_store.record_transformation(entry)
        
        logger.info(f"📊 Lineage recorded: {record.source_location} → {record.target_location}")
        
        return {
            "success": True,
            "message": "Lineage recorded successfully"
        }
    
    except Exception as e:
        logger.error(f"Failed to record lineage: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/lineage/table/{table_name:path}")
async def get_table_lineage(table_name: str):
    """Get lineage for a specific table"""
    try:
        lineage = lineage_store.get_table_lineage(table_name)
        
        return {
            "success": True,
            "table": table_name,
            "lineage": lineage
        }
    
    except Exception as e:
        logger.error(f"Failed to get table lineage: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/lineage/upstream/{table_name:path}")
async def get_upstream(table_name: str, depth: int = Query(default=3, ge=1, le=10)):
    """Get upstream sources for a table"""
    try:
        def get_upstream_recursive(table: str, current_depth: int, visited: set) -> List[Dict]:
            if current_depth > depth or table in visited:
                return []
            
            visited.add(table)
            lineage = lineage_store.get_table_lineage(table)
            upstream = []
            
            for source in lineage.get('upstream', []):
                upstream.append({
                    'table': source,
                    'depth': current_depth,
                    'transformations': [t['type'] for t in lineage.get('transformations', [])]
                })
                
                # Recursively get upstream
                upstream.extend(get_upstream_recursive(source, current_depth + 1, visited))
            
            return upstream
        
        upstream = get_upstream_recursive(table_name, 1, set())
        
        return {
            "success": True,
            "table": table_name,
            "upstream": upstream,
            "depth": depth
        }
    
    except Exception as e:
        logger.error(f"Failed to get upstream lineage: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/lineage/downstream/{table_name:path}")
async def get_downstream(table_name: str, depth: int = Query(default=3, ge=1, le=10)):
    """Get downstream dependencies for a table"""
    try:
        def get_downstream_recursive(table: str, current_depth: int, visited: set) -> List[Dict]:
            if current_depth > depth or table in visited:
                return []
            
            visited.add(table)
            lineage = lineage_store.get_table_lineage(table)
            downstream = []
            
            for target in lineage.get('downstream', []):
                target_lineage = lineage_store.get_table_lineage(target)
                downstream.append({
                    'table': target,
                    'depth': current_depth,
                    'transformations': [t['type'] for t in target_lineage.get('transformations', [])]
                })
                
                # Recursively get downstream
                downstream.extend(get_downstream_recursive(target, current_depth + 1, visited))
            
            return downstream
        
        downstream = get_downstream_recursive(table_name, 1, set())
        
        return {
            "success": True,
            "table": table_name,
            "downstream": downstream,
            "depth": depth
        }
    
    except Exception as e:
        logger.error(f"Failed to get downstream lineage: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/lineage/graph")
async def get_lineage_graph():
    """Get complete lineage graph"""
    try:
        graph = lineage_store.get_full_graph()
        
        return {
            "success": True,
            "graph": graph,
            "summary": {
                "total_tables": len(graph['nodes']),
                "total_connections": len(graph['edges']),
                "by_layer": {
                    layer: len([n for n in graph['nodes'] if n['layer'] == layer])
                    for layer in ['source', 'bronze', 'silver', 'gold']
                }
            }
        }
    
    except Exception as e:
        logger.error(f"Failed to get lineage graph: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/lineage/tables")
async def list_tables():
    """List all tables in lineage graph"""
    try:
        tables = lineage_store.get_all_tables()
        
        return {
            "success": True,
            "tables": tables,
            "count": len(tables)
        }
    
    except Exception as e:
        logger.error(f"Failed to list tables: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/lineage/impact/{table_name:path}")
async def get_impact_analysis(table_name: str):
    """Get impact analysis for a table (upstream + downstream)"""
    try:
        lineage = lineage_store.get_table_lineage(table_name)
        
        # Get upstream sources
        def get_all_upstream(table: str, visited: set) -> List[str]:
            if table in visited:
                return []
            visited.add(table)
            
            table_lineage = lineage_store.get_table_lineage(table)
            upstream = list(table_lineage.get('upstream', []))
            
            for source in table_lineage.get('upstream', []):
                upstream.extend(get_all_upstream(source, visited))
            
            return upstream
        
        # Get downstream dependencies
        def get_all_downstream(table: str, visited: set) -> List[str]:
            if table in visited:
                return []
            visited.add(table)
            
            table_lineage = lineage_store.get_table_lineage(table)
            downstream = list(table_lineage.get('downstream', []))
            
            for target in table_lineage.get('downstream', []):
                downstream.extend(get_all_downstream(target, visited))
            
            return downstream
        
        upstream = list(set(get_all_upstream(table_name, set())))
        downstream = list(set(get_all_downstream(table_name, set())))
        
        return {
            "success": True,
            "table": table_name,
            "impact": {
                "upstream_sources": upstream,
                "downstream_dependencies": downstream,
                "total_upstream": len(upstream),
                "total_downstream": len(downstream),
                "risk_level": "high" if len(downstream) > 5 else "medium" if len(downstream) > 0 else "low"
            }
        }
    
    except Exception as e:
        logger.error(f"Failed to get impact analysis: {e}")
        raise HTTPException(status_code=500, detail=str(e))
