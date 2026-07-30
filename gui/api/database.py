"""
Database connection and management utilities for PostgreSQL metadata store
"""
import psycopg2
from psycopg2 import pool, sql, extras
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from contextlib import contextmanager
from typing import Optional, List, Dict, Any, Tuple
import logging
from datetime import datetime
import json
import os
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables
env_path = Path(__file__).parent.parent.parent / ".env"
load_dotenv(env_path)

# Import config from the same directory
from app_config import PostgreSQLConfig

# Get config
postgres_config = PostgreSQLConfig.from_env()

logger = logging.getLogger(__name__)


class DatabaseManager:
    """PostgreSQL connection pool and query manager"""
    
    def __init__(self):
        self.connection_pool: Optional[pool.SimpleConnectionPool] = None
        self._initialized = False
    
    def initialize(self, min_conn: int = 1, max_conn: int = 10):
        """Initialize connection pool"""
        if self._initialized:
            logger.warning("Database already initialized")
            return
        
        try:
            self.connection_pool = pool.SimpleConnectionPool(
                min_conn,
                max_conn,
                host=postgres_config.host,
                port=postgres_config.port,
                database=postgres_config.database,
                user=postgres_config.user,
                password=postgres_config.password
            )
            self._initialized = True
            logger.info(f"Database connection pool initialized (min={min_conn}, max={max_conn})")
        except psycopg2.OperationalError as e:
            logger.error(f"Failed to initialize database connection pool: {e}")
            raise
    
    @contextmanager
    def get_connection(self):
        """Get connection from pool with context manager"""
        if not self._initialized:
            self.initialize()
        
        conn = self.connection_pool.getconn()
        try:
            yield conn
        finally:
            self.connection_pool.putconn(conn)
    
    @contextmanager
    def get_cursor(self, cursor_factory=None):
        """Get cursor with automatic connection management"""
        with self.get_connection() as conn:
            cursor = conn.cursor(cursor_factory=cursor_factory)
            try:
                yield cursor
                conn.commit()
            except Exception as e:
                conn.rollback()
                logger.error(f"Database operation failed: {e}")
                raise
            finally:
                cursor.close()
    
    def execute_query(self, query: str, params: Optional[tuple] = None, fetch: bool = True) -> Optional[List[tuple]]:
        """Execute a query and optionally fetch results"""
        with self.get_cursor() as cursor:
            cursor.execute(query, params)
            if fetch:
                return cursor.fetchall()
            return None
    
    def execute_query_dict(self, query: str, params: Optional[tuple] = None) -> List[Dict[str, Any]]:
        """Execute query and return results as list of dictionaries"""
        with self.get_cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
            cursor.execute(query, params)
            return [dict(row) for row in cursor.fetchall()]
    
    def execute_many(self, query: str, params_list: List[tuple]) -> None:
        """Execute same query with multiple parameter sets"""
        with self.get_cursor() as cursor:
            cursor.executemany(query, params_list)
    
    def close(self):
        """Close all connections in pool"""
        if self.connection_pool:
            self.connection_pool.closeall()
            self._initialized = False
            logger.info("Database connection pool closed")


class RulesRepository:
    """Repository for rules_catalog operations"""
    
    def __init__(self, db: DatabaseManager):
        self.db = db
    
    def get_rules_by_domain(self, domain: str, status: str = 'active') -> List[Dict[str, Any]]:
        """Fetch all rules for a specific domain"""
        query = """
            SELECT * FROM rules_catalog 
            WHERE domain = %s AND status = %s
            ORDER BY execution_order, created_at
        """
        return self.db.execute_query_dict(query, (domain, status))
    
    def get_rule_by_id(self, rule_id: str) -> Optional[Dict[str, Any]]:
        """Fetch a single rule by ID"""
        query = "SELECT * FROM rules_catalog WHERE rule_id = %s"
        results = self.db.execute_query_dict(query, (rule_id,))
        return results[0] if results else None
    
    def create_rule(self, rule_data: Dict[str, Any]) -> str:
        """Create a new rule"""
        query = """
            INSERT INTO rules_catalog (
                rule_name, domain, category, rule_type, description,
                target_table, target_columns, condition_expression,
                severity, action, execution_priority, created_by
            ) VALUES (
                %(rule_name)s, %(domain)s, %(category)s, %(rule_type)s, %(description)s,
                %(target_table)s, %(target_columns)s, %(condition_expression)s,
                %(severity)s, %(action)s, %(execution_priority)s, %(created_by)s
            ) RETURNING rule_id
        """
        with self.db.get_cursor() as cursor:
            cursor.execute(query, rule_data)
            return cursor.fetchone()[0]
    
    def update_rule(self, rule_id: str, updates: Dict[str, Any]) -> bool:
        """Update an existing rule"""
        # Build dynamic UPDATE query
        set_clauses = [f"{key} = %s" for key in updates.keys()]
        set_clause = ", ".join(set_clauses)
        values = list(updates.values()) + [rule_id]
        
        query = f"UPDATE rules_catalog SET {set_clause}, updated_at = CURRENT_TIMESTAMP WHERE rule_id = %s"
        self.db.execute_query(query, tuple(values), fetch=False)
        return True
    
    def delete_rule(self, rule_id: str) -> bool:
        """Soft delete a rule by setting status to 'archived'"""
        query = "UPDATE rules_catalog SET status = 'archived', updated_at = CURRENT_TIMESTAMP WHERE rule_id = %s"
        self.db.execute_query(query, (rule_id,), fetch=False)
        return True
    
    def get_rules_by_table(self, domain: str, table_name: str) -> List[Dict[str, Any]]:
        """Get all rules for a specific table (both active and inactive for management UI)"""
        query = """
            SELECT * FROM rules_catalog 
            WHERE domain = %s AND target_table = %s
            ORDER BY execution_priority, created_at DESC
        """
        return self.db.execute_query_dict(query, (domain, table_name))


class QuarantineRepository:
    """Repository for quarantine_records operations"""
    
    def __init__(self, db: DatabaseManager):
        self.db = db
    
    def create_quarantine_record(self, record_data: Dict[str, Any]) -> str:
        """Create a new quarantine record"""
        query = """
            INSERT INTO quarantine_records (
                rule_id, domain, source_table, failed_row_data,
                failure_reason, severity, quarantine_metadata
            ) VALUES (
                %(rule_id)s, %(domain)s, %(source_table)s, %(failed_row_data)s,
                %(failure_reason)s, %(severity)s, %(quarantine_metadata)s
            ) RETURNING quarantine_id
        """
        with self.db.get_cursor() as cursor:
            cursor.execute(query, record_data)
            return cursor.fetchone()[0]
    
    def get_quarantine_records(self, domain: str, status: str = 'pending', limit: int = 100) -> List[Dict[str, Any]]:
        """Fetch quarantine records for a domain"""
        query = """
            SELECT q.*, r.rule_name, r.category 
            FROM quarantine_records q
            JOIN rules_catalog r ON q.rule_id = r.rule_id
            WHERE q.domain = %s AND q.status = %s
            ORDER BY q.quarantined_at DESC
            LIMIT %s
        """
        return self.db.execute_query_dict(query, (domain, status, limit))
    
    def resolve_quarantine(self, quarantine_id: str, resolution: str, resolved_by: str) -> bool:
        """Mark quarantine record as resolved"""
        query = """
            UPDATE quarantine_records 
            SET status = 'resolved', resolution = %s, resolved_by = %s, 
                resolved_at = CURRENT_TIMESTAMP
            WHERE quarantine_id = %s
        """
        self.db.execute_query(query, (resolution, resolved_by, quarantine_id), fetch=False)
        return True
    
    def get_quarantine_summary(self, domain: str) -> Dict[str, Any]:
        """Get quarantine summary statistics"""
        query = """
            SELECT 
                COUNT(*) as total_records,
                COUNT(*) FILTER (WHERE status = 'pending') as pending_count,
                COUNT(*) FILTER (WHERE status = 'resolved') as resolved_count,
                COUNT(*) FILTER (WHERE severity = 'critical') as critical_count,
                COUNT(*) FILTER (WHERE severity = 'high') as high_count
            FROM quarantine_records
            WHERE domain = %s
        """
        results = self.db.execute_query_dict(query, (domain,))
        return results[0] if results else {}


class ExecutionLogRepository:
    """Repository for rule_execution_log operations"""
    
    def __init__(self, db: DatabaseManager):
        self.db = db
    
    def log_execution(self, log_data: Dict[str, Any]) -> str:
        """Create execution log entry"""
        query = """
            INSERT INTO rule_execution_log (
                rule_id, domain, table_name, execution_status,
                total_rows_processed, rows_passed, rows_failed,
                processing_time_ms, error_message, rule_version, rule_name
            ) VALUES (
                %(rule_id)s, %(domain)s, %(table_name)s, %(execution_status)s,
                %(total_rows_processed)s, %(rows_passed)s, %(rows_failed)s,
                %(processing_time_ms)s, %(error_message)s, 1, 
                (SELECT rule_name FROM rules_catalog WHERE rule_id = %(rule_id)s)
            ) RETURNING execution_id
        """
        with self.db.get_cursor() as cursor:
            cursor.execute(query, log_data)
            return cursor.fetchone()[0]
    
    def get_execution_history(self, rule_id: str, limit: int = 50) -> List[Dict[str, Any]]:
        """Get execution history for a rule"""
        query = """
            SELECT * FROM rule_execution_log 
            WHERE rule_id = %s 
            ORDER BY executed_at DESC 
            LIMIT %s
        """
        return self.db.execute_query_dict(query, (rule_id, limit))
    
    def get_execution_history_by_table(self, domain: str, table_name: str, limit: int = 50) -> List[Dict[str, Any]]:
        """Get execution history for a table (all rules combined)"""
        query = """
            SELECT l.*, r.rule_name 
            FROM rule_execution_log l
            LEFT JOIN rules_catalog r ON l.rule_id = r.rule_id
            WHERE l.domain = %s AND l.table_name = %s 
            ORDER BY l.execution_timestamp DESC 
            LIMIT %s
        """
        return self.db.execute_query_dict(query, (domain, table_name, limit))
    
    def get_failed_executions(self, domain: str, hours: int = 24) -> List[Dict[str, Any]]:
        """Get failed executions in the last N hours"""
        query = """
            SELECT l.*, r.rule_name 
            FROM rule_execution_log l
            JOIN rules_catalog r ON l.rule_id = r.rule_id
            WHERE l.domain = %s 
                AND l.execution_status = 'failed'
                AND l.executed_at > NOW() - INTERVAL '%s hours'
            ORDER BY l.executed_at DESC
        """
        return self.db.execute_query_dict(query, (domain, hours))


# Global database instance
db_manager = DatabaseManager()
rules_repo = RulesRepository(db_manager)
quarantine_repo = QuarantineRepository(db_manager)
execution_log_repo = ExecutionLogRepository(db_manager)


def initialize_database():
    """Initialize database connection on application startup"""
    try:
        db_manager.initialize()
        logger.info("Database initialized successfully")
        return True
    except Exception as e:
        logger.error(f"Database initialization failed: {e}")
        return False


if __name__ == "__main__":
    # Test database connection
    logging.basicConfig(level=logging.INFO)
    if initialize_database():
        print("✓ Database connection successful")
        
        # Test query
        with db_manager.get_cursor() as cursor:
            cursor.execute("SELECT version();")
            version = cursor.fetchone()
            print(f"PostgreSQL version: {version[0]}")
