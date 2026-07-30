"""
SQLite Job Tracking for Data Lakehouse Operations
Tracks ingestion, processing, and EDA jobs
"""

import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional
import json
import logging

logger = logging.getLogger(__name__)

class JobTracker:
    def __init__(self, db_path: str = "jobs.db"):
        """Initialize job tracker with SQLite database"""
        self.db_path = Path(db_path)
        self._init_database()
    
    def _init_database(self):
        """Create tables if they don't exist"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Jobs table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS jobs (
                job_id TEXT PRIMARY KEY,
                job_type TEXT NOT NULL,
                source_type TEXT,
                source TEXT,
                entity TEXT,
                layer TEXT,
                status TEXT NOT NULL,
                rows_processed INTEGER DEFAULT 0,
                total_rows INTEGER DEFAULT 0,
                chunks_written INTEGER DEFAULT 0,
                minio_location TEXT,
                error_message TEXT,
                started_at TIMESTAMP,
                completed_at TIMESTAMP,
                duration_seconds REAL,
                metadata TEXT
            )
        """)
        
        # Ingestion logs table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS ingestion_logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                job_id TEXT,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                level TEXT,
                message TEXT,
                FOREIGN KEY (job_id) REFERENCES jobs(job_id)
            )
        """)
        
        conn.commit()
        conn.close()
        logger.info(f"Job tracking database initialized at {self.db_path}")
    
    def create_job(self, job_id: str, job_type: str, source_type: str = None, 
                   source: str = None, entity: str = None, layer: str = None,
                   metadata: Dict = None) -> str:
        """Create a new job entry"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute("""
            INSERT INTO jobs (job_id, job_type, source_type, source, entity, layer, 
                            status, started_at, metadata)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            job_id,
            job_type,
            source_type,
            source,
            entity,
            layer,
            'pending',
            datetime.now().isoformat(),
            json.dumps(metadata) if metadata else None
        ))
        
        conn.commit()
        conn.close()
        
        logger.info(f"Created job {job_id}: {job_type} - {source}.{entity}")
        return job_id
    
    def update_job_status(self, job_id: str, status: str, 
                         rows_processed: int = None, total_rows: int = None,
                         chunks_written: int = None, minio_location: str = None,
                         error_message: str = None):
        """Update job status and progress"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        updates = ["status = ?"]
        params = [status]
        
        if rows_processed is not None:
            updates.append("rows_processed = ?")
            params.append(rows_processed)
        
        if total_rows is not None:
            updates.append("total_rows = ?")
            params.append(total_rows)
        
        if chunks_written is not None:
            updates.append("chunks_written = ?")
            params.append(chunks_written)
        
        if minio_location is not None:
            updates.append("minio_location = ?")
            params.append(minio_location)
        
        if error_message is not None:
            updates.append("error_message = ?")
            params.append(error_message)
        
        if status in ['completed', 'failed', 'cancelled']:
            updates.append("completed_at = ?")
            params.append(datetime.now().isoformat())
            
            # Calculate duration
            cursor.execute("SELECT started_at FROM jobs WHERE job_id = ?", (job_id,))
            result = cursor.fetchone()
            if result:
                started_at = datetime.fromisoformat(result[0])
                duration = (datetime.now() - started_at).total_seconds()
                updates.append("duration_seconds = ?")
                params.append(duration)
        
        params.append(job_id)
        
        query = f"UPDATE jobs SET {', '.join(updates)} WHERE job_id = ?"
        cursor.execute(query, params)
        
        conn.commit()
        conn.close()
        
        logger.info(f"Updated job {job_id}: status={status}, rows={rows_processed}")
    
    def get_job(self, job_id: str) -> Optional[Dict]:
        """Get job details"""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        
        cursor.execute("SELECT * FROM jobs WHERE job_id = ?", (job_id,))
        row = cursor.fetchone()
        
        conn.close()
        
        if row:
            job = dict(row)
            if job.get('metadata'):
                job['metadata'] = json.loads(job['metadata'])
            return job
        return None
    
    def list_jobs(self, status: str = None, job_type: str = None, 
                  limit: int = 50) -> List[Dict]:
        """List jobs with optional filters"""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        
        query = "SELECT * FROM jobs WHERE 1=1"
        params = []
        
        if status:
            query += " AND status = ?"
            params.append(status)
        
        if job_type:
            query += " AND job_type = ?"
            params.append(job_type)
        
        query += " ORDER BY started_at DESC LIMIT ?"
        params.append(limit)
        
        cursor.execute(query, params)
        rows = cursor.fetchall()
        
        conn.close()
        
        jobs = []
        for row in rows:
            job = dict(row)
            if job.get('metadata'):
                job['metadata'] = json.loads(job['metadata'])
            jobs.append(job)
        
        return jobs
    
    def add_log(self, job_id: str, level: str, message: str):
        """Add a log entry for a job"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute("""
            INSERT INTO ingestion_logs (job_id, level, message)
            VALUES (?, ?, ?)
        """, (job_id, level, message))
        
        conn.commit()
        conn.close()
    
    def get_logs(self, job_id: str) -> List[Dict]:
        """Get logs for a job"""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT * FROM ingestion_logs 
            WHERE job_id = ? 
            ORDER BY timestamp DESC
        """, (job_id,))
        
        rows = cursor.fetchall()
        conn.close()
        
        return [dict(row) for row in rows]
    
    def get_statistics(self) -> Dict:
        """Get job statistics"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        stats = {}
        
        # Total jobs by status
        cursor.execute("""
            SELECT status, COUNT(*) as count 
            FROM jobs 
            GROUP BY status
        """)
        stats['by_status'] = {row[0]: row[1] for row in cursor.fetchall()}
        
        # Total jobs by type
        cursor.execute("""
            SELECT job_type, COUNT(*) as count 
            FROM jobs 
            GROUP BY job_type
        """)
        stats['by_type'] = {row[0]: row[1] for row in cursor.fetchall()}
        
        # Total rows processed
        cursor.execute("SELECT SUM(rows_processed) FROM jobs WHERE status='completed'")
        stats['total_rows_processed'] = cursor.fetchone()[0] or 0
        
        # Average duration
        cursor.execute("SELECT AVG(duration_seconds) FROM jobs WHERE status='completed'")
        stats['avg_duration_seconds'] = cursor.fetchone()[0] or 0
        
        conn.close()
        
        return stats
