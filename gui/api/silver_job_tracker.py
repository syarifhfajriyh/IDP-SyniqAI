"""
In-Memory Job Tracker for Silver Processing
With TTL-based cleanup for completed jobs
"""

from datetime import datetime, timedelta
from typing import Dict, Optional
import threading
import time
import logging

logger = logging.getLogger(__name__)

class SilverJobTracker:
    """
    In-memory job tracker with automatic cleanup.
    Completed jobs are removed after a TTL period.
    """
    
    def __init__(self, ttl_hours: int = 24):
        """
        Initialize the tracker.
        
        Args:
            ttl_hours: Time to live for completed jobs in hours (default: 24)
        """
        self.jobs: Dict[str, Dict] = {}
        self.ttl_hours = ttl_hours
        self.lock = threading.Lock()
        
        # Start cleanup thread
        self._start_cleanup_thread()
        logger.info(f"Initialized SilverJobTracker with TTL={ttl_hours}h")
    
    def create_job(self, table_name: str, source: str, 
                   entity: str, bronze_path: str = None) -> str:
        """
        Create a new silver processing job
        
        Returns:
            job_id: Generated job ID
        """
        import uuid
        job_id = f"job_{uuid.uuid4().hex[:12]}"
        
        with self.lock:
            job = {
                "job_id": job_id,
                "job_type": "silver_processing",
                "table_name": table_name,
                "source": source,
                "entity": entity,
                "bronze_path": bronze_path,
                "status": "pending",
                "progress": 0,
                "message": "Job queued",
                "started_at": datetime.now().isoformat(),
                "completed_at": None,
                "error_message": None,
                "silver_path": None,
                "row_count": None,
                "quality_score": None,
                "cleaning_summary": None
            }
            self.jobs[job_id] = job
            logger.info(f"Created job {job_id} for {table_name}")
            return job_id
    
    def update_status(self, job_id: str, status: str, message: str = None,
                     progress: int = None, error_message: str = None):
        """Update job status"""
        with self.lock:
            if job_id not in self.jobs:
                logger.warning(f"Job {job_id} not found")
                return
            
            job = self.jobs[job_id]
            job["status"] = status
            
            if message:
                job["message"] = message
            if progress is not None:
                job["progress"] = progress
            if error_message:
                job["error_message"] = error_message
            
            if status in ["completed", "failed"]:
                job["completed_at"] = datetime.now().isoformat()
            
            logger.info(f"Job {job_id}: {status} - {message}")
    
    def update_results(self, job_id: str, status: str = None,
                      silver_path: str = None, row_count: int = None, 
                      quality_score: float = None, cleaning_summary: Dict = None,
                      message: str = None, error_message: str = None):
        """Update job results after completion"""
        with self.lock:
            if job_id not in self.jobs:
                return
            
            job = self.jobs[job_id]
            
            if status:
                job["status"] = status
                if status in ["completed", "failed"]:
                    job["completed_at"] = datetime.now().isoformat()
            
            if message:
                job["message"] = message
            if error_message:
                job["error_message"] = error_message
            if silver_path:
                job["silver_path"] = silver_path
            if row_count is not None:
                job["row_count"] = row_count
            if quality_score is not None:
                job["quality_score"] = quality_score
            if cleaning_summary:
                job["cleaning_summary"] = cleaning_summary
    
    def get_job(self, job_id: str) -> Optional[Dict]:
        """Get job details"""
        with self.lock:
            return self.jobs.get(job_id)
    
    def list_jobs(self, limit: int = 50, status: str = None) -> list:
        """List recent jobs"""
        with self.lock:
            jobs = list(self.jobs.values())
            
            if status:
                jobs = [j for j in jobs if j["status"] == status]
            
            # Sort by started_at descending
            jobs.sort(key=lambda j: j["started_at"], reverse=True)
            
            return jobs[:limit]
    
    def _cleanup_old_jobs(self):
        """Remove jobs older than TTL"""
        with self.lock:
            now = datetime.now()
            to_remove = []
            
            for job_id, job in self.jobs.items():
                if job["status"] in ["completed", "failed"]:
                    completed_at = job.get("completed_at")
                    if completed_at:
                        completed_time = datetime.fromisoformat(completed_at)
                        age = now - completed_time
                        
                        if age > timedelta(hours=self.ttl_hours):
                            to_remove.append(job_id)
            
            for job_id in to_remove:
                del self.jobs[job_id]
                logger.info(f"Cleaned up old job: {job_id}")
            
            if to_remove:
                logger.info(f"Cleaned up {len(to_remove)} old jobs")
    
    def _start_cleanup_thread(self):
        """Start background thread for periodic cleanup"""
        def cleanup_loop():
            while True:
                time.sleep(3600)  # Run every hour
                try:
                    self._cleanup_old_jobs()
                except Exception as e:
                    logger.error(f"Error in cleanup thread: {e}")
        
        thread = threading.Thread(target=cleanup_loop, daemon=True)
        thread.start()
        logger.info("Started cleanup thread")


# Global job tracker instance
job_tracker = SilverJobTracker(ttl_hours=24)
