"""
Custom Connectors for MongoDB and S3
=====================================
Python-based connectors that publish change events to Kafka topics.

Unlike PostgreSQL/MariaDB which use Debezium, MongoDB and S3 use
custom Python runners that poll for changes and publish to Kafka.
"""

import os
import json
import time
import logging
from typing import Dict, List, Optional, Any
from datetime import datetime, timedelta
from dataclasses import dataclass, asdict
from threading import Thread, Event
import hashlib

from kafka import KafkaProducer
from pymongo import MongoClient
from pymongo.errors import PyMongoError
import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)


# Job tracking
_active_jobs: Dict[str, Dict[str, Any]] = {}


@dataclass
class MongoDBConfig:
    """MongoDB connection configuration"""
    connection_string: str
    database: str
    collection: str
    poll_interval_seconds: int = 30
    batch_size: int = 1000
    use_change_streams: bool = True  # Use change streams if available


@dataclass
class S3Config:
    """S3 connection configuration"""
    bucket: str
    prefix: str = ""
    region: str = "us-east-1"
    access_key_id: Optional[str] = None
    secret_access_key: Optional[str] = None
    poll_interval_seconds: int = 60
    file_pattern: Optional[str] = None  # Regex pattern for file filtering


def _create_kafka_producer(bootstrap_servers: str = "localhost:9092") -> KafkaProducer:
    """Create configured Kafka producer"""
    return KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        key_serializer=lambda k: k.encode('utf-8') if k else None,
        compression_type='gzip',
        acks='all',
        retries=3,
        max_in_flight_requests_per_connection=1
    )


class MongoDBConnector:
    """
    MongoDB CDC connector using Change Streams or periodic polling.
    """
    
    def __init__(
        self,
        config: MongoDBConfig,
        kafka_topic: str = "cdc.mongodb",
        kafka_bootstrap_servers: str = "localhost:9092"
    ):
        self.config = config
        self.kafka_topic = kafka_topic
        self.kafka_bootstrap_servers = kafka_bootstrap_servers
        self.producer = None
        self.mongo_client = None
        self.stop_event = Event()
        self.last_timestamp = None
        
        logger.info(f"MongoDB Connector initialized for {config.database}.{config.collection}")
    
    
    def start(self):
        """Start the MongoDB connector"""
        try:
            # Initialize Kafka producer
            self.producer = _create_kafka_producer(self.kafka_bootstrap_servers)
            
            # Initialize MongoDB client
            self.mongo_client = MongoClient(self.config.connection_string)
            
            # Test connection
            self.mongo_client.admin.command('ping')
            logger.info("MongoDB connection established")
            
            # Use change streams if available and configured
            if self.config.use_change_streams:
                self._run_change_stream()
            else:
                self._run_polling()
                
        except PyMongoError as e:
            logger.error(f"MongoDB error: {e}")
            raise
        except Exception as e:
            logger.error(f"Failed to start MongoDB connector: {e}")
            raise
    
    
    def _run_change_stream(self):
        """Run connector using MongoDB Change Streams (real-time)"""
        logger.info("Starting MongoDB Change Stream listener...")
        
        db = self.mongo_client[self.config.database]
        collection = db[self.config.collection]
        
        try:
            # Open change stream
            with collection.watch(full_document='updateLookup') as stream:
                for change in stream:
                    if self.stop_event.is_set():
                        break
                    
                    self._process_change_event(change)
                    
        except PyMongoError as e:
            logger.error(f"Change stream error: {e}")
            logger.info("Falling back to polling mode...")
            self._run_polling()
    
    
    def _run_polling(self):
        """Run connector using periodic polling (batch mode)"""
        logger.info("Starting MongoDB polling mode...")
        
        db = self.mongo_client[self.config.database]
        collection = db[self.config.collection]
        
        while not self.stop_event.is_set():
            try:
                # Query for documents updated since last poll
                query = {}
                if self.last_timestamp and 'updated_at' in collection.find_one() or {}:
                    query['updated_at'] = {'$gt': self.last_timestamp}
                
                # Fetch batch
                cursor = collection.find(query).limit(self.config.batch_size)
                documents = list(cursor)
                
                if documents:
                    logger.info(f"Found {len(documents)} new/updated documents")
                    
                    # Publish each document as a CDC event
                    for doc in documents:
                        self._publish_document(doc, operation='read')
                    
                    # Update timestamp
                    if 'updated_at' in documents[-1]:
                        self.last_timestamp = documents[-1]['updated_at']
                
                # Sleep until next poll
                self.stop_event.wait(self.config.poll_interval_seconds)
                
            except PyMongoError as e:
                logger.error(f"Polling error: {e}")
                self.stop_event.wait(10)  # Wait before retry
    
    
    def _process_change_event(self, change: Dict[str, Any]):
        """Process a change stream event"""
        operation_type = change.get('operationType')
        
        if operation_type in ['insert', 'update', 'replace']:
            document = change.get('fullDocument', {})
            self._publish_document(document, operation=operation_type)
        elif operation_type == 'delete':
            document_key = change.get('documentKey', {})
            self._publish_document(document_key, operation='delete')
    
    
    def _publish_document(self, document: Dict[str, Any], operation: str):
        """Publish document to Kafka topic"""
        try:
            # Convert ObjectId to string
            if '_id' in document:
                document['_id'] = str(document['_id'])
            
            # Build CDC message
            message = {
                'op': operation,  # insert, update, delete, read
                'source': {
                    'db': self.config.database,
                    'collection': self.config.collection,
                    'ts_ms': int(datetime.utcnow().timestamp() * 1000)
                },
                'after': document if operation != 'delete' else None,
                'before': None  # MongoDB change streams don't provide this
            }
            
            # Use _id as key for partitioning
            key = str(document.get('_id', ''))
            
            # Send to Kafka
            future = self.producer.send(self.kafka_topic, key=key, value=message)
            future.get(timeout=5)  # Block until sent
            
        except Exception as e:
            logger.error(f"Failed to publish document: {e}")
    
    
    def stop(self):
        """Stop the connector"""
        logger.info("Stopping MongoDB connector...")
        self.stop_event.set()
        
        if self.producer:
            self.producer.flush()
            self.producer.close()
        
        if self.mongo_client:
            self.mongo_client.close()


class S3Connector:
    """
    S3 CDC connector that monitors bucket for new/updated files.
    """
    
    def __init__(
        self,
        config: S3Config,
        kafka_topic: str = "cdc.s3",
        kafka_bootstrap_servers: str = "localhost:9092"
    ):
        self.config = config
        self.kafka_topic = kafka_topic
        self.kafka_bootstrap_servers = kafka_bootstrap_servers
        self.producer = None
        self.s3_client = None
        self.stop_event = Event()
        self.seen_files: Dict[str, str] = {}  # key -> etag mapping
        
        logger.info(f"S3 Connector initialized for s3://{config.bucket}/{config.prefix}")
    
    
    def start(self):
        """Start the S3 connector"""
        try:
            # Initialize Kafka producer
            self.producer = _create_kafka_producer(self.kafka_bootstrap_servers)
            
            # Initialize S3 client
            if self.config.access_key_id and self.config.secret_access_key:
                self.s3_client = boto3.client(
                    's3',
                    region_name=self.config.region,
                    aws_access_key_id=self.config.access_key_id,
                    aws_secret_access_key=self.config.secret_access_key
                )
            else:
                # Use default credentials
                self.s3_client = boto3.client('s3', region_name=self.config.region)
            
            # Test connection
            self.s3_client.head_bucket(Bucket=self.config.bucket)
            logger.info(f"S3 connection established to bucket: {self.config.bucket}")
            
            # Start polling
            self._run_polling()
            
        except ClientError as e:
            logger.error(f"S3 error: {e}")
            raise
        except Exception as e:
            logger.error(f"Failed to start S3 connector: {e}")
            raise
    
    
    def _run_polling(self):
        """Poll S3 bucket for new/updated files"""
        logger.info("Starting S3 polling mode...")
        
        while not self.stop_event.is_set():
            try:
                # List objects in bucket
                paginator = self.s3_client.get_paginator('list_objects_v2')
                pages = paginator.paginate(
                    Bucket=self.config.bucket,
                    Prefix=self.config.prefix
                )
                
                new_files = 0
                updated_files = 0
                
                for page in pages:
                    if self.stop_event.is_set():
                        break
                    
                    for obj in page.get('Contents', []):
                        key = obj['Key']
                        etag = obj['ETag'].strip('"')
                        size = obj['Size']
                        last_modified = obj['LastModified'].isoformat()
                        
                        # Apply file pattern filter if configured
                        if self.config.file_pattern:
                            import re
                            if not re.match(self.config.file_pattern, key):
                                continue
                        
                        # Check if file is new or updated
                        if key not in self.seen_files:
                            # New file
                            self._publish_file_event(key, etag, size, last_modified, operation='create')
                            self.seen_files[key] = etag
                            new_files += 1
                        elif self.seen_files[key] != etag:
                            # Updated file
                            self._publish_file_event(key, etag, size, last_modified, operation='update')
                            self.seen_files[key] = etag
                            updated_files += 1
                
                if new_files > 0 or updated_files > 0:
                    logger.info(f"Detected {new_files} new files, {updated_files} updated files")
                
                # Sleep until next poll
                self.stop_event.wait(self.config.poll_interval_seconds)
                
            except ClientError as e:
                logger.error(f"S3 polling error: {e}")
                self.stop_event.wait(10)  # Wait before retry
    
    
    def _publish_file_event(
        self,
        key: str,
        etag: str,
        size: int,
        last_modified: str,
        operation: str
    ):
        """Publish file event to Kafka topic"""
        try:
            message = {
                'op': operation,  # create, update, delete
                'source': {
                    'bucket': self.config.bucket,
                    'region': self.config.region,
                    'ts_ms': int(datetime.utcnow().timestamp() * 1000)
                },
                'file': {
                    'key': key,
                    'etag': etag,
                    'size': size,
                    'last_modified': last_modified,
                    's3_uri': f"s3://{self.config.bucket}/{key}"
                }
            }
            
            # Use file key as Kafka key for partitioning
            kafka_key = key
            
            # Send to Kafka
            future = self.producer.send(self.kafka_topic, key=kafka_key, value=message)
            future.get(timeout=5)
            
        except Exception as e:
            logger.error(f"Failed to publish file event: {e}")
    
    
    def stop(self):
        """Stop the connector"""
        logger.info("Stopping S3 connector...")
        self.stop_event.set()
        
        if self.producer:
            self.producer.flush()
            self.producer.close()


# Job Management Functions

def mongodb_runner(
    config: MongoDBConfig,
    kafka_topic: str = "cdc.mongodb",
    kafka_bootstrap_servers: str = "localhost:9092",
    job_id: Optional[str] = None
) -> str:
    """
    Start MongoDB connector in a background thread.
    
    Args:
        config: MongoDB configuration
        kafka_topic: Target Kafka topic
        kafka_bootstrap_servers: Kafka bootstrap servers
        job_id: Optional job ID (auto-generated if None)
        
    Returns:
        Job ID for tracking
    """
    job_id = job_id or f"mongodb-{int(time.time())}"
    
    connector = MongoDBConnector(config, kafka_topic, kafka_bootstrap_servers)
    
    def run():
        try:
            connector.start()
        except Exception as e:
            logger.error(f"MongoDB connector failed: {e}")
            _active_jobs[job_id]['status'] = 'failed'
            _active_jobs[job_id]['error'] = str(e)
    
    thread = Thread(target=run, daemon=True)
    thread.start()
    
    _active_jobs[job_id] = {
        'type': 'mongodb',
        'status': 'running',
        'connector': connector,
        'thread': thread,
        'started_at': datetime.utcnow().isoformat(),
        'config': asdict(config)
    }
    
    logger.info(f"Started MongoDB connector job: {job_id}")
    return job_id


def s3_runner(
    config: S3Config,
    kafka_topic: str = "cdc.s3",
    kafka_bootstrap_servers: str = "localhost:9092",
    job_id: Optional[str] = None
) -> str:
    """
    Start S3 connector in a background thread.
    
    Args:
        config: S3 configuration
        kafka_topic: Target Kafka topic
        kafka_bootstrap_servers: Kafka bootstrap servers
        job_id: Optional job ID (auto-generated if None)
        
    Returns:
        Job ID for tracking
    """
    job_id = job_id or f"s3-{int(time.time())}"
    
    connector = S3Connector(config, kafka_topic, kafka_bootstrap_servers)
    
    def run():
        try:
            connector.start()
        except Exception as e:
            logger.error(f"S3 connector failed: {e}")
            _active_jobs[job_id]['status'] = 'failed'
            _active_jobs[job_id]['error'] = str(e)
    
    thread = Thread(target=run, daemon=True)
    thread.start()
    
    _active_jobs[job_id] = {
        'type': 's3',
        'status': 'running',
        'connector': connector,
        'thread': thread,
        'started_at': datetime.utcnow().isoformat(),
        'config': asdict(config)
    }
    
    logger.info(f"Started S3 connector job: {job_id}")
    return job_id


def stop_job(job_id: str) -> bool:
    """
    Stop a running connector job.
    
    Args:
        job_id: Job ID to stop
        
    Returns:
        True if stopped successfully
    """
    if job_id not in _active_jobs:
        logger.warning(f"Job not found: {job_id}")
        return False
    
    job = _active_jobs[job_id]
    connector = job.get('connector')
    
    if connector:
        connector.stop()
    
    job['status'] = 'stopped'
    job['stopped_at'] = datetime.utcnow().isoformat()
    
    logger.info(f"Stopped job: {job_id}")
    return True


def get_job(job_id: str) -> Optional[Dict[str, Any]]:
    """
    Get job details.
    
    Args:
        job_id: Job ID
        
    Returns:
        Job details or None if not found
    """
    if job_id not in _active_jobs:
        return None
    
    job = _active_jobs[job_id].copy()
    # Remove non-serializable objects
    job.pop('connector', None)
    job.pop('thread', None)
    return job


def list_jobs() -> List[Dict[str, Any]]:
    """
    List all active jobs.
    
    Returns:
        List of job details
    """
    return [get_job(job_id) for job_id in _active_jobs.keys()]
