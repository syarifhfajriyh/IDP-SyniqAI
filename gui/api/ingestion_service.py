"""
Ingestion Service - Orchestrates data ingestion from various sources
Wires GUI to actual data connectors
"""

import sys
from pathlib import Path
from typing import Dict, List, Optional
from datetime import datetime
import logging

# Add connector path
connector_path = Path(__file__).parent.parent.parent / "data ingestion" / "Connector"
sys.path.append(str(connector_path))

logger = logging.getLogger(__name__)

class IngestionService:
    def __init__(self, minio_client, job_tracker, kafka_bridge=None, kafka_startup_service=None):
        """Initialize ingestion service
        
        Args:
            minio_client: MinIO client for bronze layer storage
            job_tracker: Job tracker for monitoring ingestion jobs
            kafka_bridge: Optional KafkaBridge for streaming to Kafka (purely additive)
            kafka_startup_service: Optional KafkaStartupService for CDC automation
        """
        self.minio = minio_client
        self.job_tracker = job_tracker
        self.kafka_bridge = kafka_bridge
        self.kafka_startup_service = kafka_startup_service
        self.connectors = {}
    
    def test_connection(self, source_type: str, config: Dict) -> Dict:
        """Test database connection before ingestion"""
        try:
            logger.info(f"Testing {source_type} connection...")
            logger.info(f"Config keys: {list(config.keys())}")
            
            if source_type == "postgres":
                try:
                    from postgres_connector import PostgresConnector
                    logger.info("PostgresConnector imported successfully")
                except ImportError as ie:
                    logger.error(f"Failed to import PostgresConnector: {ie}")
                    return {
                        "success": False,
                        "error": f"Failed to import postgres_connector: {str(ie)}",
                        "message": "Connector module not found"
                    }
                
                connection_config = {
                    "host": config.get("host"),
                    "port": config.get("port", 5432),
                    "database": config.get("database"),
                    "user": config.get("user"),
                    "password": config.get("password")
                }
                
                logger.info(f"Attempting connection to {connection_config['host']}:{connection_config['port']}")
                
                try:
                    connector = PostgresConnector(connection_config)
                    logger.info("PostgresConnector instance created")
                    
                    # IMPORTANT: Must call connect() to create the engine
                    logger.info("Calling connector.connect() to create engine...")
                    connector.connect()
                    logger.info("Engine created successfully")
                    
                except Exception as ce:
                    logger.error(f"Failed to create connector: {ce}", exc_info=True)
                    return {
                        "success": False,
                        "error": str(ce),
                        "message": f"Failed to create connector: {type(ce).__name__}",
                        "error_type": type(ce).__name__
                    }
                
                # Test connection
                try:
                    logger.info("Validating credentials...")
                    connector.validate_credentials()
                    logger.info("Credentials validated successfully")
                    
                    # Get available tables using SQLAlchemy inspector
                    logger.info("Fetching table list...")
                    from sqlalchemy import inspect as sqla_inspect
                    inspector = sqla_inspect(connector.engine)
                    table_names = inspector.get_table_names()
                    logger.info(f"Found {len(table_names)} tables")
                    
                    return {
                        "success": True,
                        "message": "Connection successful",
                        "server_version": "PostgreSQL",
                        "available_tables": table_names[:50],  # First 50 tables
                        "total_tables": len(table_names)
                    }
                
                except Exception as e:
                    logger.error(f"Connection validation failed: {e}", exc_info=True)
                    error_msg = str(e)
                    
                    # Provide more specific error messages
                    if "password authentication failed" in error_msg.lower():
                        message = "Authentication failed: Invalid username or password"
                    elif "could not connect" in error_msg.lower() or "connection refused" in error_msg.lower():
                        message = "Connection refused: Check host and port, ensure PostgreSQL is running"
                    elif "database" in error_msg.lower() and "does not exist" in error_msg.lower():
                        message = "Database does not exist"
                    elif "timeout" in error_msg.lower():
                        message = "Connection timeout: Network issue or firewall blocking connection"
                    else:
                        message = "Connection failed. Check credentials and network."
                    
                    return {
                        "success": False,
                        "error": error_msg,
                        "message": message,
                        "error_type": type(e).__name__,
                        "config_tested": {
                            "host": connection_config["host"],
                            "port": connection_config["port"],
                            "database": connection_config["database"],
                            "user": connection_config["user"]
                        }
                    }
            
            elif source_type == "mariadb":
                try:
                    from mariadb_connector import MariaDBConnector
                    logger.info("MariaDBConnector imported successfully")
                except ImportError as ie:
                    logger.error(f"Failed to import MariaDBConnector: {ie}")
                    return {
                        "success": False,
                        "error": f"Failed to import mariadb_connector: {str(ie)}",
                        "message": "Connector module not found"
                    }
                
                connection_config = {
                    "host": config.get("host"),
                    "port": config.get("port", 3306),
                    "database": config.get("database"),
                    "user": config.get("user"),
                    "password": config.get("password")
                }
                
                logger.info(f"Attempting MariaDB connection to {connection_config['host']}:{connection_config['port']}")
                
                try:
                    connector = MariaDBConnector(connection_config)
                    logger.info("MariaDBConnector instance created")
                    
                    # IMPORTANT: Must call connect() to create the engine
                    logger.info("Calling connector.connect() to create engine...")
                    connector.connect()
                    logger.info("Engine created successfully")
                    
                except Exception as ce:
                    logger.error(f"Failed to create connector: {ce}", exc_info=True)
                    return {
                        "success": False,
                        "error": str(ce),
                        "message": f"Failed to create connector: {type(ce).__name__}",
                        "error_type": type(ce).__name__
                    }
                
                try:
                    logger.info("Validating credentials...")
                    connector.validate_credentials()
                    logger.info("Credentials validated successfully")
                    
                    # Get available tables using SQLAlchemy inspector
                    logger.info("Fetching table list...")
                    from sqlalchemy import inspect as sqla_inspect
                    inspector = sqla_inspect(connector.engine)
                    table_names = inspector.get_table_names()
                    logger.info(f"Found {len(table_names)} tables")
                    
                    return {
                        "success": True,
                        "message": "Connection successful",
                        "server_version": "MariaDB",
                        "available_tables": table_names[:50],
                        "total_tables": len(table_names)
                    }
                
                except Exception as e:
                    logger.error(f"MariaDB connection failed: {e}", exc_info=True)
                    error_msg = str(e)
                    
                    if "access denied" in error_msg.lower():
                        message = "Access denied: Invalid username or password"
                    elif "can't connect" in error_msg.lower():
                        message = "Cannot connect: Check host and port, ensure MariaDB is running"
                    elif "unknown database" in error_msg.lower():
                        message = "Database does not exist"
                    else:
                        message = "Connection failed. Check credentials and network."
                    
                    return {
                        "success": False,
                        "error": error_msg,
                        "message": message,
                        "error_type": type(e).__name__,
                        "config_tested": {
                            "host": connection_config["host"],
                            "port": connection_config["port"],
                            "database": connection_config["database"],
                            "user": connection_config["user"]
                        }
                    }
            
            elif source_type == "mariadb_cloud":
                try:
                    from mariadbcloud_conn import MariaDBCloudConnector
                    logger.info("MariaDBCloudConnector imported successfully")
                except ImportError as ie:
                    logger.error(f"Failed to import MariaDBCloudConnector: {ie}")
                    return {
                        "success": False,
                        "error": f"Failed to import mariadbcloud_conn: {str(ie)}",
                        "message": "Connector module not found"
                    }
                
                connection_config = {
                    "host": config.get("host"),
                    "port": config.get("port", 4020),
                    "database": config.get("database"),
                    "user": config.get("user"),
                    "password": config.get("password"),
                    "ssl_ca": config.get("ssl_ca", "").strip() if config.get("ssl_ca") else None,
                    "ssl_verify_cert": True,
                    "connect_timeout": 30,
                    "read_timeout": 60
                }
                
                logger.info(f"Attempting MariaDB Cloud connection to {connection_config['host']}:{connection_config['port']}")
                logger.info(f"SSL Certificate Path: '{connection_config.get('ssl_ca')}' (Type: {type(connection_config.get('ssl_ca'))})")
                logger.info(f"Full config received: {config}")
                
                try:
                    connector = MariaDBCloudConnector(connection_config)
                    logger.info("MariaDBCloudConnector instance created")
                    
                    # IMPORTANT: Must call connect() to create the engine
                    logger.info("Calling connector.connect() to create engine...")
                    connector.connect()
                    logger.info("Engine created successfully")
                    
                except Exception as ce:
                    logger.error(f"Failed to create connector: {ce}", exc_info=True)
                    return {
                        "success": False,
                        "error": str(ce),
                        "message": f"Failed to create connector: {type(ce).__name__}",
                        "error_type": type(ce).__name__
                    }
                
                try:
                    logger.info("Validating credentials...")
                    connector.validate_credentials()
                    logger.info("Credentials validated successfully")
                    
                    # Get available tables using SQLAlchemy inspector
                    logger.info("Fetching table list...")
                    from sqlalchemy import inspect as sqla_inspect
                    inspector = sqla_inspect(connector.engine)
                    table_names = inspector.get_table_names()
                    logger.info(f"Found {len(table_names)} tables")
                    
                    return {
                        "success": True,
                        "message": "Connection successful (MariaDB Cloud)",
                        "server_version": "MariaDB SkySQL",
                        "available_tables": table_names[:50],
                        "total_tables": len(table_names)
                    }
                
                except Exception as e:
                    logger.error(f"MariaDB Cloud connection failed: {e}", exc_info=True)
                    error_msg = str(e)
                    
                    if "ssl" in error_msg.lower() or "certificate" in error_msg.lower():
                        message = "SSL/Certificate error: Check ssl_ca path or certificate validity"
                    elif "show databases" in error_msg.lower() or "1227" in error_msg:
                        message = "Permission issue: User needs SELECT privilege on database (SHOW DATABASES not required)"
                    elif "access denied" in error_msg.lower() and "password" in error_msg.lower():
                        message = "Access denied: Invalid username or password"
                    elif "can't connect" in error_msg.lower():
                        message = "Cannot connect: Check host and port, ensure MariaDB Cloud is accessible"
                    elif "unknown database" in error_msg.lower():
                        message = "Database does not exist"
                    else:
                        message = "Connection failed. Check credentials, SSL certificate, and network."
                    
                    return {
                        "success": False,
                        "error": error_msg,
                        "message": message,
                        "error_type": type(e).__name__,
                        "config_tested": {
                            "host": connection_config["host"],
                            "port": connection_config["port"],
                            "database": connection_config["database"],
                            "user": connection_config["user"],
                            "ssl_ca": connection_config.get("ssl_ca", "not provided")
                        }
                    }
            
            elif source_type == "s3":
                # S3 connection test
                import boto3
                from botocore.exceptions import ClientError, NoCredentialsError
                
                try:
                    logger.info(f"Testing S3 connection to bucket: {config.get('s3_bucket')}")
                    
                    # Extract S3 config
                    s3_bucket = config.get('s3_bucket', '').strip()
                    s3_prefix = config.get('s3_prefix', '').strip()
                    aws_access_key = config.get('aws_access_key', '').strip()
                    aws_secret_key = config.get('aws_secret_key', '').strip()
                    aws_region = config.get('aws_region', 'us-east-1').strip()
                    
                    if not s3_bucket:
                        return {
                            "success": False,
                            "error": "S3 bucket name is required",
                            "message": "Missing required field: s3_bucket"
                        }
                    
                    if not aws_access_key or not aws_secret_key:
                        return {
                            "success": False,
                            "error": "AWS credentials are required",
                            "message": "Missing aws_access_key or aws_secret_key"
                        }
                    
                    # Create S3 client
                    s3_client = boto3.client(
                        's3',
                        aws_access_key_id=aws_access_key,
                        aws_secret_access_key=aws_secret_key,
                        region_name=aws_region
                    )
                    
                    logger.info(f"S3 client created for region: {aws_region}")
                    
                    # Test 1: Check if bucket exists and is accessible
                    try:
                        s3_client.head_bucket(Bucket=s3_bucket)
                        logger.info(f"✅ Bucket '{s3_bucket}' is accessible")
                    except ClientError as e:
                        error_code = e.response['Error']['Code']
                        if error_code == '404':
                            return {
                                "success": False,
                                "error": f"Bucket '{s3_bucket}' does not exist",
                                "message": "Bucket not found. Check bucket name and region.",
                                "error_type": "BucketNotFound"
                            }
                        elif error_code == '403':
                            return {
                                "success": False,
                                "error": f"Access denied to bucket '{s3_bucket}'",
                                "message": "Permission denied. Check AWS credentials and bucket permissions.",
                                "error_type": "AccessDenied"
                            }
                        else:
                            raise
                    
                    # Test 2: List files in bucket (with prefix filter)
                    list_params = {'Bucket': s3_bucket, 'MaxKeys': 50}
                    if s3_prefix:
                        list_params['Prefix'] = s3_prefix
                    
                    response = s3_client.list_objects_v2(**list_params)
                    
                    files = []
                    total_size = 0
                    if 'Contents' in response:
                        for obj in response['Contents']:
                            file_name = obj['Key']
                            file_size = obj['Size']
                            files.append({
                                'name': file_name,
                                'size': file_size,
                                'last_modified': obj['LastModified'].isoformat()
                            })
                            total_size += file_size
                    
                    logger.info(f"Found {len(files)} files in bucket (total size: {total_size / 1024 / 1024:.2f} MB)")
                    
                    return {
                        "success": True,
                        "message": f"S3 connection successful. Found {len(files)} files.",
                        "bucket_name": s3_bucket,
                        "prefix": s3_prefix or "(root)",
                        "region": aws_region,
                        "available_tables": [f['name'] for f in files[:50]],  # Use files as "tables"
                        "total_tables": len(files),
                        "total_size_mb": round(total_size / 1024 / 1024, 2)
                    }
                
                except NoCredentialsError:
                    logger.error("AWS credentials not found or invalid")
                    return {
                        "success": False,
                        "error": "Invalid AWS credentials",
                        "message": "AWS access key or secret key is invalid",
                        "error_type": "NoCredentialsError"
                    }
                except ClientError as e:
                    logger.error(f"AWS ClientError: {e}", exc_info=True)
                    return {
                        "success": False,
                        "error": str(e),
                        "message": f"AWS Error: {e.response['Error']['Message']}",
                        "error_type": "ClientError"
                    }
                except Exception as e:
                    logger.error(f"S3 connection test failed: {e}", exc_info=True)
                    return {
                        "success": False,
                        "error": str(e),
                        "message": "S3 connection failed. Check credentials and network.",
                        "error_type": type(e).__name__,
                        "config_tested": {
                            "bucket": s3_bucket,
                            "prefix": s3_prefix or "(none)",
                            "region": aws_region
                        }
                    }
            
            elif source_type == "mongodb":
                try:
                    from pymongo import MongoClient
                    from pymongo.errors import ConnectionFailure, OperationFailure, ConfigurationError
                    logger.info("pymongo imported successfully")
                except ImportError as ie:
                    logger.error(f"Failed to import pymongo: {ie}")
                    return {
                        "success": False,
                        "error": f"pymongo not installed: {str(ie)}",
                        "message": "Install pymongo: pip install pymongo"
                    }
                
                mongo_uri = config.get("mongo_uri")
                database_name = config.get("database")
                
                if not mongo_uri or not database_name:
                    return {
                        "success": False,
                        "error": "Missing required fields: mongo_uri and database are required",
                        "message": "Please provide MongoDB URI and database name"
                    }
                
                logger.info(f"Attempting MongoDB connection to {mongo_uri}")
                
                try:
                    # Create MongoDB client with timeout
                    client = MongoClient(mongo_uri, serverSelectionTimeoutMS=5000)
                    logger.info("MongoDB client created")
                    
                    # Test connection by pinging the server
                    logger.info("Pinging MongoDB server...")
                    client.admin.command('ping')
                    logger.info("MongoDB ping successful")
                    
                    # Get database
                    db = client[database_name]
                    logger.info(f"Accessing database: {database_name}")
                    
                    # List collections
                    logger.info("Fetching collection list...")
                    collection_names = db.list_collection_names()
                    logger.info(f"Found {len(collection_names)} collections")
                    
                    # Get server info
                    server_info = client.server_info()
                    version = server_info.get('version', 'Unknown')
                    
                    client.close()
                    
                    return {
                        "success": True,
                        "message": "MongoDB connection successful",
                        "server_version": f"MongoDB {version}",
                        "available_tables": collection_names[:50],  # First 50 collections
                        "total_tables": len(collection_names),
                        "database": database_name
                    }
                
                except ConnectionFailure as e:
                    logger.error(f"MongoDB connection failed: {e}", exc_info=True)
                    return {
                        "success": False,
                        "error": str(e),
                        "message": "Cannot connect to MongoDB. Check URI, network, and firewall settings.",
                        "error_type": "ConnectionFailure",
                        "config_tested": {
                            "mongo_uri": mongo_uri[:50] + "...",  # Truncate for security
                            "database": database_name
                        }
                    }
                
                except OperationFailure as e:
                    logger.error(f"MongoDB authentication failed: {e}", exc_info=True)
                    error_msg = str(e)
                    
                    if "Authentication failed" in error_msg or "auth failed" in error_msg.lower():
                        message = "Authentication failed: Invalid username or password in connection URI"
                    elif "not authorized" in error_msg.lower():
                        message = "Not authorized: User doesn't have permission to access this database"
                    else:
                        message = "Operation failed. Check credentials and permissions."
                    
                    return {
                        "success": False,
                        "error": error_msg,
                        "message": message,
                        "error_type": "OperationFailure",
                        "config_tested": {
                            "database": database_name
                        }
                    }
                
                except ConfigurationError as e:
                    logger.error(f"MongoDB configuration error: {e}", exc_info=True)
                    return {
                        "success": False,
                        "error": str(e),
                        "message": "Invalid MongoDB URI format. Check connection string syntax.",
                        "error_type": "ConfigurationError",
                        "example_uri": "mongodb://user:pass@host:27017/ or mongodb+srv://user:pass@cluster.mongodb.net/"
                    }
                
                except Exception as e:
                    logger.error(f"MongoDB connection exception: {e}", exc_info=True)
                    return {
                        "success": False,
                        "error": str(e),
                        "message": "Unexpected error during MongoDB connection test",
                        "error_type": type(e).__name__
                    }
            
            else:
                logger.warning(f"Unsupported source type: {source_type}")
                return {
                    "success": False,
                    "error": f"Unsupported source type: {source_type}",
                    "message": "Source type not supported",
                    "supported_types": ["postgres", "mariadb", "mariadb_cloud", "s3", "mongodb"]
                }
        
        except Exception as e:
            logger.error(f"Connection test exception for {source_type}: {e}", exc_info=True)
            return {
                "success": False,
                "error": str(e),
                "message": "Unexpected error during connection test",
                "error_type": type(e).__name__
            }
    
    def start_ingestion(self, source_type: str, connection_config: Dict, 
                       extraction_request: Dict, domain: str = "general", enable_cdc: bool = False) -> str:
        """Start data ingestion job
        
        Args:
            source_type: Type of data source (postgres, mariadb, mariadb_cloud, s3)
            connection_config: Database connection configuration
            extraction_request: Request containing entity name and extraction details
            domain: Data domain classification (default: "general")
            enable_cdc: Whether to automatically create CDC connector after ingestion (default: False)
        
        Returns:
            job_id: Unique identifier for the ingestion job
        """
        # Generate job ID (replace slashes for URL safety)
        entity_safe = extraction_request['entity'].replace('/', '_').replace('\\', '_')
        job_id = f"ingest_{source_type}_{entity_safe}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        
        # Create job record
        self.job_tracker.create_job(
            job_id=job_id,
            job_type="ingestion",
            source_type=source_type,
            source=extraction_request.get('entity'),
            entity=extraction_request.get('entity'),
            layer="bronze",
            metadata={
                "connection_config": {k: v for k, v in connection_config.items() if k != 'password'},
                "extraction_request": extraction_request,
                "cdc_enabled": enable_cdc
            }
        )
        
        # Update status to running
        self.job_tracker.update_job_status(job_id, "running")
        self.job_tracker.add_log(job_id, "INFO", f"Starting {source_type} ingestion for {extraction_request['entity']}")
        
        try:
            if source_type == "postgres":
                self._ingest_postgres(job_id, connection_config, extraction_request, domain)
            elif source_type == "mariadb":
                self._ingest_mariadb(job_id, connection_config, extraction_request, domain)
            elif source_type == "mariadb_cloud":
                self._ingest_mariadb_cloud(job_id, connection_config, extraction_request, domain)
            elif source_type == "s3":
                self._ingest_s3(job_id, connection_config, extraction_request, domain)
            else:
                raise ValueError(f"Unsupported source type: {source_type}")
            
            self.job_tracker.update_job_status(job_id, "completed")
            self.job_tracker.add_log(job_id, "INFO", "Ingestion completed successfully")
            
            # CREATE CDC CONNECTOR IF ENABLED (NEW!)
            if enable_cdc and self.kafka_startup_service:
                entity_name = extraction_request.get('entity')
                self.job_tracker.add_log(job_id, "INFO", f"Creating CDC connector for {entity_name}...")
                
                try:
                    success, message = self.kafka_startup_service.create_cdc_connector(
                        source_type=source_type,
                        connection_config=connection_config,
                        table_name=entity_name
                    )
                    
                    if success:
                        self.job_tracker.add_log(job_id, "INFO", f"✓ CDC connector created: {message}")
                        logger.info(f"✓ CDC connector created for {entity_name}: {message}")
                    else:
                        self.job_tracker.add_log(job_id, "WARNING", f"⚠ CDC setup failed: {message}")
                        logger.warning(f"⚠ CDC setup failed for {entity_name}: {message}")
                
                except Exception as cdc_error:
                    self.job_tracker.add_log(job_id, "WARNING", f"⚠ CDC error: {str(cdc_error)}")
                    logger.error(f"CDC connector creation error: {cdc_error}")
        
        except Exception as e:
            logger.error(f"Ingestion failed for {job_id}: {e}")
            self.job_tracker.update_job_status(job_id, "failed", error_message=str(e))
            self.job_tracker.add_log(job_id, "ERROR", f"Ingestion failed: {str(e)}")
        
        return job_id
    
    def _ingest_postgres(self, job_id: str, connection_config: Dict, extraction_request: Dict, domain: str = "general"):
        """Ingest data from PostgreSQL"""
        from postgres_connector import PostgresConnector
        import pyarrow.parquet as pq
        import pyarrow as pa
        from io import BytesIO
        
        entity = extraction_request['entity']
        logger.info(f"🚀 Starting PostgreSQL ingestion - Domain: {domain}, Entity: {entity}")
        
        connector = PostgresConnector(connection_config)
        connector.connect()  # Create the engine
        
        chunk_size = extraction_request.get('chunk_size', 50000)
        
        # Build extraction plan for connector
        extraction_plan = {
            "entity": entity,
            "chunk_size": chunk_size,
            "mode": "full",
            "enable_parallel": False,
            "num_workers": 1
        }
        
        # Extract data in chunks
        chunk_num = 0
        total_rows = 0
        
        logger.info(f"Starting extraction for {entity} with chunk_size={chunk_size}")
        
        for payload in connector.extract(extraction_plan):
            # payload has "data" (DataFrame) and "metadata" keys
            df_chunk = payload["data"]
            chunk_num += 1
            rows_in_chunk = len(df_chunk)
            total_rows += rows_in_chunk
            
            logger.info(f"Processing chunk {chunk_num}: {rows_in_chunk} rows")
            
            # Convert to Parquet
            table = pa.Table.from_pandas(df_chunk)
            buffer = BytesIO()
            pq.write_table(table, buffer)
            buffer.seek(0)
            
            # Upload to MinIO
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            minio_path = f"{domain}/postgres/{entity}/chunk_{chunk_num}_{timestamp}.parquet"
            
            logger.info(f"📦 Uploading to MinIO: bucket=syniqai-bronze, path={minio_path}")
            
            self.minio.client.put_object(
                "syniqai-bronze",
                minio_path,
                buffer,
                length=buffer.getbuffer().nbytes,
                content_type='application/octet-stream'
            )
            
            # Publish to Kafka (additive - never blocks ingestion)
            if self.kafka_bridge:
                kafka_metadata = {
                    "job_id": job_id,
                    "entity": entity,
                    "domain": domain,
                    "minio_path": minio_path,
                    "timestamp": timestamp,
                }
                self.kafka_bridge.publish_dataframe(
                    "postgres",
                    df_chunk,
                    kafka_metadata,
                    chunk_num=chunk_num,
                )
            
            # Update job progress
            self.job_tracker.update_job_status(
                job_id,
                "running",
                rows_processed=total_rows,
                chunks_written=chunk_num,
                minio_location=f"syniqai-bronze/{domain}/postgres/{entity}/"
            )
            
            self.job_tracker.add_log(
                job_id, 
                "INFO", 
                f"Chunk {chunk_num}: {rows_in_chunk} rows written to {minio_path}"
            )
            
            logger.info(f"Job {job_id}: Processed chunk {chunk_num}, total rows: {total_rows}")
        
        # Save metadata
        metadata = {
            "row_count": total_rows,
            "chunk_count": chunk_num,
            "ingested_at": datetime.now().isoformat(),
            "source_type": "postgres",
            "entity": entity,
            "domain": domain
        }
        self.minio.save_metadata("bronze", f"{domain}/postgres", entity, metadata)
        
        logger.info(f"Job {job_id}: Completed. Total rows: {total_rows}, chunks: {chunk_num}")
    
    def _ingest_mariadb(self, job_id: str, connection_config: Dict, extraction_request: Dict, domain: str = "general"):
        """Ingest data from MariaDB"""
        from mariadb_connector import MariaDBConnector
        import pyarrow.parquet as pq
        import pyarrow as pa
        from io import BytesIO
        
        connector = MariaDBConnector(connection_config)
        connector.connect()  # Create the engine
        
        entity = extraction_request['entity']
        chunk_size = extraction_request.get('chunk_size', 50000)
        
        # Build extraction plan for connector
        extraction_plan = {
            "entity": entity,
            "chunk_size": chunk_size,
            "mode": "full",
            "enable_parallel": False,
            "num_workers": 1
        }
        
        chunk_num = 0
        total_rows = 0
        
        logger.info(f"Starting extraction for {entity} with chunk_size={chunk_size}")
        
        for payload in connector.extract(extraction_plan):
            # payload has "data" (DataFrame) and "metadata" keys
            df_chunk = payload["data"]
            chunk_num += 1
            rows_in_chunk = len(df_chunk)
            total_rows += rows_in_chunk
            
            logger.info(f"Processing chunk {chunk_num}: {rows_in_chunk} rows")
            
            # Convert to Parquet
            table = pa.Table.from_pandas(df_chunk)
            buffer = BytesIO()
            pq.write_table(table, buffer)
            buffer.seek(0)
            
            # Upload to MinIO
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            minio_path = f"{domain}/mariadb/{entity}/chunk_{chunk_num}_{timestamp}.parquet"
            
            self.minio.client.put_object(
                "syniqai-bronze",
                minio_path,
                buffer,
                length=buffer.getbuffer().nbytes,
                content_type='application/octet-stream'
            )
            
            # Publish to Kafka (additive - never blocks ingestion)
            if self.kafka_bridge:
                kafka_metadata = {
                    "job_id": job_id,
                    "entity": entity,
                    "domain": domain,
                    "minio_path": minio_path,
                    "timestamp": timestamp,
                }
                self.kafka_bridge.publish_dataframe(
                    "mariadb",
                    df_chunk,
                    kafka_metadata,
                    chunk_num=chunk_num,
                )
            
            self.job_tracker.update_job_status(
                job_id,
                "running",
                rows_processed=total_rows,
                chunks_written=chunk_num,
                minio_location=f"syniqai-bronze/{domain}/mariadb/{entity}/"
            )
            
            self.job_tracker.add_log(
                job_id,
                "INFO",
                f"Chunk {chunk_num}: {rows_in_chunk} rows written"
            )
        
        # Save metadata
        metadata = {
            "row_count": total_rows,
            "chunk_count": chunk_num,
            "ingested_at": datetime.now().isoformat(),
            "source_type": "mariadb",
            "entity": entity,
            "domain": domain
        }
        self.minio.save_metadata("bronze", f"{domain}/mariadb", entity, metadata)
    
    def _ingest_mariadb_cloud(self, job_id: str, connection_config: Dict, extraction_request: Dict, domain: str = "general"):
        """Ingest data from MariaDB Cloud (SkySQL)"""
        from mariadbcloud_conn import MariaDBCloudConnector
        import pyarrow.parquet as pq
        import pyarrow as pa
        from io import BytesIO
        
        # Ensure SSL configuration
        cloud_config = {
            "host": connection_config.get("host"),
            "port": connection_config.get("port", 4020),
            "database": connection_config.get("database"),
            "user": connection_config.get("user"),
            "password": connection_config.get("password"),
            "ssl_ca": connection_config.get("ssl_ca", "").strip() if connection_config.get("ssl_ca") else None,
            "ssl_verify_cert": True,
            "connect_timeout": 30,
            "read_timeout": 60
        }
        
        connector = MariaDBCloudConnector(cloud_config)
        connector.connect()  # Create the engine with SSL
        
        entity = extraction_request['entity']
        chunk_size = extraction_request.get('chunk_size', 50000)
        
        # Build extraction plan for connector
        extraction_plan = {
            "entity": entity,
            "chunk_size": chunk_size,
            "mode": "full",
            "enable_parallel": False,
            "num_workers": 1
        }
        
        chunk_num = 0
        total_rows = 0
        
        logger.info(f"Starting MariaDB Cloud extraction for {entity} with chunk_size={chunk_size}")
        
        for payload in connector.extract(extraction_plan):
            # payload has "data" (DataFrame) and "metadata" keys
            df_chunk = payload["data"]
            chunk_num += 1
            rows_in_chunk = len(df_chunk)
            total_rows += rows_in_chunk
            
            logger.info(f"Processing chunk {chunk_num}: {rows_in_chunk} rows")
            
            # Convert to Parquet
            table = pa.Table.from_pandas(df_chunk)
            buffer = BytesIO()
            pq.write_table(table, buffer)
            buffer.seek(0)
            
            # Upload to MinIO
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            minio_path = f"{domain}/mariadb_cloud/{entity}/chunk_{chunk_num}_{timestamp}.parquet"
            
            self.minio.client.put_object(
                "syniqai-bronze",
                minio_path,
                buffer,
                length=buffer.getbuffer().nbytes,
                content_type='application/octet-stream'
            )
            
            # Publish to Kafka (additive - never blocks ingestion)
            if self.kafka_bridge:
                kafka_metadata = {
                    "job_id": job_id,
                    "entity": entity,
                    "domain": domain,
                    "minio_path": minio_path,
                    "timestamp": timestamp,
                }
                self.kafka_bridge.publish_dataframe(
                    "mariadb_cloud",
                    df_chunk,
                    kafka_metadata,
                    chunk_num=chunk_num,
                )
            
            self.job_tracker.update_job_status(
                job_id,
                "running",
                rows_processed=total_rows,
                chunks_written=chunk_num,
                minio_location=f"syniqai-bronze/{domain}/mariadb_cloud/{entity}/"
            )
            
            self.job_tracker.add_log(
                job_id,
                "INFO",
                f"Chunk {chunk_num}: {rows_in_chunk} rows written (SSL)"
            )
        
        # Save metadata
        metadata = {
            "row_count": total_rows,
            "chunk_count": chunk_num,
            "ingested_at": datetime.now().isoformat(),
            "source_type": "mariadb_cloud",
            "entity": entity,
            "cloud_provider": "SkySQL",
            "domain": domain
        }
        self.minio.save_metadata("bronze", f"{domain}/mariadb_cloud", entity, metadata)
        
        logger.info(f"Job {job_id}: MariaDB Cloud ingestion completed. Total rows: {total_rows}, chunks: {chunk_num}")
    
    def _ingest_s3(self, job_id: str, connection_config: Dict, extraction_request: Dict, domain: str = "general"):
        """Ingest data from S3 to MinIO Bronze"""
        import boto3
        from botocore.exceptions import ClientError
        from io import BytesIO
        
        # Extract config
        s3_bucket = connection_config.get('s3_bucket')
        s3_prefix = connection_config.get('s3_prefix', '')
        aws_access_key = connection_config.get('aws_access_key')
        aws_secret_key = connection_config.get('aws_secret_key')
        aws_region = connection_config.get('aws_region', 'us-east-1')
        
        s3_file_key = extraction_request['entity']  # Full S3 key/path
        
        logger.info(f"Job {job_id}: Starting S3 ingestion for {s3_file_key}")
        
        # Create S3 client
        s3_client = boto3.client(
            's3',
            aws_access_key_id=aws_access_key,
            aws_secret_access_key=aws_secret_key,
            region_name=aws_region
        )
        
        try:
            # Download file from S3
            logger.info(f"Downloading s3://{s3_bucket}/{s3_file_key}")
            response = s3_client.get_object(Bucket=s3_bucket, Key=s3_file_key)
            file_content = response['Body'].read()
            file_size = len(file_content)
            
            self.job_tracker.add_log(
                job_id,
                "INFO",
                f"Downloaded {file_size / 1024:.2f} KB from S3"
            )
            
            # Upload to MinIO Bronze
            # Preserve directory structure but remove prefix
            minio_path = s3_file_key
            if s3_prefix and minio_path.startswith(s3_prefix):
                minio_path = minio_path[len(s3_prefix):]
            
            minio_path = f"{domain}/s3/{minio_path}"
            
            logger.info(f"Uploading to MinIO: syniqai-bronze/{minio_path}")
            
            buffer = BytesIO(file_content)
            self.minio.client.put_object(
                "syniqai-bronze",
                minio_path,
                buffer,
                length=file_size,
                content_type=response.get('ContentType', 'application/octet-stream')
            )
            
            # Publish to Kafka (additive - never blocks ingestion)
            if self.kafka_bridge:
                kafka_metadata = {
                    "job_id": job_id,
                    "source_bucket": s3_bucket,
                    "source_key": s3_file_key,
                    "domain": domain,
                }
                self.kafka_bridge.publish_file(
                    "s3",
                    s3_file_key,
                    file_size,
                    minio_path,
                    kafka_metadata,
                )
            
            self.job_tracker.update_job_status(
                job_id,
                "running",
                rows_processed=1,
                chunks_written=1,
                minio_location=f"syniqai-bronze/{minio_path}"
            )
            
            self.job_tracker.add_log(
                job_id,
                "INFO",
                f"File uploaded to syniqai-bronze/{minio_path}"
            )
            
            # Save metadata
            metadata = {
                "source_bucket": s3_bucket,
                "source_key": s3_file_key,
                "file_size": file_size,
                "content_type": response.get('ContentType', 'unknown'),
                "ingested_at": datetime.now().isoformat(),
                "source_type": "s3",
                "domain": domain
            }
            self.minio.save_metadata("bronze", f"{domain}/s3", s3_file_key.replace('/', '_'), metadata)
            
            logger.info(f"Job {job_id}: S3 ingestion completed. File size: {file_size / 1024:.2f} KB")
            
        except ClientError as e:
            error_msg = f"S3 ClientError: {e.response['Error']['Message']}"
            logger.error(error_msg)
            raise Exception(error_msg)
        except Exception as e:
            logger.error(f"S3 ingestion failed: {e}", exc_info=True)
            raise
