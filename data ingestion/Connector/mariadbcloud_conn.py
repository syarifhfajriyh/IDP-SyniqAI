"""
MariaDB SkySQL Cloud Connector
================================
Optimized connector for MariaDB SkySQL (Serverless/DBaaS)
Inherits from MariaDBConnector with cloud-specific enhancements.

Key Differences from Self-Hosted:
- Mandatory SSL/TLS encryption
- Optimized for network latency
- Reduced parallelism (serverless connection limits)
- Enhanced error handling for cloud failures
- Automatic retry with exponential backoff

Usage:
    from mariadbcloud_conn import MariaDBCloudConnector
    
    config = {
        "host": "serverless-us-west-2.sysp0000.db1.skysql.com",
        "port": 4020,
        "database": "idp_ingestion_db",
        "user": "idp_reader",
        "password": "read_only_password",
        "ssl_ca": "C:/path/to/globalsignrootca.pem"
    }
    
    connector = MariaDBCloudConnector(config)
    connector.connect()
    
    for batch in connector.extract({"entity": "customers", "mode": "full"}):
        print(f"Got {len(batch)} rows")
"""

import logging
import time
from typing import Dict, Any, Tuple
from sqlalchemy import create_engine, text
from mariadb_connector import MariaDBConnector, ExtractionOptimizer

# Configure Logger
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("MariaDBCloudConnector")

# ==========================================
# POOLING
# ==========================================
CLOUD_DEFAULTS = {
    "connect_timeout": 30,      # Longer timeout for network
    "read_timeout": 60,         # Longer read timeout
    "pool_size": 3,             # Smaller pool (serverless limits)
    "max_overflow": 2,          # Reduced overflow
    "pool_recycle": 900,        # 15 min (shorter for cloud)
    "max_retries": 3,           # Connection retry attempts
    "retry_delay": 2,           # Initial retry delay (seconds)
}

SERVERLESS_OPTIMIZATION = {
    "max_workers": 2,           # Force low parallelism
    "chunk_size_multiplier": 2.0,  # Larger chunks (reduce roundtrips)
    "latency_threshold_ms": 100,   # Warn if latency > 100ms
}


# ==========================================
# CLOUD EXTRACTION OPTIMIZER
# ==========================================
class CloudExtractionOptimizer(ExtractionOptimizer):
    """Cloud-aware optimizer with latency considerations"""
    
    def calculate_optimal_params(
        self, 
        row_count: int,
        avg_row_size_bytes: int = 1024,
        is_remote: bool = True,
        engine_type: str = "innodb"
    ) -> Tuple[int, int]:
        """Override parent method with cloud optimizations"""
        
        # Get base calculations from parent
        base_chunk_size, base_workers = super().calculate_optimal_params(
            row_count, avg_row_size_bytes, is_remote, engine_type
        )
        
        # 🔥 CLOUD ADJUSTMENTS
        # 1. Increase chunk size (reduce network roundtrips)
        cloud_chunk_size = int(base_chunk_size * SERVERLESS_OPTIMIZATION["chunk_size_multiplier"])
        cloud_chunk_size = min(cloud_chunk_size, 200_000)  # Cap at 200k
        
        # 2. Reduce workers (serverless connection limits)
        cloud_workers = min(base_workers, SERVERLESS_OPTIMIZATION["max_workers"])
        
        # 3. Disable parallelism for small tables (< 10k rows)
        if row_count < 10_000:
            cloud_workers = 1
            logger.info(f"☁️  Small table detected: Using single-threaded extraction")
        
        logger.info(
            f"☁️  Cloud Optimization: "
            f"chunk_size={cloud_chunk_size:,} (base: {base_chunk_size:,}), "
            f"workers={cloud_workers} (base: {base_workers})"
        )
        
        return cloud_chunk_size, cloud_workers


# ==========================================
# MARIADB CLOUD CONNECTOR
# ==========================================
class MariaDBCloudConnector(MariaDBConnector):
    """
    MariaDB SkySQL Connector with Cloud Optimizations
    
    New Features:
    ✅ Mandatory SSL validation
    ✅ Automatic retry with exponential backoff
    ✅ Latency monitoring
    ✅ Cloud-optimized parallelism (max 2 workers)
    ✅ Enhanced error messages for cloud issues
    """
    
    source_type = "mariadb_cloud"
    
    def __init__(self, connection_config: Dict[str, Any], secret_handler=None):
        """Initialize with cloud-specific defaults"""
        
        # Validate SSL configuration (mandatory for cloud)
        ssl_ca = connection_config.get("ssl_ca", "").strip()
        if not ssl_ca:
            raise ValueError(
                "❌ SSL certificate is REQUIRED for MariaDB SkySQL.\n"
                "Download from: https://supplychain.mariadb.com/skysql-chain.pem\n"
                "Add to config: 'ssl_ca': '/path/to/certificate.pem'"
            )
        
        # Validate SSL certificate file exists
        import os
        if not os.path.isfile(ssl_ca):
            raise FileNotFoundError(
                f"❌ SSL certificate file not found: {ssl_ca}\n"
                f"Please check:\n"
                f"  1. File path is correct\n"
                f"  2. File exists at specified location\n"
                f"  3. No leading/trailing spaces in path"
            )
        
        # Update config with cleaned path
        connection_config["ssl_ca"] = ssl_ca
        
        # Inject cloud defaults
        connection_config.setdefault("connect_timeout", CLOUD_DEFAULTS["connect_timeout"])
        connection_config.setdefault("read_timeout", CLOUD_DEFAULTS["read_timeout"])
        connection_config.setdefault("ssl_verify_cert", True)
        connection_config["is_cloud"] = True  # Flag for optimization logic
        
        # Initialize parent
        super().__init__(connection_config, secret_handler)
        
        # Override optimizer with cloud version
        self.optimizer = CloudExtractionOptimizer()
        
        logger.info(f"☁️  MariaDB Cloud Connector initialized for {connection_config.get('host')}")
    
    def connect(self) -> None:
        """Cloud-aware connection with retry logic"""
        cfg = self.connection_config
        
        # Build connection string
        conn_str = (
            f"mysql+pymysql://{cfg['user']}:{cfg['password']}"
            f"@{cfg['host']}:{cfg['port']}/{cfg['database']}"
        )
        
        # 🔥 CLOUD-SPECIFIC CONNECT ARGS
        connect_args = {
            'connect_timeout': cfg.get('connect_timeout', CLOUD_DEFAULTS['connect_timeout']),
            'read_timeout': cfg.get('read_timeout', CLOUD_DEFAULTS['read_timeout']),
        }
        
        # Mandatory SSL for cloud
        if cfg.get("ssl_ca"):
            connect_args['ssl'] = {
                'ca': cfg['ssl_ca'],
                'check_hostname': cfg.get('ssl_verify_cert', True)
            }
            logger.info(f"🔒 SSL Certificate: {cfg['ssl_ca']}")
        
        # Retry logic for cloud connectivity issues
        max_retries = CLOUD_DEFAULTS['max_retries']
        retry_delay = CLOUD_DEFAULTS['retry_delay']
        
        for attempt in range(1, max_retries + 1):
            try:
                logger.info(f"🔌 Connecting to MariaDB SkySQL at {cfg['host']}:{cfg['port']} (Attempt {attempt}/{max_retries})")
                
                self.engine = create_engine(
                    conn_str,
                    pool_size=CLOUD_DEFAULTS['pool_size'],
                    max_overflow=CLOUD_DEFAULTS['max_overflow'],
                    pool_recycle=CLOUD_DEFAULTS['pool_recycle'],
                    pool_pre_ping=True,
                    connect_args=connect_args
                    # Note: No isolation_level for read-only users
                )
                
                # Test connection + validate SSL
                with self.engine.connect() as conn:
                    # Check SSL cipher
                    ssl_check = conn.execute(text('SHOW STATUS LIKE "Ssl_cipher"')).fetchone()
                    if ssl_check and ssl_check[1]:
                        logger.info(f"✅ SSL Active: {ssl_check[1]}")
                    else:
                        logger.warning("⚠️  SSL cipher not detected (may be serverless encryption)")
                    
                    # Measure latency
                    latency_ms = self._measure_latency(conn)
                    
                    # Validate read-only permissions (lightweight check)
                    self._validate_readonly_permissions()
                
                logger.info("✅ Connection successful!")
                return  # Success - exit retry loop
                
            except Exception as e:
                if attempt < max_retries:
                    wait_time = retry_delay * (2 ** (attempt - 1))  # Exponential backoff
                    logger.warning(f"⚠️  Connection failed: {e}")
                    logger.info(f"🔄 Retrying in {wait_time}s...")
                    time.sleep(wait_time)
                else:
                    logger.error(f"❌ Failed to connect after {max_retries} attempts")
                    self._print_cloud_troubleshooting(cfg, e)
                    raise
    
    def _measure_latency(self, conn) -> float:
        """Measure network latency to cloud instance"""
        try:
            start = time.time()
            conn.execute(text("SELECT 1"))
            latency_ms = (time.time() - start) * 1000
            
            if latency_ms > SERVERLESS_OPTIMIZATION['latency_threshold_ms']:
                logger.warning(
                    f"⚠️  High latency detected: {latency_ms:.1f}ms "
                    f"(threshold: {SERVERLESS_OPTIMIZATION['latency_threshold_ms']}ms)"
                )
            else:
                logger.info(f"⚡ Latency: {latency_ms:.1f}ms")
            
            return latency_ms
        except Exception:
            return 0.0
    
    def _validate_readonly_permissions(self):
        """Lightweight validation for read-only users (doesn't require SHOW DATABASES)"""
        try:
            with self.engine.connect() as conn:
                # Test 1: Check if we can access the database
                db_name = self.connection_config.get('database')
                result = conn.execute(text("SELECT DATABASE()")).scalar()
                logger.info(f"✅ Database access verified: {result}")
                
                # Test 2: Check if we can list tables (basic read permission)
                try:
                    conn.execute(text(f"SHOW TABLES FROM {db_name}"))
                    logger.info(f"✅ Read permissions verified on {db_name}")
                except Exception as e:
                    logger.warning(f"⚠️  Cannot list tables: {e}")
                    # This is OK - user might only have SELECT on specific tables
                
                logger.info("✅ Permission validation complete (read-only mode)")
        except Exception as e:
            logger.error(f"❌ Permission validation failed: {e}")
            raise PermissionError(f"Cannot access database: {e}")
    
    def validate_credentials(self) -> None:
        """
        Override parent's validate_credentials to use read-only friendly checks.
        Cloud users typically don't have SHOW DATABASES privilege.
        """
        logger.info("☁️  Validating credentials (cloud read-only mode)...")
        self._validate_readonly_permissions()
    
    def _print_cloud_troubleshooting(self, cfg: Dict, error: Exception):
        """Print helpful troubleshooting steps"""
        print("\n" + "=" * 60)
        print("🔧 TROUBLESHOOTING MARIADB SKYSQL CONNECTION")
        print("=" * 60)
        print(f"❌ Error: {error}\n")
        print("📋 Checklist:")
        print("   1. Verify credentials in SkySQL portal")
        print("   2. Check IP whitelist (Security → Firewall)")
        print(f"   3. Confirm SSL certificate exists: {cfg.get('ssl_ca')}")
        print(f"   4. Test connectivity: ping {cfg['host']}")
        print(f"   5. Verify port {cfg['port']} is correct")
        print("   6. Check service status in SkySQL dashboard")
        print("\n💡 Common Issues:")
        print("   - SSL Error: Re-download certificate from portal")
        print("   - Timeout: Check firewall/security groups")
        print("   - Access Denied: Reset password in SkySQL portal")
        print("=" * 60 + "\n")
    
    def extract(self, extraction_request: Dict[str, Any]):
        """Override to log cloud-specific metrics"""
        entity = extraction_request.get("entity")
        
        logger.info(f"☁️  Starting cloud extraction for table: {entity}")
        logger.info(f"☁️  Cloud Mode: Optimized for serverless (max {SERVERLESS_OPTIMIZATION['max_workers']} workers)")
        
        # Call parent extract method
        yield from super().extract(extraction_request)
        
        logger.info(f"☁️  Cloud extraction completed for {entity}")
    
    def extract_with_metadata(self, extraction_request: Dict[str, Any], output_dir: str = "bronze_layer") -> Dict[str, Any]:
        """
        Extract data and save both Parquet file + metadata JSON
        
        Args:
            extraction_request: Standard extraction request dict
            output_dir: Base output directory (default: "bronze_layer")
        
        Returns:
            Dict with extraction summary:
            {
                "success": True/False,
                "table": "table_name",
                "row_count": 1234,
                "data_file": "path/to/data.parquet",
                "metadata_file": "path/to/metadata.json",
                "file_size_kb": 123.45,
                "extraction_time_seconds": 12.34
            }
        """
        import pandas as pd
        import os
        import json
        from datetime import datetime
        import time
        
        entity = extraction_request.get("entity")
        start_time = time.time()
        
        result = {
            "success": False,
            "table": entity,
            "row_count": 0,
            "data_file": None,
            "metadata_file": None,
            "file_size_kb": 0,
            "extraction_time_seconds": 0,
            "error": None
        }
        
        try:
            logger.info(f"☁️  Starting extraction with metadata for: {entity}")
            
            # Extract data
            all_batches = []
            batch_count = 0
            
            for batch_df in self.extract(extraction_request):
                batch_count += 1
                all_batches.append(batch_df)
                logger.info(f"   📦 Batch {batch_count}: {len(batch_df)} rows")
            
            if not all_batches:
                logger.warning(f"⚠️  No data extracted for {entity}")
                result["error"] = "No data returned"
                return result
            
            # Combine batches
            final_df = pd.concat(all_batches, ignore_index=True)
            result["row_count"] = len(final_df)
            
            # Create output directories
            table_dir = os.path.join(output_dir, entity)
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            run_dir = os.path.join(table_dir, timestamp)
            os.makedirs(run_dir, exist_ok=True)
            
            # Save Parquet data
            data_file = os.path.join(run_dir, f"data.parquet")
            final_df.to_parquet(data_file, index=False, compression='snappy')
            result["data_file"] = data_file
            result["file_size_kb"] = os.path.getsize(data_file) / 1024
            
            logger.info(f"✅ Data saved: {data_file}")
            logger.info(f"📊 Rows: {len(final_df):,}, Size: {result['file_size_kb']:.2f} KB")
            
            # Create metadata
            extraction_time = time.time() - start_time
            result["extraction_time_seconds"] = round(extraction_time, 2)
            
            metadata = {
                "source": {
                    "type": "mariadb_cloud",
                    "host": self.connection_config.get("host"),
                    "database": self.connection_config.get("database"),
                    "user": self.connection_config.get("user"),
                    "ssl_enabled": bool(self.connection_config.get("ssl_ca"))
                },
                "extraction": {
                    "timestamp": datetime.now().isoformat(),
                    "table": entity,
                    "mode": extraction_request.get("mode", "full"),
                    "row_count": len(final_df),
                    "column_count": len(final_df.columns),
                    "columns": list(final_df.columns),
                    "batch_count": batch_count,
                    "extraction_time_seconds": result["extraction_time_seconds"]
                },
                "output": {
                    "data_file": data_file,
                    "file_format": "parquet",
                    "compression": "snappy",
                    "file_size_kb": result["file_size_kb"],
                    "file_size_mb": round(result["file_size_kb"] / 1024, 2)
                },
                "statistics": {
                    "rows_per_second": round(len(final_df) / extraction_time, 2) if extraction_time > 0 else 0,
                    "mb_per_second": round((result["file_size_kb"] / 1024) / extraction_time, 2) if extraction_time > 0 else 0
                }
            }
            
            # Add data types
            metadata["schema"] = {
                col: str(dtype) for col, dtype in final_df.dtypes.items()
            }
            
            # Save metadata JSON
            metadata_file = os.path.join(run_dir, "metadata.json")
            with open(metadata_file, 'w') as f:
                json.dump(metadata, f, indent=2)
            
            result["metadata_file"] = metadata_file
            result["success"] = True
            
            logger.info(f"✅ Metadata saved: {metadata_file}")
            logger.info(f"🎉 Extraction complete: {len(final_df):,} rows in {extraction_time:.2f}s")
            
        except Exception as e:
            logger.error(f"❌ Extraction failed: {e}")
            result["error"] = str(e)
            import traceback
            traceback.print_exc()
        
        return result


# ==========================================
# STANDALONE TEST
# ==========================================
if __name__ == "__main__":
    print("🚀 MariaDB SkySQL Cloud Connector - Test Mode")
    print("=" * 60)
    
    # Configuration
    config = {
        "host": "serverless-us-west-2.sysp0000.db1.skysql.com",
        "port": 4020,
        "database": "idp_ingestion_db",
        "user": "analyst_readonly",  # Read-only user
        "password": "$BgyzM&H123EN#B0",
        "ssl_ca": r"C:\Users\Syarifah\OneDrive - M Telecommunication Sdn Bhd\INTERNSHIP\globalsignrootca.pem",
    }
    
    extraction_request = {
        "entity": "customers",
        "mode": "full",
        "enable_parallel": False,  # Start with single-threaded
        "flatten_json": {}
    }
    
    # Test connection
    connector = MariaDBCloudConnector(config)
    
    try:
        connector.connect()
        
        print("\n📊 Starting extraction test...")
        print("-" * 60)
        
        batch_count = 0
        total_rows = 0
        
        for batch_df in connector.extract(extraction_request):
            batch_count += 1
            total_rows += len(batch_df)
            
            print(f"\n✅ Batch {batch_count}: {len(batch_df)} rows")
            print(batch_df.to_string(index=False))
        
        print("-" * 60)
        print(f"\n🎉 Extraction Complete!")
        print(f"   Total Batches: {batch_count}")
        print(f"   Total Rows: {total_rows}")
        
    except Exception as e:
        print(f"\n❌ Test failed: {e}")
        import traceback
        traceback.print_exc()
    
    finally:
        connector.close()
        print("\n✅ Connection closed")
