# filepath: connectors/mariadb_cloud_connector.py
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
    from utils.config_loader import load_config
    from connectors import MariaDBCloudConnector
    
    config = load_config("config/db_config.yaml")
    connector = MariaDBCloudConnector(config["mariadb_cloud"])
    connector.connect()
    
    for batch in connector.extract({"entity": "customers", "mode": "full"}):
        df = batch["data"]
        print(f"Got {len(df)} rows")
"""

import logging
import time
from typing import Dict, Any, Tuple
from sqlalchemy import create_engine, text

from .mariadb_connector import MariaDBConnector, ExtractionOptimizer

logger = logging.getLogger(__name__)


# ==========================================
# CLOUD DEFAULTS
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
    "max_workers": 2,                  # Force low parallelism
    "chunk_size_multiplier": 2.0,      # Larger chunks (reduce roundtrips)
    "latency_threshold_ms": 100,       # Warn if latency > 100ms
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
    
    def __init__(self, connection_config: Dict[str, Any]):
        """Initialize with cloud-specific defaults"""
        
        # Validate SSL configuration (mandatory for cloud)
        if not connection_config.get("ssl", {}).get("ca"):
            raise ValueError(
                "❌ SSL certificate is REQUIRED for MariaDB SkySQL.\n"
                "Download from: https://supplychain.mariadb.com/skysql-chain.pem\n"
                "Add to config: 'ssl': {'ca': '/path/to/certificate.pem'}"
            )
        
        # Inject cloud defaults
        connection_config.setdefault("connect_timeout", CLOUD_DEFAULTS["connect_timeout"])
        connection_config.setdefault("read_timeout", CLOUD_DEFAULTS["read_timeout"])
        connection_config.setdefault("pool_size", CLOUD_DEFAULTS["pool_size"])
        connection_config.setdefault("max_overflow", CLOUD_DEFAULTS["max_overflow"])
        connection_config.setdefault("pool_recycle", CLOUD_DEFAULTS["pool_recycle"])
        
        # Ensure SSL verification is enabled
        if "ssl" not in connection_config:
            connection_config["ssl"] = {}
        connection_config["ssl"].setdefault("verify_cert", True)
        
        connection_config["is_cloud"] = True  # Flag for optimization logic
        
        # Initialize parent
        super().__init__(connection_config)
        
        # Override optimizer with cloud version
        self.optimizer = CloudExtractionOptimizer()
        
        logger.info(f"☁️  MariaDB Cloud Connector initialized for {connection_config.get('host')}")
    
    def connect(self) -> None:
        """Cloud-aware connection with retry logic"""
        cfg = self.connection_config
        
        # Build connection string
        port = cfg.get('port', 3306)
        conn_str = (
            f"mysql+pymysql://{cfg['user']}:{cfg['password']}"
            f"@{cfg['host']}:{port}/{cfg['database']}"
        )
        
        # 🔥 CLOUD-SPECIFIC CONNECT ARGS
        connect_args = {
            'connect_timeout': cfg.get('connect_timeout', CLOUD_DEFAULTS['connect_timeout']),
            'read_timeout': cfg.get('read_timeout', CLOUD_DEFAULTS['read_timeout']),
        }
        
        # Mandatory SSL for cloud
        if cfg.get("ssl", {}).get("ca"):
            connect_args['ssl'] = {
                'ca': cfg["ssl"]["ca"],
                'check_hostname': cfg.get("ssl", {}).get("verify_cert", True)
            }
            logger.info(f"🔒 SSL Certificate: {cfg['ssl']['ca']}")
        
        # Retry logic for cloud connectivity issues
        max_retries = CLOUD_DEFAULTS['max_retries']
        retry_delay = CLOUD_DEFAULTS['retry_delay']
        
        for attempt in range(1, max_retries + 1):
            try:
                logger.info(f"🔌 Connecting to MariaDB SkySQL at {cfg['host']}:{port} (Attempt {attempt}/{max_retries})")
                
                self.engine = create_engine(
                    conn_str,
                    pool_size=cfg.get('pool_size', CLOUD_DEFAULTS['pool_size']),
                    max_overflow=cfg.get('max_overflow', CLOUD_DEFAULTS['max_overflow']),
                    pool_recycle=cfg.get('pool_recycle', CLOUD_DEFAULTS['pool_recycle']),
                    pool_pre_ping=True,
                    connect_args=connect_args
                    # Note: No isolation_level setting for read-only users
                )
                
                # Test connection + validate SSL
                with self.engine.connect() as conn:
                    # Check SSL cipher
                    ssl_check = conn.execute(text('SHOW STATUS LIKE "Ssl_cipher"')).fetchone()
                    if ssl_check and ssl_check[1]:
                        logger.info(f"✅ SSL Active: {ssl_check[1]}")
                    else:
                        logger.warning("⚠️  SSL cipher not detected (may be serverless encryption)")
                    
                    # Get version
                    result = conn.execute(text("SELECT VERSION()")).fetchone()
                    version = result[0] if result else "Unknown"
                    logger.info(f"✅ Connected to MariaDB SkySQL {version}")
                    
                    # Measure latency
                    latency_ms = self._measure_latency(conn)
                    
                    # Validate permissions
                    self._validate_cloud_permissions()
                
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
    
    def _validate_cloud_permissions(self):
        """Lightweight validation for read-only cloud users"""
        try:
            with self.engine.connect() as conn:
                # Check database access
                db_name = self.connection_config.get('database')
                result = conn.execute(text("SELECT DATABASE()")).scalar()
                logger.info(f"✅ Database access verified: {result}")
                
                # Check if we can list tables
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
        """Override parent to use cloud-specific validation"""
        self._validate_cloud_permissions()
    
    def _print_cloud_troubleshooting(self, cfg: Dict, error: Exception):
        """Print helpful troubleshooting steps"""
        print("\n" + "=" * 60)
        print("🔧 TROUBLESHOOTING MARIADB SKYSQL CONNECTION")
        print("=" * 60)
        print(f"❌ Error: {error}\n")
        print("📋 Checklist:")
        print("   1. Verify credentials in SkySQL portal")
        print("   2. Check IP whitelist (Security → Firewall)")
        print(f"   3. Confirm SSL certificate exists: {cfg.get('ssl', {}).get('ca')}")
        print(f"   4. Test connectivity: ping {cfg['host']}")
        print(f"   5. Verify port {cfg['port']} is correct")
        print("   6. Check service status in SkySQL dashboard")
        print("\n💡 Common Issues:")
        print("   - SSL Error: Re-download certificate from portal")
        print("   - Timeout: Check firewall/security groups")
        print("   - Access Denied: Reset password in SkySQL portal")
        print("=" * 60 + "\n")
    
    def extract(self, extraction_plan: Dict[str, Any]):
        """Override to log cloud-specific metrics"""
        entity = extraction_plan.get("entity")
        
        logger.info(f"☁️  Starting cloud extraction for table: {entity}")
        logger.info(f"☁️  Cloud Mode: Optimized for serverless (max {SERVERLESS_OPTIMIZATION['max_workers']} workers)")
        
        # Call parent extract method
        yield from super().extract(extraction_plan)
        
        logger.info(f"☁️  Cloud extraction completed for {entity}")
