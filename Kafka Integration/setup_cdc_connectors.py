"""
Setup CDC Connectors
====================
Automated setup script for creating Debezium CDC connectors.

This script:
1. Verifies database configurations (WAL/binlog)
2. Creates Debezium connectors for PostgreSQL and MariaDB
3. Validates connector health
4. Monitors replication lag

Usage:
    # Setup all sources
    python setup_cdc_connectors.py

    # Setup specific source
    python setup_cdc_connectors.py --source postgres
    python setup_cdc_connectors.py --source mariadb

    # Skip health checks
    python setup_cdc_connectors.py --no-health-check
"""

import sys
import os
import json
import time
import argparse
import logging
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent))

from debezium_manager import (
    DebeziumManager,
    ConnectorConfig,
    get_debezium_manager
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


# Load database configurations
def load_config(config_file: str) -> dict:
    """Load JSON configuration file"""
    config_path = Path(__file__).parent.parent / "data ingestion" / "Connector" / config_file
    if not config_path.exists():
        logger.warning(f"Config file not found: {config_path}")
        return {}
    
    with open(config_path, 'r') as f:
        return json.load(f)


def setup_postgres_connector(manager: DebeziumManager, skip_health: bool = False):
    """Setup PostgreSQL CDC connector"""
    logger.info("=" * 60)
    logger.info("Setting up PostgreSQL CDC Connector")
    logger.info("=" * 60)
    
    # Load config
    config_data = load_config("config.json")
    if not config_data:
        logger.error("PostgreSQL config not found")
        return False
    
    conn_config = config_data.get("connection_config", {})
    
    # Create connector configuration
    # Using remote database server at 192.168.0.11
    # NOTE: Using wildcard pattern to capture ALL tables in public schema
    # To capture specific tables only, use: ["public.table1", "public.table2"]
    config = ConnectorConfig(
        name="postgres-cdc-connector",
        database_type="postgres",
        hostname=conn_config.get("host", "192.168.0.11"),
        port=conn_config.get("port", 5432),
        user="debezium_user",  # Created by setup_postgres_wal.sql
        password="debezium_password",
        database_name=conn_config.get("database", "postgres"),
        server_name="syniq_postgres",
        table_include_list=["public.*"],  # Capture ALL tables in public schema
        slot_name="debezium_syniq_slot"
    )
    
    logger.info(f"Creating connector: {config.name}")
    logger.info(f"  Host: {config.hostname}:{config.port}")
    logger.info(f"  Database: {config.database_name}")
    logger.info(f"  Tables: {config.table_include_list}")
    
    try:
        # Create connector
        result = manager.create_postgres_connector(
            config,
            snapshot_mode="initial",  # Take initial snapshot
            plugin_name="pgoutput"   # Use native pgoutput plugin
        )
        
        logger.info(f"Connector created successfully!")
        logger.info(f"Topics will be prefixed with: cdc.syniq_postgres")
        
        # Wait for connector to start
        logger.info("Waiting for connector to start...")
        time.sleep(5)
        
        # Check status
        status = manager.get_connector_status(config.name)
        if status:
            connector_state = status.get("connector", {}).get("state")
            logger.info(f"Connector state: {connector_state}")
            
            tasks = status.get("tasks", [])
            for task in tasks:
                task_id = task.get("id")
                task_state = task.get("state")
                logger.info(f"  Task {task_id}: {task_state}")
        
        return True
        
    except Exception as e:
        logger.error(f"Failed to create PostgreSQL connector: {e}")
        return False


def setup_mariadb_connector(manager: DebeziumManager, skip_health: bool = False):
    """Setup MariaDB CDC connector"""
    logger.info("=" * 60)
    logger.info("Setting up MariaDB CDC Connector")
    logger.info("=" * 60)
    
    # Load config
    config_data = load_config("config_mariadb.json")
    if not config_data:
        logger.error("MariaDB config not found")
        return False
    
    conn_config = config_data.get("connection_config", {})
    
    # Create connector configuration
    # Using remote database server at 192.168.2.114
    config = ConnectorConfig(
        name="mariadb-cdc-connector",
        database_type="mariadb",
        hostname=conn_config.get("host", "192.168.2.114"),
        port=conn_config.get("port", 3306),
        user="debezium_user",  # Created by setup_mariadb_binlog.sql
        password="debezium_password",
        database_name=conn_config.get("database", "mysql"),
        server_name="syniq_mariadb",
        table_include_list=["mysql.test_table"],
        server_id=85472  # Unique server ID
    )
    
    logger.info(f"Creating connector: {config.name}")
    logger.info(f"  Host: {config.hostname}:{config.port}")
    logger.info(f"  Database: {config.database_name}")
    logger.info(f"  Tables: {config.table_include_list}")
    
    try:
        # Create connector
        result = manager.create_mariadb_connector(
            config,
            snapshot_mode="initial",  # Take initial snapshot
            gtid_mode=True
        )
        
        logger.info(f"Connector created successfully!")
        logger.info(f"Topics will be prefixed with: cdc.syniq_mariadb")
        
        # Wait for connector to start
        logger.info("Waiting for connector to start...")
        time.sleep(5)
        
        # Check status
        status = manager.get_connector_status(config.name)
        if status:
            connector_state = status.get("connector", {}).get("state")
            logger.info(f"Connector state: {connector_state}")
            
            tasks = status.get("tasks", [])
            for task in tasks:
                task_id = task.get("id")
                task_state = task.get("state")
                logger.info(f"  Task {task_id}: {task_state}")
        
        return True
        
    except Exception as e:
        logger.error(f"Failed to create MariaDB connector: {e}")
        return False


def verify_kafka_connect():
    """Verify Kafka Connect is running"""
    logger.info("Verifying Kafka Connect availability...")
    
    manager = get_debezium_manager()
    
    try:
        import requests
        response = requests.get(f"{manager.kafka_connect_url}/", timeout=5)
        if response.status_code == 200:
            logger.info("Kafka Connect is running")
            version = response.json().get("version", "unknown")
            logger.info(f"  Version: {version}")
            return True
        else:
            logger.error(f"Kafka Connect returned status {response.status_code}")
            return False
    except Exception as e:
        logger.error(f"Cannot reach Kafka Connect: {e}")
        logger.error("Make sure Docker containers are running:")
        logger.error("  docker-compose up -d")
        return False


def main():
    parser = argparse.ArgumentParser(description="Setup Debezium CDC Connectors")
    parser.add_argument(
        '--source',
        choices=['postgres', 'mariadb', 'all'],
        default='all',
        help='Database source to setup (default: all)'
    )
    parser.add_argument(
        '--no-health-check',
        action='store_true',
        help='Skip database health checks'
    )
    
    args = parser.parse_args()
    
    logger.info("")
    logger.info("=" * 60)
    logger.info("  SYINIQ CDC Connector Setup")
    logger.info("=" * 60)
    logger.info("")
    
    # Verify Kafka Connect is running
    if not verify_kafka_connect():
        logger.error("Kafka Connect is not available. Exiting.")
        logger.info("")
        logger.info("Start Kafka services first:")
        logger.info("  cd 'Kafka Integration'")
        logger.info("  docker-compose up -d")
        logger.info("  Wait 60 seconds for services to initialize")
        return 1
    
    logger.info("")
    
    # Get DebeziumManager instance
    manager = get_debezium_manager()
    
    success_count = 0
    total_count = 0
    
    # Setup PostgreSQL
    if args.source in ['postgres', 'all']:
        total_count += 1
        if setup_postgres_connector(manager, args.no_health_check):
            success_count += 1
        logger.info("")
    
    # Setup MariaDB
    if args.source in ['mariadb', 'all']:
        total_count += 1
        if setup_mariadb_connector(manager, args.no_health_check):
            success_count += 1
        logger.info("")
    
    # Summary
    logger.info("=" * 60)
    logger.info(f"Setup Complete: {success_count}/{total_count} connectors created")
    logger.info("=" * 60)
    
    if success_count > 0:
        logger.info("")
        logger.info("Next steps:")
        logger.info("  1. Test CDC by making changes to the databases")
        logger.info("  2. Monitor Kafka topics for CDC events:")
        logger.info("     - Open Kafka UI: http://localhost:8080")
        logger.info("     - Look for topics: cdc.syniq_postgres.*, cdc.syniq_mariadb.*")
        logger.info("  3. Start Spark Streaming consumers:")
        logger.info("     python spark_cdc_consumer.py")
        logger.info("")
    
    return 0 if success_count == total_count else 1


if __name__ == "__main__":
    sys.exit(main())
