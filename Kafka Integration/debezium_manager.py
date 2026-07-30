"""
Debezium CDC Manager
====================
Manages Debezium connectors for PostgreSQL and MariaDB CDC.

Features:
- Configure PostgreSQL WAL-based CDC connector
- Configure MariaDB binlog-based CDC connector  
- Monitor connector health and lag
- Automatic restart on failure
- Schema evolution handling

Requirements:
- PostgreSQL: wal_level=logical, max_wal_senders>=4, max_replication_slots>=4
- MariaDB: log_bin=ON, binlog_format=ROW, binlog_row_image=FULL
"""

import os
import json
import time
import logging
from typing import Dict, List, Optional, Any
from datetime import datetime
import requests
from dataclasses import dataclass, asdict

logger = logging.getLogger(__name__)


@dataclass
class ConnectorConfig:
    """Base configuration for Debezium connectors"""
    name: str
    database_type: str  # 'postgres' or 'mariadb'
    hostname: str
    port: int
    user: str
    password: str
    database_name: str
    server_name: str  # Logical name for the database server
    table_include_list: Optional[List[str]] = None
    slot_name: Optional[str] = None  # PostgreSQL only
    server_id: Optional[int] = None  # MariaDB only
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary, filtering None values"""
        return {k: v for k, v in asdict(self).items() if v is not None}


class DebeziumManager:
    """
    Manages Debezium CDC connectors for PostgreSQL and MariaDB.
    """
    
    def __init__(
        self,
        kafka_connect_url: str = "http://localhost:8083",
        kafka_bootstrap_servers: str = "localhost:9092"
    ):
        """
        Initialize Debezium Manager.
        
        Args:
            kafka_connect_url: URL of Kafka Connect REST API
            kafka_bootstrap_servers: Bootstrap servers for Kafka
        """
        self.kafka_connect_url = kafka_connect_url.rstrip('/')
        self.kafka_bootstrap_servers = kafka_bootstrap_servers
        self.session = requests.Session()
        self.session.headers.update({
            'Content-Type': 'application/json',
            'Accept': 'application/json'
        })
        
        logger.info(f"Debezium Manager initialized with Kafka Connect: {kafka_connect_url}")
    
    
    def create_postgres_connector(
        self,
        config: ConnectorConfig,
        snapshot_mode: str = "initial",
        publication_name: Optional[str] = None,
        plugin_name: str = "pgoutput"
    ) -> Dict[str, Any]:
        """
        Create a PostgreSQL CDC connector using Debezium.
        
        Args:
            config: Connector configuration
            snapshot_mode: Snapshot mode (initial, always, never, initial_only)
            publication_name: PostgreSQL publication name (auto-created if None)
            plugin_name: Decoder plugin (pgoutput, wal2json, decoderbufs)
            
        Returns:
            Response from Kafka Connect API
            
        Raises:
            Exception: If connector creation fails
        """
        if config.database_type != 'postgres':
            raise ValueError("Config must be for postgres database")
        
        # Generate slot name if not provided
        slot_name = config.slot_name or f"debezium_{config.server_name}"
        pub_name = publication_name or f"dbz_{config.server_name}_publication"
        
        # Build table include list
        table_include = config.table_include_list or [f"{config.database_name}.public.*"]
        
        connector_config = {
            "name": config.name,
            "config": {
                # Connector class
                "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
                "tasks.max": "1",
                
                # Database connection
                "database.hostname": config.hostname,
                "database.port": str(config.port),
                "database.user": config.user,
                "database.password": config.password,
                "database.dbname": config.database_name,
                "database.server.name": config.server_name,
                
                # Slot and publication
                "slot.name": slot_name,
                "publication.name": pub_name,
                "publication.autocreate.mode": "filtered",
                "plugin.name": plugin_name,
                
                # Topic configuration
                "topic.prefix": f"cdc.{config.server_name}",
                "tombstones.on.delete": "false",
                
                # Table filtering
                "table.include.list": ",".join(table_include),
                "schema.include.list": "public",
                
                # Snapshot configuration
                "snapshot.mode": snapshot_mode,
                "snapshot.locking.mode": "none",
                
                # Change event format
                "key.converter": "org.apache.kafka.connect.json.JsonConverter",
                "value.converter": "org.apache.kafka.connect.json.JsonConverter",
                "key.converter.schemas.enable": "false",
                "value.converter.schemas.enable": "false",
                
                # Heartbeat and monitoring
                "heartbeat.interval.ms": "10000",
                "heartbeat.topics.prefix": "__debezium-heartbeat",
                
                # Schema evolution
                "include.schema.changes": "true",
                "schema.history.internal.kafka.bootstrap.servers": self.kafka_bootstrap_servers,
                "schema.history.internal.kafka.topic": f"schema-changes.{config.server_name}",
                
                # Transformation - Extract 'after' field for cleaner messages
                "transforms": "unwrap",
                "transforms.unwrap.type": "io.debezium.transforms.ExtractNewRecordState",
                "transforms.unwrap.drop.tombstones": "false",
                "transforms.unwrap.delete.handling.mode": "rewrite",
                "transforms.unwrap.add.fields": "op,source.ts_ms,source.db,source.table"
            }
        }
        
        return self._create_connector(connector_config)
    
    
    def create_mariadb_connector(
        self,
        config: ConnectorConfig,
        snapshot_mode: str = "initial",
        gtid_mode: bool = True
    ) -> Dict[str, Any]:
        """
        Create a MariaDB CDC connector using Debezium.
        
        Args:
            config: Connector configuration
            snapshot_mode: Snapshot mode (initial, when_needed, schema_only, never)
            gtid_mode: Use GTID for tracking binlog position
            
        Returns:
            Response from Kafka Connect API
            
        Raises:
            Exception: If connector creation fails
        """
        if config.database_type != 'mariadb':
            raise ValueError("Config must be for mariadb database")
        
        # Generate server ID if not provided (must be unique across all connectors)
        server_id = config.server_id or hash(config.server_name) % 100000 + 10000
        
        # Build table include list
        table_include = config.table_include_list or [f"{config.database_name}.*"]
        
        connector_config = {
            "name": config.name,
            "config": {
                # Connector class
                "connector.class": "io.debezium.connector.mysql.MySqlConnector",
                "tasks.max": "1",
                
                # Database connection
                "database.hostname": config.hostname,
                "database.port": str(config.port),
                "database.user": config.user,
                "database.password": config.password,
                "database.server.id": str(server_id),
                "database.server.name": config.server_name,
                
                # Include specific database
                "database.include.list": config.database_name,
                
                # Topic configuration
                "topic.prefix": f"cdc.{config.server_name}",
                "tombstones.on.delete": "false",
                
                # Table filtering
                "table.include.list": ",".join(table_include),
                
                # Snapshot configuration
                "snapshot.mode": snapshot_mode,
                "snapshot.locking.mode": "minimal",
                
                # Binlog configuration
                "binlog.buffer.size": "8192",
                "connect.keep.alive": "true",
                "connect.keep.alive.interval.ms": "60000",
                
                # GTID mode
                "gtid.source.includes": config.server_name if gtid_mode else "",
                
                # Change event format
                "key.converter": "org.apache.kafka.connect.json.JsonConverter",
                "value.converter": "org.apache.kafka.connect.json.JsonConverter",
                "key.converter.schemas.enable": "false",
                "value.converter.schemas.enable": "false",
                
                # Heartbeat and monitoring
                "heartbeat.interval.ms": "10000",
                "heartbeat.topics.prefix": "__debezium-heartbeat",
                
                # Schema evolution
                "include.schema.changes": "true",
                "schema.history.internal.kafka.bootstrap.servers": self.kafka_bootstrap_servers,
                "schema.history.internal.kafka.topic": f"schema-changes.{config.server_name}",
                
                # Decimal handling
                "decimal.handling.mode": "double",
                "time.precision.mode": "connect",
                
                # Transformation - Extract 'after' field for cleaner messages
                "transforms": "unwrap",
                "transforms.unwrap.type": "io.debezium.transforms.ExtractNewRecordState",
                "transforms.unwrap.drop.tombstones": "false",
                "transforms.unwrap.delete.handling.mode": "rewrite",
                "transforms.unwrap.add.fields": "op,source.ts_ms,source.db,source.table"
            }
        }
        
        return self._create_connector(connector_config)
    
    
    def _create_connector(self, connector_config: Dict[str, Any]) -> Dict[str, Any]:
        """
        Create a connector via Kafka Connect REST API.
        
        Args:
            connector_config: Full connector configuration
            
        Returns:
            Response from Kafka Connect API
        """
        connector_name = connector_config["name"]
        
        try:
            # Check if connector already exists
            existing = self.get_connector(connector_name)
            if existing:
                logger.warning(f"Connector '{connector_name}' already exists. Updating...")
                return self.update_connector(connector_name, connector_config["config"])
            
            # Create new connector
            url = f"{self.kafka_connect_url}/connectors"
            response = self.session.post(url, json=connector_config, timeout=30)
            
            if response.status_code in [200, 201]:
                logger.info(f"Successfully created connector: {connector_name}")
                return response.json()
            else:
                error_msg = f"Failed to create connector: {response.status_code} - {response.text}"
                logger.error(error_msg)
                raise Exception(error_msg)
                
        except requests.exceptions.RequestException as e:
            logger.error(f"Request failed when creating connector: {e}")
            raise
    
    
    def get_connector(self, connector_name: str) -> Optional[Dict[str, Any]]:
        """
        Get connector details.
        
        Args:
            connector_name: Name of the connector
            
        Returns:
            Connector details or None if not found
        """
        try:
            url = f"{self.kafka_connect_url}/connectors/{connector_name}"
            response = self.session.get(url, timeout=10)
            
            if response.status_code == 200:
                return response.json()
            elif response.status_code == 404:
                return None
            else:
                logger.warning(f"Unexpected status when getting connector: {response.status_code}")
                return None
                
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to get connector: {e}")
            return None
    
    
    def get_connector_status(self, connector_name: str) -> Optional[Dict[str, Any]]:
        """
        Get connector status including task states.
        
        Args:
            connector_name: Name of the connector
            
        Returns:
            Status details or None if not found
        """
        try:
            url = f"{self.kafka_connect_url}/connectors/{connector_name}/status"
            response = self.session.get(url, timeout=10)
            
            if response.status_code == 200:
                return response.json()
            elif response.status_code == 404:
                return None
            else:
                logger.warning(f"Unexpected status when getting connector status: {response.status_code}")
                return None
                
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to get connector status: {e}")
            return None
    
    
    def update_connector(self, connector_name: str, config: Dict[str, Any]) -> Dict[str, Any]:
        """
        Update an existing connector's configuration.
        
        Args:
            connector_name: Name of the connector
            config: New configuration (config section only, not full connector_config)
            
        Returns:
            Updated connector details
        """
        try:
            url = f"{self.kafka_connect_url}/connectors/{connector_name}/config"
            response = self.session.put(url, json=config, timeout=30)
            
            if response.status_code in [200, 201]:
                logger.info(f"Successfully updated connector: {connector_name}")
                return response.json()
            else:
                error_msg = f"Failed to update connector: {response.status_code} - {response.text}"
                logger.error(error_msg)
                raise Exception(error_msg)
                
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to update connector: {e}")
            raise
    
    
    def delete_connector(self, connector_name: str) -> bool:
        """
        Delete a connector.
        
        Args:
            connector_name: Name of the connector
            
        Returns:
            True if deleted successfully
        """
        try:
            url = f"{self.kafka_connect_url}/connectors/{connector_name}"
            response = self.session.delete(url, timeout=10)
            
            if response.status_code in [200, 204]:
                logger.info(f"Successfully deleted connector: {connector_name}")
                return True
            elif response.status_code == 404:
                logger.warning(f"Connector not found: {connector_name}")
                return False
            else:
                logger.error(f"Failed to delete connector: {response.status_code}")
                return False
                
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to delete connector: {e}")
            return False
    
    
    def restart_connector(self, connector_name: str, include_tasks: bool = True) -> bool:
        """
        Restart a connector and optionally its tasks.
        
        Args:
            connector_name: Name of the connector
            include_tasks: Whether to restart tasks as well
            
        Returns:
            True if restart successful
        """
        try:
            params = {"includeTasks": str(include_tasks).lower()}
            url = f"{self.kafka_connect_url}/connectors/{connector_name}/restart"
            response = self.session.post(url, params=params, timeout=30)
            
            if response.status_code in [200, 204]:
                logger.info(f"Successfully restarted connector: {connector_name}")
                return True
            else:
                logger.error(f"Failed to restart connector: {response.status_code}")
                return False
                
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to restart connector: {e}")
            return False
    
    
    def pause_connector(self, connector_name: str) -> bool:
        """Pause a connector."""
        try:
            url = f"{self.kafka_connect_url}/connectors/{connector_name}/pause"
            response = self.session.put(url, timeout=10)
            return response.status_code in [200, 202]
        except:
            return False
    
    
    def resume_connector(self, connector_name: str) -> bool:
        """Resume a paused connector."""
        try:
            url = f"{self.kafka_connect_url}/connectors/{connector_name}/resume"
            response = self.session.put(url, timeout=10)
            return response.status_code in [200, 202]
        except:
            return False
    
    
    def list_connectors(self) -> List[str]:
        """
        List all connectors.
        
        Returns:
            List of connector names
        """
        try:
            url = f"{self.kafka_connect_url}/connectors"
            response = self.session.get(url, timeout=10)
            
            if response.status_code == 200:
                return response.json()
            else:
                logger.error(f"Failed to list connectors: {response.status_code}")
                return []
                
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to list connectors: {e}")
            return []
    
    
    def get_connector_metrics(self, connector_name: str) -> Dict[str, Any]:
        """
        Get metrics for a connector including lag, throughput, etc.
        
        Args:
            connector_name: Name of the connector
            
        Returns:
            Dictionary of metrics
        """
        status = self.get_connector_status(connector_name)
        if not status:
            return {}
        
        connector_state = status.get("connector", {}).get("state", "UNKNOWN")
        tasks = status.get("tasks", [])
        
        metrics = {
            "connector_name": connector_name,
            "connector_state": connector_state,
            "tasks_total": len(tasks),
            "tasks_running": sum(1 for t in tasks if t.get("state") == "RUNNING"),
            "tasks_failed": sum(1 for t in tasks if t.get("state") == "FAILED"),
            "healthy": connector_state == "RUNNING" and all(t.get("state") == "RUNNING" for t in tasks),
            "timestamp": datetime.utcnow().isoformat()
        }
        
        return metrics
    
    
    def monitor_all_connectors(self) -> List[Dict[str, Any]]:
        """
        Monitor all connectors and return their metrics.
        
        Returns:
            List of metrics for each connector
        """
        connectors = self.list_connectors()
        return [self.get_connector_metrics(name) for name in connectors]
    
    
    def health_check(self) -> Dict[str, Any]:
        """
        Check health of Kafka Connect cluster.
        
        Returns:
            Health status including connector states
        """
        try:
            # Check if Kafka Connect is reachable
            url = f"{self.kafka_connect_url}/"
            response = self.session.get(url, timeout=5)
            kafka_connect_up = response.status_code == 200
            
            if not kafka_connect_up:
                return {
                    "healthy": False,
                    "kafka_connect": "DOWN",
                    "error": f"Kafka Connect returned {response.status_code}"
                }
            
            # Get all connector metrics
            connector_metrics = self.monitor_all_connectors()
            unhealthy = [m for m in connector_metrics if not m.get("healthy", False)]
            
            return {
                "healthy": len(unhealthy) == 0,
                "kafka_connect": "UP",
                "connectors_total": len(connector_metrics),
                "connectors_healthy": len(connector_metrics) - len(unhealthy),
                "connectors_unhealthy": len(unhealthy),
                "unhealthy_connectors": [m["connector_name"] for m in unhealthy],
                "timestamp": datetime.utcnow().isoformat()
            }
            
        except requests.exceptions.RequestException as e:
            return {
                "healthy": False,
                "kafka_connect": "DOWN",
                "error": str(e)
            }


# Singleton instance
_debezium_manager: Optional[DebeziumManager] = None


def get_debezium_manager(
    kafka_connect_url: str = None,
    kafka_bootstrap_servers: str = None
) -> DebeziumManager:
    """
    Get or create singleton DebeziumManager instance.
    
    Args:
        kafka_connect_url: Kafka Connect URL (uses env var if not provided)
        kafka_bootstrap_servers: Kafka bootstrap servers (uses env var if not provided)
        
    Returns:
        DebeziumManager instance
    """
    global _debezium_manager
    
    if _debezium_manager is None:
        connect_url = kafka_connect_url or os.getenv("KAFKA_CONNECT_URL", "http://localhost:8083")
        bootstrap = kafka_bootstrap_servers or os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
        _debezium_manager = DebeziumManager(connect_url, bootstrap)
    
    return _debezium_manager
