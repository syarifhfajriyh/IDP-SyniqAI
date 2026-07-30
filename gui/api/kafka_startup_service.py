"""
Kafka Startup Service
=====================
Automatically starts and manages Kafka + Debezium when backend starts.

Features:
- Auto-starts Kafka docker-compose if not running
- Waits for Kafka Connect to be ready
- Verifies Debezium connectors are installed
- Creates CDC connectors automatically during ingestion
"""

import os
import sys
import time
import logging
import subprocess
from pathlib import Path
from typing import Dict, Optional, Tuple
import requests

logger = logging.getLogger(__name__)


class KafkaStartupService:
    """Manages Kafka + Debezium lifecycle"""
    
    def __init__(self):
        # Find Kafka Integration folder
        base_path = Path(__file__).parent.parent.parent
        self.kafka_path = base_path / "Kafka Integration"
        self.docker_compose_file = self.kafka_path / "docker-compose.yml"
        
        self.kafka_connect_url = "http://localhost:8083"
        self.kafka_bootstrap = "kafka:29092"  # Docker internal
        
        logger.info(f"KafkaStartupService initialized (path: {self.kafka_path})")
    
    def is_kafka_running(self) -> bool:
        """Check if Kafka Connect is accessible"""
        try:
            response = requests.get(f"{self.kafka_connect_url}/", timeout=2)
            return response.status_code == 200
        except:
            return False
    
    def is_docker_running(self) -> bool:
        """Check if Docker Desktop is running"""
        try:
            result = subprocess.run(
                ["docker", "ps"],
                capture_output=True,
                text=True,
                timeout=5
            )
            return result.returncode == 0
        except:
            return False
    
    def start_kafka_stack(self) -> Tuple[bool, str]:
        """Start Kafka + Debezium using docker-compose"""
        
        if not self.docker_compose_file.exists():
            return False, f"docker-compose.yml not found at {self.docker_compose_file}"
        
        try:
            logger.info("Starting Kafka stack with docker-compose...")
            
            # Change to Kafka Integration directory
            os.chdir(self.kafka_path)
            
            # Start docker-compose
            result = subprocess.run(
                ["docker-compose", "up", "-d"],
                capture_output=True,
                text=True,
                timeout=60
            )
            
            if result.returncode != 0:
                return False, f"docker-compose failed: {result.stderr}"
            
            logger.info("Docker containers started, waiting for Kafka Connect...")
            
            # Wait for Kafka Connect to be ready (max 120 seconds)
            for i in range(24):
                time.sleep(5)
                if self.is_kafka_running():
                    logger.info(f"✓ Kafka Connect ready after {(i+1)*5}s")
                    
                    # Wait extra 30s for Debezium installation
                    logger.info("Waiting for Debezium connectors to install...")
                    time.sleep(30)
                    
                    return True, "Kafka stack started successfully"
                
                logger.info(f"Waiting for Kafka Connect... ({i+1}/24)")
            
            return False, "Kafka Connect did not become ready within 120 seconds"
            
        except Exception as e:
            logger.error(f"Failed to start Kafka stack: {e}")
            return False, str(e)
    
    def verify_debezium_installed(self) -> Tuple[bool, Dict]:
        """Check if Debezium connectors are installed"""
        try:
            response = requests.get(f"{self.kafka_connect_url}/connector-plugins", timeout=5)
            
            if response.status_code != 200:
                return False, {"error": "Failed to query connector plugins"}
            
            plugins = response.json()
            plugin_classes = [p.get('class', '') for p in plugins]
            
            has_postgres = any('PostgresConnector' in c for c in plugin_classes)
            has_mysql = any('MySqlConnector' in c for c in plugin_classes)
            
            return (has_postgres and has_mysql), {
                "postgres_installed": has_postgres,
                "mysql_installed": has_mysql,
                "total_plugins": len(plugins)
            }
            
        except Exception as e:
            logger.error(f"Failed to verify Debezium: {e}")
            return False, {"error": str(e)}
    
    def create_cdc_connector(
        self, 
        source_type: str, 
        connection_config: Dict,
        table_name: str
    ) -> Tuple[bool, str]:
        """
        Automatically enable CDC for a table by updating the existing connector
        
        Strategy:
        - Use shared connector (postgres-cdc-connector, mariadb-cdc-connector)
        - Add table to existing table.include.list instead of creating new connectors
        - Only create new connector if none exists for this database
        
        Args:
            source_type: 'postgres', 'mariadb', or 'mariadb_cloud'
            connection_config: Database connection details from ingestion
            table_name: Table/entity name to monitor (e.g., 'loan_applications')
        """
        
        if source_type not in ['postgres', 'mariadb', 'mariadb_cloud']:
            return False, f"CDC not supported for {source_type}"
        
        try:
            # Use shared connector name for the database type
            if source_type == 'postgres':
                main_connector = "postgres-cdc-connector"
                full_table_name = f"public.{table_name}" if '.' not in table_name else table_name
            else:  # mariadb or mariadb_cloud
                main_connector = "mariadb-cdc-connector"
                db_name = connection_config.get('database', 'mysql')
                full_table_name = f"{db_name}.{table_name}" if '.' not in table_name else table_name
            
            # Check if main connector exists
            existing = requests.get(
                f"{self.kafka_connect_url}/connectors/{main_connector}",
                timeout=5
            )
            
            if existing.status_code == 200:
                # Connector exists - UPDATE table list
                logger.info(f"Found existing connector '{main_connector}', adding table '{full_table_name}'")
                
                current_config = existing.json().get('config', {})
                current_tables = current_config.get('table.include.list', '')
                
                # Parse existing tables
                existing_table_list = [t.strip() for t in current_tables.split(',') if t.strip()]
                
                # Check if table already monitored
                if full_table_name in existing_table_list:
                    logger.info(f"Table '{full_table_name}' already in CDC monitoring")
                    return True, f"CDC already enabled for {table_name}"
                
                # Add new table to list
                existing_table_list.append(full_table_name)
                new_table_list = ','.join(existing_table_list)
                
                # Update connector config
                updated_config = current_config.copy()
                updated_config['table.include.list'] = new_table_list
                
                logger.info(f"Updating table list: {new_table_list}")
                
                update_response = requests.put(
                    f"{self.kafka_connect_url}/connectors/{main_connector}/config",
                    json=updated_config,
                    headers={'Content-Type': 'application/json'},
                    timeout=10
                )
                
                if update_response.status_code in [200, 201]:
                    logger.info(f"✓ CDC connector updated - now monitoring {len(existing_table_list)} tables")
                    return True, f"CDC enabled for {table_name} (added to {main_connector})"
                else:
                    error_msg = update_response.text
                    logger.error(f"Failed to update connector: {error_msg}")
                    return False, f"Update failed: {error_msg}"
                    
            else:
                # Connector doesn't exist - CREATE it with this table
                logger.info(f"No connector found, creating '{main_connector}' for table '{full_table_name}'")
                
                if source_type == 'postgres':
                    connector_config = self._build_postgres_connector(
                        main_connector.replace('-cdc-connector', ''), 
                        connection_config, 
                        full_table_name
                    )
                else:
                    connector_config = self._build_mariadb_connector(
                        main_connector.replace('-cdc-connector', ''), 
                        connection_config, 
                        full_table_name
                    )
                
                # Override name to use main connector
                connector_config['name'] = main_connector
                
                response = requests.post(
                    f"{self.kafka_connect_url}/connectors",
                    json=connector_config,
                    headers={'Content-Type': 'application/json'},
                    timeout=10
                )
                
                if response.status_code in [200, 201]:
                    logger.info(f"✓ CDC connector '{main_connector}' created successfully")
                    
                    time.sleep(2)
                    health_status = self._check_connector_health(main_connector)
                    
                    if health_status['state'] == 'RUNNING':
                        return True, f"CDC enabled for {table_name} (new connector created)"
                    elif health_status['state'] == 'FAILED':
                        error_detail = health_status.get('error', 'Unknown error')
                        logger.warning(f"⚠ Connector created but failed: {error_detail}")
                        return False, f"Connector failed: {error_detail}"
                    else:
                        return True, f"CDC enabled for {table_name} (state: {health_status['state']})"
                else:
                    error_msg = response.json().get('message', response.text)
                    logger.error(f"Failed to create connector: {error_msg}")
                    return False, error_msg
                
        except Exception as e:
            logger.error(f"Exception creating CDC connector: {e}")
            return False, str(e)
    
    def _check_connector_health(self, connector_name: str) -> Dict:
        """Check the health status of a connector"""
        try:
            response = requests.get(
                f"{self.kafka_connect_url}/connectors/{connector_name}/status",
                timeout=5
            )
            
            if response.status_code == 200:
                status = response.json()
                connector_state = status.get('connector', {}).get('state', 'UNKNOWN')
                
                # Check if any tasks failed
                tasks = status.get('tasks', [])
                task_errors = [t.get('trace') for t in tasks if t.get('state') == 'FAILED']
                
                return {
                    'state': connector_state,
                    'error': task_errors[0] if task_errors else None,
                    'tasks': len(tasks)
                }
            else:
                return {'state': 'UNKNOWN', 'error': 'Status check failed'}
                
        except Exception as e:
            logger.warning(f"Failed to check connector health: {e}")
            return {'state': 'UNKNOWN', 'error': str(e)}
    
    def _build_postgres_connector(
        self, 
        name: str, 
        config: Dict, 
        table: str
    ) -> Dict:
        """Build PostgreSQL CDC connector configuration"""
        
        return {
            "name": name,
            "config": {
                "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
                "database.hostname": config.get('host', 'localhost'),
                "database.port": str(config.get('port', 5432)),
                "database.user": config.get('user', 'postgres'),
                "database.password": config.get('password', ''),
                "database.dbname": config.get('database', 'postgres'),
                "database.server.name": f"syniq_{name}",
                "table.include.list": table,
                "plugin.name": "pgoutput",
                "slot.name": f"debezium_{name}_slot",
                "publication.name": f"dbz_{name}_publication",
                "schema.history.internal.kafka.bootstrap.servers": self.kafka_bootstrap,
                "schema.history.internal.kafka.topic": f"schema-changes.{name}",
                "snapshot.mode": "initial",
                "tombstones.on.delete": "false"
            }
        }
    
    def _build_mariadb_connector(
        self, 
        name: str, 
        config: Dict, 
        table: str
    ) -> Dict:
        """Build MariaDB/MySQL CDC connector configuration"""
        
        return {
            "name": name,
            "config": {
                "connector.class": "io.debezium.connector.mysql.MySqlConnector",
                "database.hostname": config.get('host', 'localhost'),
                "database.port": str(config.get('port', 3306)),
                "database.user": config.get('user', 'root'),
                "database.password": config.get('password', ''),
                "database.server.id": str(hash(name) % 100000),  # Unique server ID
                "database.server.name": f"syniq_{name}",
                "table.include.list": table,
                "database.history.kafka.bootstrap.servers": self.kafka_bootstrap,
                "database.history.kafka.topic": f"schema-changes.{name}",
                "snapshot.mode": "initial",
                "tombstones.on.delete": "false",
                "include.schema.changes": "false"
            }
        }
    
    def auto_startup(self) -> Dict:
        """
        Main startup routine - called when backend starts
        
        Returns:
            Status dict with startup results
        """
        
        result = {
            "kafka_started": False,
            "debezium_verified": False,
            "message": "",
            "warnings": []
        }
        
        # Check Docker
        if not self.is_docker_running():
            result["message"] = "Docker Desktop not running - Kafka services unavailable"
            result["warnings"].append("Start Docker Desktop to enable Kafka")
            logger.warning(result["message"])
            return result
        
        # Check if Kafka already running
        if self.is_kafka_running():
            logger.info("✓ Kafka already running")
            result["kafka_started"] = True
            result["message"] = "Kafka already running"
        else:
            # Start Kafka
            logger.info("Kafka not running, starting docker-compose...")
            success, message = self.start_kafka_stack()
            
            if not success:
                result["message"] = f"Failed to start Kafka: {message}"
                result["warnings"].append("CDC features will be unavailable")
                logger.error(result["message"])
                return result
            
            result["kafka_started"] = True
            result["message"] = message
        
        # Verify Debezium
        is_installed, details = self.verify_debezium_installed()
        result["debezium_verified"] = is_installed
        result["debezium_details"] = details
        
        if not is_installed:
            result["warnings"].append("Debezium connectors not fully installed - CDC may be limited")
            logger.warning(f"Debezium verification failed: {details}")
        else:
            logger.info(f"✓ Debezium verified: {details}")
        
        return result


# Singleton instance
_kafka_service: Optional[KafkaStartupService] = None

def get_kafka_startup_service() -> KafkaStartupService:
    """Get singleton instance"""
    global _kafka_service
    if _kafka_service is None:
        _kafka_service = KafkaStartupService()
    return _kafka_service
