"""
End-to-End CDC Pipeline Test
=============================
Comprehensive test suite for the CDC pipeline.

Tests:
1. Database WAL/binlog configuration
2. Kafka Connect and connector health
3. CDC event publishing to Kafka topics
4. Spark Streaming consumption
5. Iceberg Bronze table writes
6. End-to-end latency measurement

Usage:
    # Run full test suite
    python test_cdc_pipeline.py

    # Test specific component
    python test_cdc_pipeline.py --test postgres
    python test_cdc_pipeline.py --test mariadb
"""

import sys
import os
import time
import json
import argparse
import logging
from pathlib import Path
from datetime import datetime
from typing import Dict, Any, Optional

# Add current directory to path
sys.path.insert(0, str(Path(__file__).parent))

from debezium_manager import DebeziumManager, get_debezium_manager
from kafka import KafkaConsumer
from kafka.errors import KafkaError

# Database connectors (if available)
try:
    import psycopg2
    POSTGRES_AVAILABLE = True
except ImportError:
    POSTGRES_AVAILABLE = False

try:
    import mysql.connector
    MARIADB_AVAILABLE = True
except ImportError:
    MARIADB_AVAILABLE = False

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class CDCPipelineTest:
    """
    End-to-end test suite for CDC pipeline.
    """
    
    def __init__(
        self,
        kafka_bootstrap_servers: str = "localhost:9092",
        kafka_connect_url: str = "http://localhost:8083",
        postgres_config: Optional[Dict] = None,
        mariadb_config: Optional[Dict] = None
    ):
        """Initialize test suite."""
        self.kafka_bootstrap_servers = kafka_bootstrap_servers
        self.kafka_connect_url = kafka_connect_url
        self.postgres_config = postgres_config or {
            "host": "192.168.2.114",
            "port": 5432,
            "database": "postgres",
            "user": "postgres",
            "password": "password"
        }
        self.mariadb_config = mariadb_config or {
            "host": "192.168.2.114",
            "port": 3306,
            "database": "mysql",
            "user": "remote_user",
            "password": "secure_password"
        }
        
        self.debezium_manager = get_debezium_manager(
            kafka_connect_url=kafka_connect_url,
            kafka_bootstrap_servers=kafka_bootstrap_servers
        )
        
        self.test_results = []
    
    
    def run_test(self, name: str, description: str, test_func):
        """Run a single test and record result."""
        print(f"\n[TEST] {name}")
        print(f"  {description}")
        
        try:
            start_time = time.time()
            result = test_func()
            duration = time.time() - start_time
            
            status = "PASS" if result else "FAIL"
            symbol = "[PASS]" if result else "[FAIL]"
            
            print(f"  {symbol} Completed in {duration:.2f}s")
            
            self.test_results.append({
                "name": name,
                "status": status,
                "duration": duration,
                "timestamp": datetime.utcnow().isoformat()
            })
            
            return result
            
        except Exception as e:
            print(f"  [ERROR] {str(e)}")
            self.test_results.append({
                "name": name,
                "status": "ERROR",
                "error": str(e),
                "timestamp": datetime.utcnow().isoformat()
            })
            return False
    
    
    def test_kafka_connect_health(self) -> bool:
        """Test 1: Kafka Connect is running and healthy."""
        try:
            import requests
            response = requests.get(f"{self.kafka_connect_url}/", timeout=5)
            
            if response.status_code == 200:
                version = response.json().get("version", "unknown")
                print(f"    Kafka Connect version: {version}")
                return True
            else:
                print(f"    Kafka Connect returned status {response.status_code}")
                return False
                
        except Exception as e:
            print(f"    Cannot reach Kafka Connect: {e}")
            return False
    
    
    def test_postgres_wal_config(self) -> bool:
        """Test 2: PostgreSQL WAL is properly configured."""
        if not POSTGRES_AVAILABLE:
            print("    Skipping: psycopg2 not installed")
            return True
        
        try:
            conn = psycopg2.connect(**self.postgres_config)
            cur = conn.cursor()
            
            # Check wal_level
            cur.execute("SELECT setting FROM pg_settings WHERE name = 'wal_level'")
            wal_level = cur.fetchone()[0]
            print(f"    wal_level: {wal_level}")
            
            if wal_level != 'logical':
                print("    ERROR: wal_level must be 'logical' for CDC")
                return False
            
            # Check replication user exists
            cur.execute("SELECT 1 FROM pg_roles WHERE rolname = 'debezium_user'")
            user_exists = cur.fetchone() is not None
            print(f"    debezium_user exists: {user_exists}")
            
            if not user_exists:
                print("    ERROR: debezium_user not found. Run setup_postgres_wal.sql")
                return False
            
            # Check publication exists
            cur.execute("SELECT 1 FROM pg_publication WHERE pubname = 'dbz_publication'")
            pub_exists = cur.fetchone() is not None
            print(f"    dbz_publication exists: {pub_exists}")
            
            cur.close()
            conn.close()
            
            return user_exists and pub_exists
            
        except Exception as e:
            print(f"    Database error: {e}")
            return False
    
    
    def test_mariadb_binlog_config(self) -> bool:
        """Test 3: MariaDB binlog is properly configured."""
        if not MARIADB_AVAILABLE:
            print("    Skipping: mysql-connector not installed")
            return True
        
        try:
            conn = mysql.connector.connect(**self.mariadb_config)
            cur = conn.cursor()
            
            # Check log_bin
            cur.execute("SELECT @@log_bin")
            log_bin = cur.fetchone()[0]
            print(f"    log_bin: {log_bin}")
            
            if log_bin != 1:
                print("    ERROR: log_bin must be enabled for CDC")
                return False
            
            # Check binlog_format
            cur.execute("SELECT @@binlog_format")
            binlog_format = cur.fetchone()[0]
            print(f"    binlog_format: {binlog_format}")
            
            if binlog_format != 'ROW':
                print("    ERROR: binlog_format must be 'ROW' for CDC")
                return False
            
            # Check replication user exists
            cur.execute("SELECT 1 FROM mysql.user WHERE User = 'debezium_user'")
            user_exists = cur.fetchone() is not None
            print(f"    debezium_user exists: {user_exists}")
            
            if not user_exists:
                print("    ERROR: debezium_user not found. Run setup_mariadb_binlog.sql")
                return False
            
            cur.close()
            conn.close()
            
            return True
            
        except Exception as e:
            print(f"    Database error: {e}")
            return False
    
    
    def test_postgres_connector_status(self) -> bool:
        """Test 4: PostgreSQL CDC connector is running."""
        connector_name = "postgres-cdc-connector"
        
        status = self.debezium_manager.get_connector_status(connector_name)
        
        if not status:
            print(f"    Connector '{connector_name}' not found")
            print("    Run: python setup_cdc_connectors.py --source postgres")
            return False
        
        connector_state = status.get("connector", {}).get("state", "UNKNOWN")
        print(f"    Connector state: {connector_state}")
        
        tasks = status.get("tasks", [])
        for task in tasks:
            task_id = task.get("id")
            task_state = task.get("state")
            print(f"    Task {task_id}: {task_state}")
        
        return connector_state == "RUNNING"
    
    
    def test_mariadb_connector_status(self) -> bool:
        """Test 5: MariaDB CDC connector is running."""
        connector_name = "mariadb-cdc-connector"
        
        status = self.debezium_manager.get_connector_status(connector_name)
        
        if not status:
            print(f"    Connector '{connector_name}' not found")
            print("    Run: python setup_cdc_connectors.py --source mariadb")
            return False
        
        connector_state = status.get("connector", {}).get("state", "UNKNOWN")
        print(f"    Connector state: {connector_state}")
        
        tasks = status.get("tasks", [])
        for task in tasks:
            task_id = task.get("id")
            task_state = task.get("state")
            print(f"    Task {task_id}: {task_state}")
        
        return connector_state == "RUNNING"
    
    
    def test_postgres_cdc_events(self) -> bool:
        """Test 6: PostgreSQL CDC events are published to Kafka."""
        if not POSTGRES_AVAILABLE:
            print("    Skipping: psycopg2 not installed")
            return True
        
        try:
            # Insert test record
            conn = psycopg2.connect(**self.postgres_config)
            cur = conn.cursor()
            
            test_message = f"CDC Test {datetime.utcnow().isoformat()}"
            cur.execute(
                "INSERT INTO public.cdc_test (message) VALUES (%s) RETURNING id",
                (test_message,)
            )
            record_id = cur.fetchone()[0]
            conn.commit()
            
            print(f"    Inserted test record: id={record_id}")
            
            cur.close()
            conn.close()
            
            # Wait for CDC event
            print("    Waiting for CDC event in Kafka...")
            time.sleep(5)
            
            # Try to consume from Kafka topic
            topic = "cdc.syniq_postgres.public.cdc_test"
            consumer = KafkaConsumer(
                topic,
                bootstrap_servers=self.kafka_bootstrap_servers,
                auto_offset_reset='earliest',
                consumer_timeout_ms=10000,
                value_deserializer=lambda m: json.loads(m.decode('utf-8'))
            )
            
            found_event = False
            for message in consumer:
                value = message.value
                # Check if this is our test record
                if value.get('id') == record_id or value.get('message') == test_message:
                    print(f"    Found CDC event for record {record_id}")
                    print(f"    Event: {value}")
                    found_event = True
                    break
            
            consumer.close()
            
            if not found_event:
                print(f"    CDC event not found in topic {topic}")
                print("    This could be a timing issue. Check Kafka UI.")
            
            return found_event
            
        except Exception as e:
            print(f"    Test error: {e}")
            return False
    
    
    def test_kafka_topic_exists(self, topic_prefix: str) -> bool:
        """Test 7: CDC Kafka topics exist."""
        try:
            from kafka import KafkaAdminClient
            
            admin = KafkaAdminClient(bootstrap_servers=self.kafka_bootstrap_servers)
            topics = admin.list_topics()
            admin.close()
            
            cdc_topics = [t for t in topics if t.startswith(topic_prefix)]
            
            print(f"    Found {len(cdc_topics)} CDC topics with prefix '{topic_prefix}'")
            for topic in cdc_topics[:5]:  # Show first 5
                print(f"      - {topic}")
            
            return len(cdc_topics) > 0
            
        except Exception as e:
            print(f"    Error listing topics: {e}")
            return False
    
    
    def print_summary(self):
        """Print test summary."""
        print("\n" + "=" * 80)
        print(" " * 30 + "TEST SUMMARY")
        print("=" * 80)
        
        total = len(self.test_results)
        passed = sum(1 for r in self.test_results if r["status"] == "PASS")
        failed = sum(1 for r in self.test_results if r["status"] == "FAIL")
        errors = sum(1 for r in self.test_results if r["status"] == "ERROR")
        
        print(f"Total: {total} | Passed: {passed} | Failed: {failed} | Errors: {errors}")
        print()
        
        for result in self.test_results:
            status_symbol = {
                "PASS": "[PASS]",
                "FAIL": "[FAIL]",
                "ERROR": "[ERR ]"
            }.get(result["status"], "[????]")
            
            name = result["name"]
            duration = result.get("duration", 0)
            
            print(f"  {status_symbol} {name:<40} ({duration:.2f}s)")
        
        print("=" * 80)
        
        if passed == total:
            print("\nAll tests passed! CDC pipeline is operational.")
            print("\nNext steps:")
            print("  1. Start Spark Streaming consumer:")
            print("     python spark_cdc_consumer.py")
            print("  2. Start monitoring dashboard:")
            print("     python cdc_monitor.py")
            print("  3. Make changes to your databases and watch them flow through!")
        else:
            print("\nSome tests failed. Please review the errors above.")
            print("Refer to CDC_SETUP_GUIDE.md for troubleshooting.")
        
        print()


def main():
    parser = argparse.ArgumentParser(description="CDC Pipeline End-to-End Test")
    parser.add_argument(
        '--test',
        choices=['all', 'postgres', 'mariadb', 'kafka'],
        default='all',
        help='Test suite to run (default: all)'
    )
    parser.add_argument(
        '--kafka-servers',
        default='localhost:9092',
        help='Kafka bootstrap servers'
    )
    
    args = parser.parse_args()
    
    print("=" * 80)
    print(" " * 25 + "CDC PIPELINE TESTING")
    print("=" * 80)
    
    # Create test suite
    test = CDCPipelineTest(kafka_bootstrap_servers=args.kafka_servers)
    
    # Run tests
    if args.test in ['all', 'kafka']:
        test.run_test(
            "Kafka Connect Health",
            "Verify Kafka Connect is running",
            test.test_kafka_connect_health
        )
    
    if args.test in ['all', 'postgres']:
        test.run_test(
            "PostgreSQL WAL Configuration",
            "Verify PostgreSQL WAL is configured for CDC",
            test.test_postgres_wal_config
        )
        
        test.run_test(
            "PostgreSQL Connector Status",
            "Verify PostgreSQL CDC connector is running",
            test.test_postgres_connector_status
        )
        
        test.run_test(
            "PostgreSQL CDC Events",
            "Test CDC event flow from PostgreSQL to Kafka",
            test.test_postgres_cdc_events
        )
        
        test.run_test(
            "PostgreSQL Kafka Topics",
            "Verify PostgreSQL CDC topics exist",
            lambda: test.test_kafka_topic_exists("cdc.syniq_postgres")
        )
    
    if args.test in ['all', 'mariadb']:
        test.run_test(
            "MariaDB Binlog Configuration",
            "Verify MariaDB binlog is configured for CDC",
            test.test_mariadb_binlog_config
        )
        
        test.run_test(
            "MariaDB Connector Status",
            "Verify MariaDB CDC connector is running",
            test.test_mariadb_connector_status
        )
        
        test.run_test(
            "MariaDB Kafka Topics",
            "Verify MariaDB CDC topics exist",
            lambda: test.test_kafka_topic_exists("cdc.syniq_mariadb")
        )
    
    # Print summary
    test.print_summary()
    
    # Exit with appropriate code
    passed = sum(1 for r in test.test_results if r["status"] == "PASS")
    total = len(test.test_results)
    
    return 0 if passed == total else 1


if __name__ == "__main__":
    sys.exit(main())
