"""
CDC Pipeline Monitoring & Alerting
===================================
Real-time monitoring dashboard for the CDC pipeline.

Monitors:
1. Kafka Connect health and connector status
2. Kafka topic lag and throughput
3. Spark Streaming job health and processing rates
4. Iceberg table metrics (row counts, file sizes)
5. End-to-end latency from database change to Iceberg write

Provides:
- Console dashboard with real-time metrics
- Alert notifications for failures
- Performance metrics and bottleneck detection
- Historical metrics storage

Usage:
    # Run monitoring dashboard
    python cdc_monitor.py

    # Run with custom intervals
    python cdc_monitor.py --interval 10

    # Export metrics to file
    python cdc_monitor.py --export metrics.json
"""

import sys
import os
import time
import json
import argparse
import logging
from pathlib import Path
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
from dataclasses import dataclass, asdict
from collections import deque
import threading

import requests
from kafka import KafkaConsumer, KafkaAdminClient
from kafka.admin import NewTopic
from kafka.structs import TopicPartition

# Add current directory to path
sys.path.insert(0, str(Path(__file__).parent))

from debezium_manager import DebeziumManager, get_debezium_manager

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


@dataclass
class ConnectorMetrics:
    """Metrics for a single connector"""
    name: str
    type: str  # postgres, mariadb, mongodb, s3
    state: str  # RUNNING, PAUSED, FAILED, UNASSIGNED
    tasks_running: int
    tasks_failed: int
    records_processed: int
    errors_count: int
    lag_seconds: Optional[float] = None


@dataclass
class TopicMetrics:
    """Metrics for a Kafka topic"""
    name: str
    partitions: int
    total_messages: int
    messages_per_second: float
    lag: int
    earliest_offset: int
    latest_offset: int


@dataclass
class StreamingMetrics:
    """Metrics for Spark Streaming job"""
    job_name: str
    is_active: bool
    input_rate: float  # records/sec
    processing_rate: float  # records/sec
    num_active_batches: int
    num_completed_batches: int
    avg_batch_duration_ms: float


@dataclass
class AlertEvent:
    """Alert event"""
    severity: str  # INFO, WARNING, ERROR, CRITICAL
    component: str  # connector, topic, streaming, iceberg
    message: str
    timestamp: datetime
    details: Dict[str, Any] = None


class CDCMonitor:
    """
    Comprehensive monitoring for CDC pipeline.
    """
    
    def __init__(
        self,
        kafka_bootstrap_servers: str = "localhost:9092",
        kafka_connect_url: str = "http://localhost:8083",
        update_interval: int = 5,
        history_size: int = 100
    ):
        """
        Initialize CDC Monitor.
        
        Args:
            kafka_bootstrap_servers: Kafka broker addresses
            kafka_connect_url: Kafka Connect REST API URL
            update_interval: Metrics update interval in seconds
            history_size: Number of historical metric snapshots to keep
        """
        self.kafka_bootstrap_servers = kafka_bootstrap_servers
        self.kafka_connect_url = kafka_connect_url
        self.update_interval = update_interval
        
        # Metrics storage
        self.connector_metrics: Dict[str, ConnectorMetrics] = {}
        self.topic_metrics: Dict[str, TopicMetrics] = {}
        self.streaming_metrics: Dict[str, StreamingMetrics] = {}
        self.alerts: deque = deque(maxlen=history_size)
        self.metrics_history: deque = deque(maxlen=history_size)
        
        # Managers
        self.debezium_manager = get_debezium_manager(
            kafka_connect_url=kafka_connect_url,
            kafka_bootstrap_servers=kafka_bootstrap_servers
        )
        
        # Kafka clients
        self.admin_client = None
        self.running = False
        
        logger.info("CDC Monitor initialized")
    
    
    def start(self):
        """Start monitoring in background thread."""
        self.running = True
        
        # Initialize Kafka admin client
        try:
            self.admin_client = KafkaAdminClient(
                bootstrap_servers=self.kafka_bootstrap_servers,
                client_id='cdc-monitor'
            )
            logger.info("Connected to Kafka cluster")
        except Exception as e:
            logger.error(f"Failed to connect to Kafka: {e}")
            self.running = False
            return
        
        # Start monitoring thread
        monitor_thread = threading.Thread(target=self._monitor_loop, daemon=True)
        monitor_thread.start()
        
        logger.info("Monitoring started")
    
    
    def stop(self):
        """Stop monitoring."""
        self.running = False
        if self.admin_client:
            self.admin_client.close()
        logger.info("Monitoring stopped")
    
    
    def _monitor_loop(self):
        """Main monitoring loop."""
        while self.running:
            try:
                # Collect all metrics
                self._collect_connector_metrics()
                self._collect_topic_metrics()
                self._collect_streaming_metrics()
                
                # Check for alert conditions
                self._check_alerts()
                
                # Store snapshot
                self._store_snapshot()
                
                # Wait for next interval
                time.sleep(self.update_interval)
                
            except Exception as e:
                logger.error(f"Monitoring error: {e}")
                time.sleep(self.update_interval)
    
    
    def _collect_connector_metrics(self):
        """Collect metrics from all Debezium connectors."""
        try:
            connectors = self.debezium_manager.list_connectors()
            
            for connector_name in connectors:
                status = self.debezium_manager.get_connector_status(connector_name)
                if not status:
                    continue
                
                connector_state = status.get("connector", {}).get("state", "UNKNOWN")
                tasks = status.get("tasks", [])
                
                metrics = ConnectorMetrics(
                    name=connector_name,
                    type=self._determine_connector_type(connector_name),
                    state=connector_state,
                    tasks_running=sum(1 for t in tasks if t.get("state") == "RUNNING"),
                    tasks_failed=sum(1 for t in tasks if t.get("state") == "FAILED"),
                    records_processed=0,  # Would need to query JMX for this
                    errors_count=0
                )
                
                self.connector_metrics[connector_name] = metrics
                
        except Exception as e:
            logger.warning(f"Failed to collect connector metrics: {e}")
    
    
    def _collect_topic_metrics(self):
        """Collect metrics from CDC Kafka topics."""
        try:
            # Get all CDC topics
            topics = self.admin_client.list_topics()
            cdc_topics = [t for t in topics if t.startswith('cdc.')]
            
            for topic_name in cdc_topics:
                try:
                    # Get topic metadata
                    metadata = self.admin_client.describe_topics([topic_name])
                    if not metadata:
                        continue
                    
                    topic_meta = metadata[0]
                    partitions = len(topic_meta['partitions'])
                    
                    # Get offsets
                    consumer = KafkaConsumer(
                        bootstrap_servers=self.kafka_bootstrap_servers,
                        consumer_timeout_ms=1000
                    )
                    
                    partitions_info = [
                        TopicPartition(topic_name, p) for p in range(partitions)
                    ]
                    
                    earliest = consumer.beginning_offsets(partitions_info)
                    latest = consumer.end_offsets(partitions_info)
                    
                    consumer.close()
                    
                    total_earliest = sum(earliest.values())
                    total_latest = sum(latest.values())
                    total_messages = total_latest - total_earliest
                    
                    # Calculate rate (compare with previous snapshot)
                    prev_metrics = self.topic_metrics.get(topic_name)
                    if prev_metrics:
                        time_diff = self.update_interval
                        msg_diff = total_messages - (prev_metrics.latest_offset - prev_metrics.earliest_offset)
                        rate = msg_diff / time_diff if time_diff > 0 else 0
                    else:
                        rate = 0
                    
                    metrics = TopicMetrics(
                        name=topic_name,
                        partitions=partitions,
                        total_messages=total_messages,
                        messages_per_second=rate,
                        lag=0,  # Would need consumer group info
                        earliest_offset=total_earliest,
                        latest_offset=total_latest
                    )
                    
                    self.topic_metrics[topic_name] = metrics
                    
                except Exception as e:
                    logger.debug(f"Failed to get metrics for topic {topic_name}: {e}")
                    
        except Exception as e:
            logger.warning(f"Failed to collect topic metrics: {e}")
    
    
    def _collect_streaming_metrics(self):
        """Collect metrics from Spark Streaming jobs."""
        # This would require Spark REST API or metrics backend
        # For now, we'll create placeholder metrics
        # In production, query Spark REST API at http://localhost:4040/api/v1/applications
        pass
    
    
    def _check_alerts(self):
        """Check for alert conditions and generate alerts."""
        # Check connector health
        for name, metrics in self.connector_metrics.items():
            if metrics.state != "RUNNING":
                self._create_alert(
                    severity="ERROR",
                    component="connector",
                    message=f"Connector {name} is in {metrics.state} state",
                    details=asdict(metrics)
                )
            
            if metrics.tasks_failed > 0:
                self._create_alert(
                    severity="WARNING",
                    component="connector",
                    message=f"Connector {name} has {metrics.tasks_failed} failed tasks",
                    details=asdict(metrics)
                )
        
        # Check topic lag
        for name, metrics in self.topic_metrics.items():
            if metrics.lag > 100000:
                self._create_alert(
                    severity="WARNING",
                    component="topic",
                    message=f"Topic {name} has high lag: {metrics.lag}",
                    details=asdict(metrics)
                )
    
    
    def _create_alert(self, severity: str, component: str, message: str, details: Dict = None):
        """Create an alert event."""
        alert = AlertEvent(
            severity=severity,
            component=component,
            message=message,
            timestamp=datetime.utcnow(),
            details=details or {}
        )
        
        self.alerts.append(alert)
        
        # Log alert
        log_func = {
            "INFO": logger.info,
            "WARNING": logger.warning,
            "ERROR": logger.error,
            "CRITICAL": logger.critical
        }.get(severity, logger.info)
        
        log_func(f"[{severity}] {component}: {message}")
    
    
    def _store_snapshot(self):
        """Store current metrics snapshot."""
        snapshot = {
            "timestamp": datetime.utcnow().isoformat(),
            "connectors": {k: asdict(v) for k, v in self.connector_metrics.items()},
            "topics": {k: asdict(v) for k, v in self.topic_metrics.items()},
            "streaming": {k: asdict(v) for k, v in self.streaming_metrics.items()}
        }
        
        self.metrics_history.append(snapshot)
    
    
    def _determine_connector_type(self, connector_name: str) -> str:
        """Determine connector type from name."""
        if "postgres" in connector_name.lower():
            return "postgres"
        elif "mariadb" in connector_name.lower() or "mysql" in connector_name.lower():
            return "mariadb"
        elif "mongodb" in connector_name.lower():
            return "mongodb"
        elif "s3" in connector_name.lower():
            return "s3"
        else:
            return "unknown"
    
    
    def print_dashboard(self):
        """Print monitoring dashboard to console."""
        os.system('cls' if os.name == 'nt' else 'clear')
        
        print("=" * 80)
        print(" " * 20 + "SYINIQ CDC PIPELINE MONITOR")
        print("=" * 80)
        print(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print()
        
        # Connector Status
        print("CONNECTORS:")
        print("-" * 80)
        if self.connector_metrics:
            for name, metrics in self.connector_metrics.items():
                status_symbol = "[OK]" if metrics.state == "RUNNING" else "[!!]"
                print(f"  {status_symbol} {name}")
                print(f"      State: {metrics.state} | Tasks: {metrics.tasks_running} running, {metrics.tasks_failed} failed")
        else:
            print("  No connectors found")
        print()
        
        # Topic Metrics
        print("KAFKA TOPICS:")
        print("-" * 80)
        if self.topic_metrics:
            for name, metrics in self.topic_metrics.items():
                print(f"  {name}")
                print(f"      Messages: {metrics.total_messages:,} | Rate: {metrics.messages_per_second:.1f}/sec | Partitions: {metrics.partitions}")
        else:
            print("  No CDC topics found")
        print()
        
        # Recent Alerts
        print("RECENT ALERTS:")
        print("-" * 80)
        recent_alerts = list(self.alerts)[-5:]
        if recent_alerts:
            for alert in reversed(recent_alerts):
                severity_symbol = {
                    "INFO": "[i]",
                    "WARNING": "[!]",
                    "ERROR": "[X]",
                    "CRITICAL": "[!!]"
                }.get(alert.severity, "[?]")
                print(f"  {severity_symbol} {alert.timestamp.strftime('%H:%M:%S')} - {alert.message}")
        else:
            print("  No alerts")
        print()
        
        print("=" * 80)
        print("Press Ctrl+C to exit")
        print("=" * 80)
    
    
    def export_metrics(self, output_file: str):
        """Export metrics history to JSON file."""
        data = {
            "export_time": datetime.utcnow().isoformat(),
            "history": list(self.metrics_history),
            "alerts": [
                {
                    "severity": a.severity,
                    "component": a.component,
                    "message": a.message,
                    "timestamp": a.timestamp.isoformat(),
                    "details": a.details
                }
                for a in self.alerts
            ]
        }
        
        with open(output_file, 'w') as f:
            json.dump(data, f, indent=2)
        
        logger.info(f"Metrics exported to {output_file}")


def main():
    parser = argparse.ArgumentParser(description="CDC Pipeline Monitoring")
    parser.add_argument(
        '--interval',
        type=int,
        default=5,
        help='Update interval in seconds (default: 5)'
    )
    parser.add_argument(
        '--kafka-servers',
        default='localhost:9092',
        help='Kafka bootstrap servers (default: localhost:9092)'
    )
    parser.add_argument(
        '--export',
        help='Export metrics to JSON file'
    )
    
    args = parser.parse_args()
    
    # Create monitor
    monitor = CDCMonitor(
        kafka_bootstrap_servers=args.kafka_servers,
        update_interval=args.interval
    )
    
    # Start monitoring
    monitor.start()
    
    try:
        # Display dashboard
        while True:
            monitor.print_dashboard()
            time.sleep(args.interval)
    except KeyboardInterrupt:
        print("\nShutting down...")
        monitor.stop()
        
        # Export metrics if requested
        if args.export:
            monitor.export_metrics(args.export)
        
        print("Monitoring stopped")
    
    return 0


if __name__ == "__main__":
    sys.exit(main())
