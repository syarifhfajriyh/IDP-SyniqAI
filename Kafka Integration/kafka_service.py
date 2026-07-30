"""
kafka_service.py
================
SYINIQ Kafka Management Service for the GUI backend.

Provides:
  - Overall Kafka cluster health check
  - Topic listing with offset / lag metadata
  - Kafka Connect (Debezium) connector status & management
  - Topic creation via AdminClient
  - Live-tail sampling (last N messages from a topic)

All methods return plain dicts so they can be serialized as JSON directly
by the FastAPI routes in kafka_routes.py.
"""

from __future__ import annotations

import json
import logging
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import requests

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Load Kafka .env
# ---------------------------------------------------------------------------
_THIS_DIR = Path(__file__).resolve().parent
_KAFKA_ENV_PATH = _THIS_DIR / ".env"

try:
    from dotenv import load_dotenv

    if _KAFKA_ENV_PATH.exists():
        load_dotenv(_KAFKA_ENV_PATH, override=False)
except ImportError:
    pass

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
KAFKA_BOOTSTRAP_SERVERS: str = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_CONNECT_HOST: str = os.getenv("KAFKA_CONNECT_HOST", "localhost")
KAFKA_CONNECT_PORT: str = os.getenv("KAFKA_CONNECT_PORT", "8083")
KAFKA_CONNECT_URL: str = f"http://{KAFKA_CONNECT_HOST}:{KAFKA_CONNECT_PORT}"
KAFKA_CONNECT_TIMEOUT: int = int(os.getenv("KAFKA_CONNECT_TIMEOUT", "5"))

KAFKA_SOURCE_TOPICS: Dict[str, str] = {
    "postgres":      "bronze-postgres",
    "mariadb":       "bronze-mariadb",
    "mariadb_cloud": "bronze-mariadb-cloud",
    "mongodb":       "bronze-mongodb",
    "s3":            "bronze-s3",
}

ALL_MANAGED_TOPICS: List[str] = list(KAFKA_SOURCE_TOPICS.values()) + [
    os.getenv("KAFKA_DLQ_TOPIC", "dlq-errors")
]


# ---------------------------------------------------------------------------
# KafkaService
# ---------------------------------------------------------------------------
class KafkaService:
    """
    Stateless helper used by kafka_routes.py.
    Each method creates short-lived clients so the GUI can always get
    a fresh snapshot without needing a persistent connection.
    """

    # ------------------------------------------------------------------
    # Cluster Health
    # ------------------------------------------------------------------

    def cluster_health(self) -> Dict[str, Any]:
        """
        Check whether the Kafka brokers and Kafka Connect are reachable.

        Returns:
        {
            "kafka_broker": {"status": "ok"|"error", "message": str},
            "kafka_connect": {"status": "ok"|"error", "message": str, "version": str},
            "bootstrap_servers": str,
            "connect_url": str,
            "checked_at": str
        }
        """
        result: Dict[str, Any] = {
            "bootstrap_servers": KAFKA_BOOTSTRAP_SERVERS,
            "connect_url": KAFKA_CONNECT_URL,
            "checked_at": datetime.now(timezone.utc).isoformat(),
        }

        # --- Broker check ---
        try:
            from kafka import KafkaAdminClient
            client = KafkaAdminClient(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS.split(","),
                request_timeout_ms=3000,
            )
            client.close()
            result["kafka_broker"] = {"status": "ok", "message": "Broker reachable"}
        except ImportError:
            result["kafka_broker"] = {
                "status": "error",
                "message": "kafka-python not installed. Run: pip install kafka-python",
            }
        except Exception as exc:
            result["kafka_broker"] = {"status": "error", "message": str(exc)}

        # --- Kafka Connect check ---
        try:
            resp = requests.get(f"{KAFKA_CONNECT_URL}/", timeout=KAFKA_CONNECT_TIMEOUT)
            if resp.status_code == 200:
                info = resp.json()
                result["kafka_connect"] = {
                    "status": "ok",
                    "message": "Kafka Connect reachable",
                    "version": info.get("version", "unknown"),
                }
            else:
                result["kafka_connect"] = {
                    "status": "error",
                    "message": f"HTTP {resp.status_code}",
                }
        except requests.exceptions.ConnectionError:
            result["kafka_connect"] = {
                "status": "error",
                "message": f"Cannot reach Kafka Connect at {KAFKA_CONNECT_URL}",
            }
        except Exception as exc:
            result["kafka_connect"] = {"status": "error", "message": str(exc)}

        return result

    # ------------------------------------------------------------------
    # Topics
    # ------------------------------------------------------------------

    def list_topics(self) -> Dict[str, Any]:
        """
        List all managed Kafka topics with partition and offset information.

        Returns:
        {
            "topics": [
                {
                    "name": str,
                    "source_type": str|None,
                    "partitions": int,
                    "offsets": { "0": {"earliest": int, "latest": int, "lag": int}, ... }
                },
                ...
            ],
            "total": int,
            "error": str|None
        }
        """
        try:
            from kafka import KafkaAdminClient, KafkaConsumer
            from kafka.structs import TopicPartition

            admin = KafkaAdminClient(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS.split(","),
                request_timeout_ms=5000,
            )
            
            # Get topic metadata
            metadata = admin.list_topics()
            topics_data = []

            for topic in ALL_MANAGED_TOPICS:
                if topic not in metadata:
                    continue

                # Determine source type
                source_type = None
                for src, topic_name in KAFKA_SOURCE_TOPICS.items():
                    if topic == topic_name:
                        source_type = src
                        break

                partitions = metadata[topic]
                partition_count = len(partitions)

                # Get offsets for each partition
                consumer = KafkaConsumer(
                    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS.split(","),
                    enable_auto_commit=False,
                )
                
                offsets_info = {}
                for partition_id in range(partition_count):
                    tp = TopicPartition(topic, partition_id)
                    consumer.assign([tp])
                    
                    earliest = consumer.beginning_offsets([tp])[tp]
                    latest = consumer.end_offsets([tp])[tp]
                    lag = latest - earliest
                    
                    offsets_info[str(partition_id)] = {
                        "earliest": earliest,
                        "latest": latest,
                        "lag": lag,
                    }
                
                consumer.close()

                topics_data.append({
                    "name": topic,
                    "source_type": source_type,
                    "partitions": partition_count,
                    "offsets": offsets_info,
                })

            admin.close()

            return {
                "topics": topics_data,
                "total": len(topics_data),
                "error": None,
            }

        except ImportError:
            return {"topics": [], "total": 0, "error": "kafka-python not installed"}
        except Exception as exc:
            logger.error(f"Failed to list topics: {exc}")
            return {"topics": [], "total": 0, "error": str(exc)}

    def topic_sample(
        self, topic_name: str, max_messages: int = 10
    ) -> Dict[str, Any]:
        """
        Return up to `max_messages` recent messages from a topic.

        Returns:
        {
            "topic": str,
            "messages": [{"partition": int, "offset": int, "key": str, "value": dict}, ...],
            "count": int,
            "error": str|None
        }
        """
        try:
            from kafka import KafkaConsumer
            from kafka.structs import TopicPartition

            consumer = KafkaConsumer(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS.split(","),
                value_deserializer=lambda v: json.loads(v.decode("utf-8")),
                auto_offset_reset="latest",
                enable_auto_commit=False,
                consumer_timeout_ms=2000,
            )

            # Get partitions
            partitions = consumer.partitions_for_topic(topic_name)
            if not partitions:
                return {
                    "topic": topic_name,
                    "messages": [],
                    "count": 0,
                    "error": f"Topic '{topic_name}' not found",
                }

            # Seek to end - max_messages for each partition
            tps = [TopicPartition(topic_name, p) for p in partitions]
            consumer.assign(tps)
            
            end_offsets = consumer.end_offsets(tps)
            for tp in tps:
                offset = max(0, end_offsets[tp] - max_messages)
                consumer.seek(tp, offset)

            messages = []
            for msg in consumer:
                messages.append({
                    "partition": msg.partition,
                    "offset": msg.offset,
                    "key": msg.key.decode("utf-8") if msg.key else None,
                    "value": msg.value,
                })
                if len(messages) >= max_messages:
                    break

            consumer.close()

            return {
                "topic": topic_name,
                "messages": messages,
                "count": len(messages),
                "error": None,
            }

        except ImportError:
            return {
                "topic": topic_name,
                "messages": [],
                "count": 0,
                "error": "kafka-python not installed",
            }
        except Exception as exc:
            logger.error(f"Failed to sample topic {topic_name}: {exc}")
            return {
                "topic": topic_name,
                "messages": [],
                "count": 0,
                "error": str(exc),
            }

    def create_topic(
        self,
        topic_name: str,
        num_partitions: int = 3,
        replication_factor: int = 1,
    ) -> Dict[str, Any]:
        """Create a Kafka topic via AdminClient."""
        try:
            from kafka import KafkaAdminClient
            from kafka.admin import NewTopic

            admin = KafkaAdminClient(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS.split(","),
                request_timeout_ms=5000,
            )
            
            topic = NewTopic(
                name=topic_name,
                num_partitions=num_partitions,
                replication_factor=replication_factor,
            )
            
            admin.create_topics([topic])
            admin.close()
            
            return {"success": True, "message": f"Topic '{topic_name}' created"}
        except Exception as exc:
            return {"success": False, "error": str(exc)}

    # ------------------------------------------------------------------
    # Kafka Connect / Debezium Connectors
    # ------------------------------------------------------------------

    def list_connectors(self) -> Dict[str, Any]:
        """
        List all registered Kafka Connect connectors with status.

        Returns:
        {
            "connectors": [
                {"name": str, "type": str, "state": str, "tasks": [...], "config": {...}}
            ],
            "total": int,
            "error": str|None
        }
        """
        try:
            resp = requests.get(
                f"{KAFKA_CONNECT_URL}/connectors",
                timeout=KAFKA_CONNECT_TIMEOUT,
            )
            resp.raise_for_status()
            
            names = resp.json()
            connectors = []
            
            for name in names:
                # Get status
                status_resp = requests.get(
                    f"{KAFKA_CONNECT_URL}/connectors/{name}/status",
                    timeout=KAFKA_CONNECT_TIMEOUT,
                )
                status = status_resp.json() if status_resp.status_code == 200 else {}
                
                # Get config
                config_resp = requests.get(
                    f"{KAFKA_CONNECT_URL}/connectors/{name}/config",
                    timeout=KAFKA_CONNECT_TIMEOUT,
                )
                config = config_resp.json() if config_resp.status_code == 200 else {}
                
                connectors.append({
                    "name": name,
                    "type": config.get("connector.class", "unknown"),
                    "state": status.get("connector", {}).get("state", "unknown"),
                    "tasks": status.get("tasks", []),
                    "config": config,
                })
            
            return {
                "connectors": connectors,
                "total": len(connectors),
                "error": None,
            }

        except requests.exceptions.ConnectionError:
            return {
                "connectors": [],
                "total": 0,
                "error": f"Cannot reach Kafka Connect at {KAFKA_CONNECT_URL}",
            }
        except Exception as exc:
            logger.error(f"Failed to list connectors: {exc}")
            return {"connectors": [], "total": 0, "error": str(exc)}

    def get_connector(self, connector_name: str) -> Dict[str, Any]:
        """Get status and config of a single connector."""
        try:
            status_resp = requests.get(
                f"{KAFKA_CONNECT_URL}/connectors/{connector_name}/status",
                timeout=KAFKA_CONNECT_TIMEOUT,
            )
            
            if status_resp.status_code == 404:
                return {"error": f"Connector '{connector_name}' not found"}
            
            status = status_resp.json()
            
            config_resp = requests.get(
                f"{KAFKA_CONNECT_URL}/connectors/{connector_name}/config",
                timeout=KAFKA_CONNECT_TIMEOUT,
            )
            config = config_resp.json() if config_resp.status_code == 200 else {}
            
            return {
                "name": connector_name,
                "status": status,
                "config": config,
            }
        except requests.exceptions.ConnectionError:
            return {"error": f"Cannot reach Kafka Connect at {KAFKA_CONNECT_URL}"}
        except Exception as exc:
            return {"error": str(exc)}

    def restart_connector(self, connector_name: str) -> Dict[str, Any]:
        """Restart a Kafka Connect connector."""
        try:
            resp = requests.post(
                f"{KAFKA_CONNECT_URL}/connectors/{connector_name}/restart",
                timeout=KAFKA_CONNECT_TIMEOUT,
            )
            
            if resp.status_code in (200, 204):
                return {"success": True, "message": f"Connector '{connector_name}' restarted"}
            else:
                return {"success": False, "error": f"HTTP {resp.status_code}"}
        except Exception as exc:
            return {"success": False, "error": str(exc)}

    def pause_connector(self, connector_name: str) -> Dict[str, Any]:
        """Pause a Kafka Connect connector."""
        try:
            resp = requests.put(
                f"{KAFKA_CONNECT_URL}/connectors/{connector_name}/pause",
                timeout=KAFKA_CONNECT_TIMEOUT,
            )
            return {"success": resp.status_code in (200, 202)}
        except Exception as exc:
            return {"success": False, "error": str(exc)}

    def resume_connector(self, connector_name: str) -> Dict[str, Any]:
        """Resume a paused Kafka Connect connector."""
        try:
            resp = requests.put(
                f"{KAFKA_CONNECT_URL}/connectors/{connector_name}/resume",
                timeout=KAFKA_CONNECT_TIMEOUT,
            )
            return {"success": resp.status_code in (200, 202)}
        except Exception as exc:
            return {"success": False, "error": str(exc)}

    def delete_connector(self, connector_name: str) -> Dict[str, Any]:
        """Delete (unregister) a Kafka Connect connector."""
        try:
            resp = requests.delete(
                f"{KAFKA_CONNECT_URL}/connectors/{connector_name}",
                timeout=KAFKA_CONNECT_TIMEOUT,
            )
            return {"success": resp.status_code in (200, 204)}
        except Exception as exc:
            return {"success": False, "error": str(exc)}

    def register_debezium_connector(
        self, config_name: str, config: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Register a new Debezium connector via Kafka Connect REST API.

        `config` should be the full connector configuration dict.
        """
        try:
            resp = requests.post(
                f"{KAFKA_CONNECT_URL}/connectors",
                json={"name": config_name, "config": config},
                headers={"Content-Type": "application/json"},
                timeout=KAFKA_CONNECT_TIMEOUT * 2,
            )
            
            if resp.status_code in (200, 201):
                return {
                    "success": True,
                    "message": f"Connector '{config_name}' registered",
                    "connector": resp.json(),
                }
            else:
                return {
                    "success": False,
                    "error": f"HTTP {resp.status_code}: {resp.text}",
                }
        except Exception as exc:
            return {"success": False, "error": str(exc)}

    # ------------------------------------------------------------------
    # Convenience - full status summary for dashboard
    # ------------------------------------------------------------------

    def dashboard_summary(self) -> Dict[str, Any]:
        """
        Return a combined summary used by the GUI Kafka dashboard card.
        """
        health = self.cluster_health()
        broker_ok = health.get("kafka_broker", {}).get("status") == "ok"
        connect_ok = health.get("kafka_connect", {}).get("status") == "ok"

        topics_data = self.list_topics() if broker_ok else {"topics": [], "total": 0}
        connectors_data = (
            self.list_connectors() if connect_ok else {"connectors": [], "total": 0}
        )

        # Aggregate connector states
        conn_states: Dict[str, int] = {}
        for conn in connectors_data.get("connectors", []):
            state = conn.get("state", "unknown")
            conn_states[state] = conn_states.get(state, 0) + 1

        # Count total messages across all topics
        total_messages = 0
        for topic in topics_data.get("topics", []):
            for partition_offset in topic.get("offsets", {}).values():
                total_messages += partition_offset.get("lag", 0)

        return {
            "health": health,
            "topics": {
                "total": topics_data.get("total", 0),
                "total_messages": total_messages,
            },
            "connectors": {
                "total": connectors_data.get("total", 0),
                "by_state": conn_states,
            },
        }
