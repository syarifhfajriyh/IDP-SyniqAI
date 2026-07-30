"""
Kafka Integration Package
=========================
Wires the SYINIQ data ingestion pipeline and GUI with Kafka.

Components:
  - kafka_bridge.py       : Publish extracted DataFrame/file chunks to Kafka topics
  - kafka_service.py      : Manage topics, connectors, and health monitoring
  - kafka_routes.py       : FastAPI router exposing /api/kafka/* endpoints to the GUI
  - debezium_manager.py   : Debezium CDC connectors for PostgreSQL, MariaDB, MariaDB Cloud
  - custom_connectors.py  : Custom Python connectors for MongoDB and AWS S3
"""
from .kafka_bridge import KafkaBridge, get_kafka_bridge
from .debezium_manager import DebeziumManager
from .custom_connectors import (
    s3_runner,
    mongodb_runner,
    get_job,
    list_jobs,
)

__all__ = [
    "KafkaBridge",
    "get_kafka_bridge",
    "DebeziumManager",
    "s3_runner",
    "mongodb_runner",
    "get_job",
    "list_jobs",
]
