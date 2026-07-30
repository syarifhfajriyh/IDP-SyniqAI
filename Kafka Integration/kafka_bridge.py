"""
kafka_bridge.py
===============
Core bridge that publishes SYINIQ ingestion chunks to Kafka topics.

This module is ADDITIVE — it never replaces the existing MinIO write.
If Kafka is unavailable, all publish calls silently return False so the
existing pipeline continues without interruption.

Usage (from ingestion_service.py):
    from Kafka_Integration import get_kafka_bridge

    bridge = get_kafka_bridge()   # singleton
    bridge.publish_dataframe("postgres", df_chunk, metadata, chunk_num=1)
    bridge.publish_file("s3", s3_key, file_size, minio_path, metadata)
"""

import json
import logging
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Locate and load the Kafka .env file from "Kafka Integration/.env"
# ---------------------------------------------------------------------------
_THIS_DIR = Path(__file__).resolve().parent
_KAFKA_ENV_PATH = _THIS_DIR / ".env"

try:
    from dotenv import load_dotenv

    if _KAFKA_ENV_PATH.exists():
        load_dotenv(_KAFKA_ENV_PATH, override=False)  # Don't override existing env
        logger.info(f"KafkaBridge: Loaded Kafka .env from {_KAFKA_ENV_PATH}")
    else:
        logger.warning(
            f"KafkaBridge: Kafka .env not found at {_KAFKA_ENV_PATH}. "
            "Using environment variables or defaults."
        )
except ImportError:
    logger.warning(
        "KafkaBridge: python-dotenv not installed. "
        "Kafka env vars must be set manually."
    )

# ---------------------------------------------------------------------------
# Configuration from environment
# ---------------------------------------------------------------------------
KAFKA_BOOTSTRAP_SERVERS: str = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_DLQ_TOPIC: str = os.getenv("KAFKA_DLQ_TOPIC", "dlq-errors")

# Map source_type → Kafka topic name
KAFKA_SOURCE_TOPICS: Dict[str, str] = {
    "postgres": "bronze-postgres",
    "mariadb": "bronze-mariadb",
    "mariadb_cloud": "bronze-mariadb-cloud",
    "mongodb": "bronze-mongodb",
    "s3": "bronze-s3",
}

# Maximum rows to publish per Kafka message (to keep message size manageable)
MAX_ROWS_PER_MESSAGE = int(os.getenv("KAFKA_MAX_ROWS_PER_MSG", "500"))


# ---------------------------------------------------------------------------
# KafkaBridge
# ---------------------------------------------------------------------------
class KafkaBridge:
    """
    Thread-safe bridge between the SYINIQ ingestion pipeline and Kafka.

    Lifecycle:
        bridge = KafkaBridge()          # connects on first use
        bridge.publish_dataframe(...)   # called inside _ingest_* loops
        bridge.close()                  # called on app shutdown
    """

    def __init__(self) -> None:
        self._producer = None
        self._available: bool = False
        self._init_producer()

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _init_producer(self) -> None:
        """Attempt to create a KafkaProducer. Silently disables if Kafka is down."""
        try:
            from kafka import KafkaProducer  # type: ignore

            self._producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS.split(","),
                value_serializer=lambda v: json.dumps(
                    v, default=str, ensure_ascii=False
                ).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8") if k else None,
                acks="all",
                retries=3,
                retry_backoff_ms=200,
                max_block_ms=5000,      # Never block ingestion > 5s
                request_timeout_ms=10000,
                compression_type="gzip",
            )
            self._available = True
            logger.info(
                f"KafkaBridge: Producer connected → {KAFKA_BOOTSTRAP_SERVERS}"
            )

        except ImportError:
            logger.warning(
                "KafkaBridge: kafka-python not installed. "
                "Run: pip install kafka-python"
            )
            self._available = False

        except Exception as exc:
            logger.warning(
                f"KafkaBridge: Cannot connect to Kafka ({exc}). "
                "Kafka publishing is disabled — ingestion will continue normally."
            )
            self._available = False

    @property
    def is_available(self) -> bool:
        """True when a live Kafka producer exists."""
        return self._available and self._producer is not None

    def get_topic(self, source_type: str) -> str:
        """Return the Kafka topic for a given source type."""
        return KAFKA_SOURCE_TOPICS.get(source_type, f"bronze-{source_type}")

    def _utc_now(self) -> str:
        return datetime.now(timezone.utc).isoformat()

    def _flush(self) -> None:
        """Flush pending messages, ignoring timeout errors."""
        try:
            if self._producer:
                self._producer.flush(timeout=5)
        except Exception as exc:
            logger.debug(f"KafkaBridge flush warning: {exc}")

    def _send_to_dlq(self, source_type: str, metadata: Dict, error: str) -> None:
        """Send a failed-publish record to the Dead Letter Queue topic."""
        if not self._producer:
            return
        try:
            dlq_msg = {
                "source_type": source_type,
                "metadata": metadata,
                "error": error,
                "failed_at": self._utc_now(),
            }
            self._producer.send(KAFKA_DLQ_TOPIC, value=dlq_msg)
        except Exception:
            pass  # DLQ send failure must never propagate

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def publish_dataframe(
        self,
        source_type: str,
        df: Any,          # pd.DataFrame — avoid hard import to stay lightweight
        metadata: Dict[str, Any],
        chunk_num: int = 0,
    ) -> bool:
        """
        Publish a DataFrame chunk to the appropriate bronze Kafka topic.

        The DataFrame is split into batches of MAX_ROWS_PER_MESSAGE records.
        Each Kafka message contains:
            {
                "data": [<row_dict>, ...],
                "row_count": <int>,
                "metadata": { ...original_metadata, "chunk_num": int, "batch": int }
            }

        Returns True on success, False on any error (never raises).
        """
        if not self.is_available:
            return False

        topic = self.get_topic(source_type)

        try:
            records = df.to_dict(orient="records")
            total = len(records)
            batch_size = MAX_ROWS_PER_MESSAGE
            batch_num = 0

            for start in range(0, total, batch_size):
                batch = records[start : start + batch_size]
                batch_num += 1
                message = {
                    "data": batch,
                    "row_count": len(batch),
                    "metadata": {
                        **metadata,
                        "chunk_num": chunk_num,
                        "batch": batch_num,
                        "total_rows_in_chunk": total,
                        "published_at": self._utc_now(),
                        "source_type": source_type,
                    },
                }
                key = (
                    f"{source_type}_{metadata.get('entity', 'unknown')}"
                    f"_c{chunk_num}_b{batch_num}"
                )
                self._producer.send(topic, key=key, value=message)

            self._flush()
            logger.debug(
                f"KafkaBridge: [{source_type}] chunk={chunk_num} "
                f"→ '{topic}' ({total} rows, {batch_num} msg(s))"
            )
            return True

        except Exception as exc:
            logger.warning(
                f"KafkaBridge: Failed to publish chunk={chunk_num} "
                f"to '{topic}': {exc}"
            )
            self._send_to_dlq(source_type, metadata, str(exc))
            return False

    def publish_file(
        self,
        source_type: str,
        file_key: str,
        file_size: int,
        minio_path: str,
        metadata: Dict[str, Any],
    ) -> bool:
        """
        Publish a file-ingestion event (used for S3 source) to Kafka.

        Message format:
            {
                "event": "file_ingested",
                "file_key": str,
                "file_size": int,
                "minio_path": str,
                "metadata": { ...job metadata, "published_at": ... }
            }

        Returns True on success, False on any error (never raises).
        """
        if not self.is_available:
            return False

        topic = self.get_topic(source_type)

        try:
            message = {
                "event": "file_ingested",
                "file_key": file_key,
                "file_size": file_size,
                "minio_path": minio_path,
                "metadata": {
                    **metadata,
                    "published_at": self._utc_now(),
                    "source_type": source_type,
                },
            }
            key = f"{source_type}_{file_key.replace('/', '_')}"
            self._producer.send(topic, key=key, value=message)
            self._flush()
            logger.debug(
                f"KafkaBridge: [{source_type}] file '{file_key}' "
                f"→ '{topic}' ({file_size} bytes)"
            )
            return True

        except Exception as exc:
            logger.warning(
                f"KafkaBridge: Failed to publish file event to '{topic}': {exc}"
            )
            self._send_to_dlq(
                source_type,
                {**metadata, "file_key": file_key},
                str(exc),
            )
            return False

    def health_check(self) -> Dict[str, Any]:
        """Return a health snapshot for the GUI status endpoint."""
        return {
            "available": self.is_available,
            "bootstrap_servers": KAFKA_BOOTSTRAP_SERVERS,
            "topics": list(KAFKA_SOURCE_TOPICS.values()),
            "dlq_topic": KAFKA_DLQ_TOPIC,
            "max_rows_per_message": MAX_ROWS_PER_MESSAGE,
        }

    def close(self) -> None:
        """Gracefully close the producer. Call on application shutdown."""
        if self._producer:
            try:
                self._producer.close(timeout=5)
                logger.info("KafkaBridge: Producer closed.")
            except Exception:
                pass
            finally:
                self._producer = None
                self._available = False


# ---------------------------------------------------------------------------
# Module-level singleton
# ---------------------------------------------------------------------------
_bridge_instance: Optional[KafkaBridge] = None


def get_kafka_bridge() -> KafkaBridge:
    """
    Return the module-level KafkaBridge singleton.
    Creates it on first call. Thread-safe for read access.
    """
    global _bridge_instance
    if _bridge_instance is None:
        _bridge_instance = KafkaBridge()
    return _bridge_instance
