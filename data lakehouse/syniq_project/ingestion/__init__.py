"""
Data ingestion pipeline for Syniq Lakehouse.

Components:
- BronzeIngestor: Writes raw data to Bronze layer
- IngestionOrchestrator: Manages multi-table ingestion
- IngestionJob: Individual ingestion task wrapper
- ConnectorFactory: Dynamic connector instantiation
- run_ingestion: Convenience function for orchestration

Usage:
    from ingestion import BronzeIngestor, IngestionOrchestrator, run_ingestion
    
    # Direct bronze ingestion
    ingestor = BronzeIngestor(minio_config)
    ingestor.ingest_batch(df, "postgres_prod", "customers")
    
    # Orchestrated ingestion
    report = run_ingestion(sources=["postgres_prod"])
"""

from .bronze_ingestor import BronzeIngestor
from .ingestion_orchestrator import (
    IngestionOrchestrator,
    IngestionJob,
    IngestionStatus,
    ConnectorFactory,
    run_ingestion
)

__all__ = [
    "BronzeIngestor",
    "create_bronze_validator",
    "IngestionOrchestrator",
    "IngestionJob",
    "IngestionStatus",
    "ConnectorFactory",
    "run_ingestion"
]

__version__ = "0.1.0"
