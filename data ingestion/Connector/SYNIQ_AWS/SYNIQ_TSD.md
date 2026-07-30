SyniqAI Data Ingestion System
Consolidated Connector Technical Specification Document
Enterprise Multi-Source Data Integration Platform

Version: 1.1
Date: February 16, 2026
Status: Production-Ready
Document Type: Master Reference

Table of Contents

1. Executive Summary
2. System Overview & Architecture
   2.1 Product Behaviour & Capabilities
   2.2 Unified System Architecture
   2.3 Design Principles
   2.4 Explicit Non-Goals

## Source-Aware Intelligence

Each connector is purpose-built with deep knowledge of its target system:

| Principle      | PostgreSQL Example                                 | MongoDB Example                                                        | MariaDB Example                                 | MariaDB Cloud Example                | AWS S3 Example                                                                 |
|----------------|----------------------------------------------------|-----------------------------------------------------------------------|-------------------------------------------------|--------------------------------------|-------------------------------------------------------------------------------|
| **Parallelism**| Table partitioning awareness                       | Sharded cluster and collection chunking awareness                     | Storage engine detection (InnoDB vs MyISAM)      | Serverless connection limits         | Multi-threaded/multi-process downloads and uploads for high throughput        |
| **Consistency**| Snapshot isolation                                 | MVCC (Multi-Version Concurrency Control) and read concern support for consistent reads | MVCC consistent snapshots                        | Cross-region latency                 | Handles eventual consistency and cross-region latency for object listing and retrieval |
| **Optimization**| Index-aware chunking                              | Index-based chunking and batch cursor usage for efficient large dataset traversal | Quantile sampling for sparse PKs                | Large chunks to reduce roundtrips    | Large chunking and multipart transfers to minimize API roundtrips and maximize throughput |

3. Connector Specifications
   3.1 PostgreSQL Connector
   3.2 MariaDB (Self-Hosted) Connector
   3.3 MariaDB Cloud (SkySQL) Connector
   3.4 MongoDB Connector
   3.5 AWS S3 Connector
4. Common Features & Optimizations
5. Technical Implementation
6. Security & Compliance
7. Performance Benchmarks
8. GUI Integration
9. Operational Guidelines
10. Appendix: Environmental Setup & Prerequisites

1. Executive Summary

The SyniqAI Data Ingestion System is an enterprise-grade, multi-source ingestion platform designed to extract, transform, and load (ETL) data from a variety of sources into the SyniqAI Bronze Layer. It supports structured and unstructured data, batch and incremental modes, and is built for extensibility, security, and operational transparency. This document consolidates all connector specifications and serves as the definitive reference for engineering, integration, and troubleshooting.

2. System Overview & Architecture

2.1 Product Behaviour & Capabilities

- Multi-Source Ingestion: Supports PostgreSQL, MariaDB (Self-Hosted & Cloud), MongoDB (Atlas & On-Prem), and AWS S3.
- Deterministic Three-Stage Flow:
  1. Producers ingest files/records from source systems, extracting bytes and canonical metadata.
  2. Consumers persist sanitized artifacts to the primary datastore (DB, S3, GridFS, etc.).
  3. Bounded In-Memory Queue enforces backpressure, ensuring predictable throughput and preventing memory exhaustion.
- Immutable Artifacts: Each run produces both data (e.g., Parquet, ZIP, GridFS) and metadata (JSONL, MongoDB, etc.).
- Fail-Fast Safety: Errors are surfaced immediately; partial or silent failures are not permitted.
- Schema Handling: No implicit schema changes; normalization/enrichment is performed downstream.

2.2 Unified System Architecture

[See original markdown for diagram]

2.3 Design Principles

- Separation of Concerns: Decouples network/DB I/O from local disk I/O.
- Backpressure by Design: Bounded queues prevent memory growth.
- Deterministic Outputs: Identical inputs yield identical outputs.
- Fail-Fast: Immediate error surfacing and controlled aborts.
- Extensibility: New connectors can be added with minimal changes to the core.

2.4 Explicit Non-Goals

- No implicit flattening/exploding of complex data.
- No business logic or domain-specific transformation during ingestion.
- No destructive schema changes at the Bronze layer.
- No operation without metadata and lineage.

3. Connector Specifications

3.1 PostgreSQL Connector

- Status: Production-ready
- Features:
  - Parallel extraction for high throughput.
  - Supports full and incremental loads.
  - Schema discovery and type mapping.
  - Output: Parquet files and metadata JSON.
- Configuration:
  - Host, port, database, user, password, SSL options.
  - Table/column selection, extraction mode.
- Optimizations:
  - Batch fetch, connection pooling, retry logic.

3.2 MariaDB (Self-Hosted) Connector

3.2 MongoDB Connector

#### 3.2.1 Overview

**Technology:** MongoDB 4.x+ (Atlas & On-Prem)
**Driver:** pymongo

**Key Features:**
- Sharded cluster and collection chunking awareness
- GridFS for large file storage (videos, large images)
- Batch/bulk writes for high throughput
- Deduplication by SHA256 hash
- Index-based chunking for efficient traversal
- Document-level locking (MVCC)

#### 3.2.2 Connection Configuration

```json
{
  "source_type": "mongodb",
  "connection_config": {
   "MONGO_URI": "mongodb+srv://user:pass@cluster.mongodb.net/?retryWrites=true&w=majority",
   "MONGO_DB": "media_db"
  },
  "extraction_request": {
   "entity": "images",
   "mode": "full",
   "batch_size": 1000
  }
}
```

#### 3.2.3 Architecture & Optimizations

1. **Connection Pooling:**
  - `maxPoolSize` configurable (default 20-50)
  - Connection URI or individual host/port/user/pass
2. **Batch Processing:**
  - Bulk writes for metadata, GridFS streaming for large files
  - Batch size tuning for throughput/memory
3. **Deduplication:**
  - SHA256 hash for duplicate detection
4. **Index-Aware Chunking:**
  - Uses index scans for efficient large dataset traversal
5. **Document-Level Locking:**
  - MVCC ensures high concurrency, no table locks

#### 3.2.4 Performance Characteristics

| Collection Size | Batch Size | Avg Throughput | Notes |
|-----------------|-----------|---------------|-------|
| 100k docs       | 1,000     | ~25k docs/sec | Optimal for images/text |
| 1M docs         | 1,000     | ~22k docs/sec | GridFS for large files |
| 10M docs        | 1,000     | ~20k docs/sec | Sustained, depends on I/O |

#### 3.2.5 Error Handling

| Error Type           | Detection                        | Response                                  |
|---------------------|----------------------------------|-------------------------------------------|
| Connection Timeout  | pymongo.errors.ServerSelectionTimeoutError | Immediate failure, log details           |
| Duplicate Key       | pymongo.errors.BulkWriteError    | Skip/mark duplicate, continue batch        |
| Out of Memory       | MemoryError, OOM kill            | Reduce batch size, retry                   |
| Invalid Document    | bson.errors.InvalidDocument      | Log and skip invalid doc                   |
| GridFS Error        | gridfs.errors.*                  | Log error, skip file, continue             |

#### 3.2.6 Known Limitations

**Batch Size Tuning:** Large batches may cause memory pressure on very large files.
**GridFS Overhead:** GridFS adds some overhead for very large files (videos).
**Schema Changes:** Dynamic schema changes in collections may cause extraction issues.
**Network Latency:** Throughput depends on network speed, especially for cloud (Atlas).
  - Storage engine-aware extraction.
  - Advanced optimizations for InnoDB/MyISAM.
  - Full/incremental loads, schema mapping.
  - Host, port, database, user, password, SSL.
  - Table/column selection, extraction mode.
  - Parallel reads, batch fetch, deduplication.

3.3 MariaDB Cloud (SkySQL) Connector

3.5 AWS S3 Connector

#### 3.5.1 Overview

**Technology:** AWS S3 (boto3)
**Driver:** boto3

**Key Features:**
- Supports both structured (CSV, Parquet) and unstructured (images, videos, documents) data
- Batch and parallel download/upload
- Deduplication by file hash or S3 object metadata
- Manifest tracking for already-copied files
- Multi-part upload/download for large files
- Performance metrics and optimization impact reporting

#### 3.5.2 Connection Configuration

```json
{
  "source_type": "aws_s3",
  "connection_config": {
   "aws_access_key_id": "YOUR_ACCESS_KEY",
   "aws_secret_access_key": "YOUR_SECRET_KEY",
   "region_name": "ap-southeast-1",
   "bucket_name": "your-bucket",
   "prefix": "data/"
  },
  "extraction_request": {
   "mode": "full",
   "batch_size": 1000
  }
}
```

#### 3.5.3 Architecture & Optimizations

1. **Connection Pooling:**
  - boto3 uses a small connection pool (default 3, overflow 2)
  - Credentials via environment variables or .env file
2. **Batch & Parallel Processing:**
  - Multi-threaded/multi-process downloads and uploads
  - Batch size and worker count configurable
3. **Deduplication:**
  - File hash or S3 object metadata
  - Manifest tracking for already-copied files
4. **Multi-part Upload/Download:**
  - Efficient handling of large files
5. **Error Handling & Retry:**
  - Automatic retries for throttling (429/503) and transient errors
- Status: Production-ready
#### 3.5.4 Performance Characteristics

| Object Count | Batch Size | Avg Throughput | Notes |
|--------------|-----------|---------------|-------|
| 100k files   | 1,000     | ~20k files/hr | Small files, optimal batch |
| 1M files     | 1,000     | ~18k files/hr | Parallel, depends on network |
| 10M files    | 1,000     | ~15k files/hr | Sustained, large manifests |

#### 3.5.5 Error Handling

| Error Type           | Detection                        | Response                                  |
|---------------------|----------------------------------|-------------------------------------------|
| Connection Timeout  | botocore.exceptions.EndpointConnectionError | Immediate failure, log details           |
| Throttling          | botocore.exceptions.ClientError (429/503)  | Automatic retry with backoff             |
| Duplicate File      | File hash or manifest            | Skip/mark duplicate, continue batch        |
| Out of Memory       | MemoryError, OOM kill            | Reduce batch size, retry                   |
| Invalid Object      | botocore.exceptions.ClientError  | Log and skip invalid object                |

#### 3.5.6 Known Limitations

**Batch Size Tuning:** Large batches may cause memory pressure for very large files.
**S3 Rate Limits:** Excessive parallelism may trigger throttling; tune worker count.
**Eventual Consistency:** S3 listings may be eventually consistent in rare cases.
**Network Latency:** Throughput depends on network speed and S3 region.
- Features:
3.3 MariaDB Cloud (SkySQL) Connector
  - Cloud-native, security-first design.
  - Handles SkySQL-specific endpoints and auth.
  - Full/incremental loads, schema mapping.
- Configuration:
  - SkySQL endpoint, user, password, SSL certs.
  - Table/column selection, extraction mode.
- Optimizations:
  - Cloud connection pooling, network retry.

3.4 MongoDB Connector

- Status: Production-ready (Atlas & On-Prem)
- Features:
  - Supports both MongoDB Atlas (cloud) and on-premise deployments.
  - Unified connection via MONGO_URI (Atlas/Custom) or legacy host/port/user/pass.
  - Ingests media files (images, text, videos) and metadata.
  - Uses GridFS for large files (videos, large images).
  - Bounded in-memory queue for ingestion.
  - Batch processing for high throughput (configurable batch size).
  - Deduplication by hash (SHA256).
  - Metadata and lineage stored in dedicated collections.
- Configuration:
  - MONGO_URI or individual host/port/user/pass/authSource.
  - Target database and collection names.
  - Batch size, allowed file types.
- Optimizations:
  - Bulk/batch writes for metadata.
  - GridFS streaming for large files.
  - Connection pooling.
- Security:
  - Credentials via environment file (on_prem.env).
  - No credentials in code.
  - Supports both password and X.509 authentication.
- Error Handling:
  - Fail-fast on connection or write errors.
  - Duplicate detection and skip logic.
- Output:
  - Data in GridFS/blobs, metadata in JSON/collections.
  - Run-level metrics and audit trail.

3.5 AWS S3 Connector

- Status: Production-ready
- Features:
  - Ingests files from S3 buckets (client or internal).
  - Supports both structured (CSV, Parquet) and unstructured (images, videos, documents) data.
  - Deduplication by file hash or S3 object metadata.
  - Batch and parallel download/upload.
  - Manifest tracking for already-copied files.
  - Performance metrics and optimization impact reporting.
- Configuration:
  - Source and destination bucket names, prefixes.
  - AWS credentials (access key, secret key, region) via environment variables.
  - Output directory for local artifacts.
- Optimizations:
  - S3 paginator for efficient listing.
  - Multi-part upload/download for large files.
  - Skips duplicates and tracks errors.
- Security:
  - Credentials via .env file.
  - No credentials in code.
  - Supports IAM roles and key rotation.
- Error Handling:
  - Immediate abort on critical errors.
  - Detailed error and performance summary.
- Output:
  - Data in local output directory and/or S3.
  - Metadata manifests for audit and deduplication.

4. Common Features & Optimizations

- Intelligent Auto-Tuning: Dynamic adjustment of batch size, parallelism, and retry logic based on source/target performance.
- Schema Handling Strategy: No implicit schema changes; all transformations are explicit and versioned.
- Deduplication: Hash-based or manifest-based duplicate detection.
- Audit Trail: All runs produce detailed logs, metrics, and metadata for traceability.
- Backpressure: Bounded queues and streaming to prevent memory exhaustion.

5. Technical Implementation

- Technology Stack: Python 3.9+, pandas, boto3, pymongo, SQLAlchemy, GridFS, dotenv, Tkinter (for UI).
- Connection Management: Unified connection system for all connectors; credentials via environment files.
- Batch Processing: All connectors support batch/bulk operations for throughput.
- Error Handling: Fail-fast, with detailed error reporting and no silent failures.
- Extensibility: New connectors can be added by subclassing the base connector and implementing required methods.

6. Security & Compliance

- Credential Management: All secrets are loaded from environment files; never hardcoded.
- Data Privacy: No sensitive data is logged; all logs are sanitized.
- Auditability: All operations are logged with run IDs, timestamps, and user context.
- Compliance: Supports audit trail, access control, and encryption at rest/in transit.

7. Performance Benchmarks

- Connector Comparison Matrix: Each connector is benchmarked for throughput, latency, and resource utilization.
- Resource Utilization: Monitors CPU, memory, and network usage during ingestion.
- Optimization Impact: Before/after metrics for deduplication, batching, and parallelism.

8. GUI Integration

- Unified Interface: All connectors are accessible via a single UI, with consistent workflows for configuration, execution, and monitoring.
- User Experience: Professional dark theme, clear error messages, and real-time progress reporting.
- Extensibility: New connectors appear automatically in the UI when added to the backend.

9. Operational Guidelines

- Deployment: All connectors are deployed as part of the unified ingestion platform; environment files must be configured before use.
- Rollout & Rollback: Versioned deployments with rollback support.
- Monitoring: All runs are logged; failures are surfaced immediately.
- Troubleshooting: Detailed error messages and logs are available for all connectors.

10. Appendix: Environmental Setup & Prerequisites

- Python Environment: Python 3.9+ with all required packages installed.
- Environment Files: .env or on_prem.env must be configured for each connector.
- Firewall Rules: Ensure required ports (e.g., 27017 for MongoDB, 443 for S3) are open.
- Permissions: IAM roles for S3, user roles for databases.
- Testing: Use provided test scripts (e.g., test_connection.py) to validate configuration before production use.

End of Document
