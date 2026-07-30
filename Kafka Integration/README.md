# Kafka Integration

This folder wires the SYINIQ **data ingestion pipeline** and **GUI backend**
to Kafka. It is **purely additive** — the existing MinIO bronze-layer writes
are never replaced. If Kafka is unavailable, the pipeline continues in
MinIO-only mode without any errors.

---

## Folder Contents

| File | Purpose |
|------|---------|
| `kafka_bridge.py` | Core bridge: publishes extracted DataFrame chunks / file events to Kafka topics alongside the MinIO write |
| `kafka_service.py` | Management service: cluster health, topic listing, Debezium connector CRUD |
| `kafka_routes.py` | FastAPI router mounted at `/api/kafka/*` for GUI consumption |
| `__init__.py` | Package init — exports `KafkaBridge`, `get_kafka_bridge` |

---

## Architecture

```
GUI Browser
   │
   ▼
backend.py  (FastAPI)
   ├── POST /api/ingestion/start
   │       └── IngestionService._ingest_*(...)
   │               ├── connector.extract() → DataFrame chunks
   │               ├── chunk → Parquet → MinIO (existing pipeline, unchanged)
   │               └── chunk → KafkaBridge.publish_dataframe()  ← NEW
   │                           └── bronze-postgres / bronze-mariadb / ...
   │
   └── GET/POST /api/kafka/*  ← NEW endpoints
           └── KafkaService (status, topics, connectors)
```

---

## Kafka Topics Used

| Source | Kafka Topic |
|--------|-------------|
| PostgreSQL | `bronze-postgres` |
| MariaDB | `bronze-mariadb` |
| MariaDB Cloud (SkySQL) | `bronze-mariadb-cloud` |
| MongoDB | `bronze-mongodb` |
| AWS S3 | `bronze-s3` |
| Dead-letter queue | `dlq-errors` |

Topics are defined in `.env` and created automatically on first use.

---

## GUI API Endpoints

All endpoints are prefixed with `/api/kafka/`.

### Status & Health

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/api/kafka/status` | Dashboard summary (broker, connect, topic counts, message totals) |
| `GET` | `/api/kafka/health` | Lightweight broker + Kafka Connect reachability ping |
| `GET` | `/api/kafka/bridge` | In-process KafkaBridge producer status |

### Topics

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/api/kafka/topics` | List all topics with partition & message counts |
| `GET` | `/api/kafka/topics/{name}/sample?max_messages=10` | Last N messages from a topic |
| `POST` | `/api/kafka/topics` | Create a new topic (`{"name":"...", "num_partitions":3}`) |

### Connectors (Debezium / Kafka Connect)

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/api/kafka/connectors` | List all connectors with state + task info |
| `GET` | `/api/kafka/connectors/{name}` | Single connector detail |
| `POST` | `/api/kafka/connectors/{name}/restart` | Restart a connector |
| `PUT` | `/api/kafka/connectors/{name}/pause` | Pause a connector |
| `PUT` | `/api/kafka/connectors/{name}/resume` | Resume a paused connector |
| `DELETE` | `/api/kafka/connectors/{name}` | Delete a connector |
| `POST` | `/api/kafka/connectors` | Register a new Debezium connector |

---

## Configuration

All Kafka configuration is read from `Kafka Integration/.env`.

Key variables:

```
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
KAFKA_CONNECT_HOST=localhost
KAFKA_CONNECT_PORT=8083
KAFKA_DLQ_TOPIC=dlq-errors
KAFKA_MAX_ROWS_PER_MSG=500
```

---

## Run / Test

```bash
# Start FastAPI (already runs via gui start)
cd gui
uvicorn api.backend:app --reload

# Check Kafka integration status
curl http://localhost:8000/api/kafka/status

# List topics
curl http://localhost:8000/api/kafka/topics

# Sample last 5 messages from postgres topic
curl "http://localhost:8000/api/kafka/topics/bronze-postgres/sample?max_messages=5"

# List Debezium connectors
curl http://localhost:8000/api/kafka/connectors
```

---

## Fallback Behaviour

| Situation | Behaviour |
|-----------|-----------|
| Kafka broker down | `KafkaBridge.is_available = False` — all publish calls silently no-op |
| `kafka-python` not installed | Same — warning logged once at startup |
| Kafka Connect unreachable | `/api/kafka/connectors` returns `{"error": "Cannot reach Kafka Connect at ..."}` |
| Publish error on a single chunk | Error logged, message sent to DLQ, ingestion continues |
