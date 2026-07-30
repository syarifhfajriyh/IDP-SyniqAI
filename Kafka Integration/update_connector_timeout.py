#!/usr/bin/env python3
"""
Update PostgreSQL Debezium connector with longer connection timeout
"""
import requests
import json

KAFKA_CONNECT_URL = "http://localhost:8083"

# Updated connector configuration with longer timeouts
config = {
    "name": "postgres-cdc-connector",
    "config": {
        "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
        "database.hostname": "192.168.2.114",
        "database.port": "5432",
        "database.user": "debezium_user",
        "database.password": "debezium_password",
        "database.dbname": "postgres",
        "database.server.name": "syniq_postgres",
        "topic.prefix": "cdc.syniq_postgres",
        
        # Table configuration
        "table.include.list": "public.hosp_raya_patient_record,public.cdc_test,public.loan_applications",
        "schema.include.list": "public",
        
        # Snapshot configuration
        "snapshot.mode": "initial",
        "snapshot.locking.mode": "none",
        
        # PostgreSQL-specific
        "plugin.name": "pgoutput",
        "publication.name": "dbz_syniq_postgres_publication",
        "publication.autocreate.mode": "filtered",
        "slot.name": "debezium_syniq_slot",
        
        # Connection timeouts (INCREASED)
        "database.connect.timeout.ms": "30000",  # 30 seconds
        "database.query.timeout.ms": "60000",    # 60 seconds
        
        # Converters
        "key.converter": "org.apache.kafka.connect.json.JsonConverter",
        "value.converter": "org.apache.kafka.connect.json.JsonConverter",
        "key.converter.schemas.enable": "false",
        "value.converter.schemas.enable": "false",
        
        # Transform to flatten CDC events
        "transforms": "unwrap",
        "transforms.unwrap.type": "io.debezium.transforms.ExtractNewRecordState",
        "transforms.unwrap.drop.tombstones": "false",
        "transforms.unwrap.delete.handling.mode": "rewrite",
        "transforms.unwrap.add.fields": "op,source.ts_ms,source.db,source.table",
        
        # Schema history
        "schema.history.internal.kafka.bootstrap.servers": "localhost:9092",
        "schema.history.internal.kafka.topic": "schema-changes.syniq_postgres",
        
        # Other
        "tasks.max": "1",
        "include.schema.changes": "true",
        "tombstones.on.delete": "false",
        "heartbeat.interval.ms": "10000",
        "heartbeat.topics.prefix": "__debezium-heartbeat"
    }
}

try:
    # Delete existing connector
    print("Deleting existing connector...")
    resp = requests.delete(f"{KAFKA_CONNECT_URL}/connectors/postgres-cdc-connector")
    if resp.status_code in [204, 404]:
        print("✓ Existing connector deleted")
    
    # Wait a moment
    import time
    time.sleep(2)
    
    # Create new connector with updated config
    print("\nCreating connector with updated configuration...")
    resp = requests.post(
        f"{KAFKA_CONNECT_URL}/connectors",
        json=config,
        headers={"Content-Type": "application/json"}
    )
    
    if resp.status_code == 201:
        print("✓ Connector created successfully!")
        print("\nWait 5 seconds then check status with:")
        print("  Invoke-WebRequest -Uri 'http://localhost:8083/connectors/postgres-cdc-connector/status' -UseBasicParsing")
    else:
        print(f"✗ Failed to create connector: {resp.status_code}")
        print(resp.text)
        
except Exception as e:
    print(f"✗ Error: {e}")
