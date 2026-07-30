"""
Kafka Schema History Routes - Fetch CDC schema changes from Kafka topics
"""
from fastapi import APIRouter, HTTPException, Query
from typing import Optional, List, Dict, Any
import logging
import json
from datetime import datetime
from kafka import KafkaConsumer
from kafka.errors import KafkaError

logger = logging.getLogger(__name__)

router = APIRouter()

KAFKA_BROKER = 'localhost:9092'


def parse_debezium_schema_change(record: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Parse Debezium CDC record to extract schema changes"""
    try:
        # Debezium schema change events have a specific structure
        payload = record.get('payload', {})
        
        # Check if this is a schema change or DDL event
        if payload.get('databaseName'):
            # This is a database change event
            ddl_statement = payload.get('ddl', '')
            
            # Parse common DDL operations
            change_type = None
            description = ddl_statement
            
            if 'CREATE TABLE' in ddl_statement:
                change_type = 'table_created'
            elif 'ALTER TABLE' in ddl_statement and 'ADD COLUMN' in ddl_statement:
                change_type = 'column_added'
            elif 'ALTER TABLE' in ddl_statement and 'RENAME COLUMN' in ddl_statement:
                change_type = 'column_renamed'
            elif 'ALTER TABLE' in ddl_statement and 'MODIFY COLUMN' in ddl_statement:
                change_type = 'column_modified'
            elif 'DROP COLUMN' in ddl_statement:
                change_type = 'column_dropped'
            
            return {
                'timestamp': payload.get('ts_ms', 0),
                'database': payload.get('databaseName'),
                'table': payload.get('tableName'),
                'change_type': change_type,
                'ddl': ddl_statement,
                'description': description
            }
        
        # Check for schema field changes in regular CDC events
        schema = record.get('schema', {})
        if schema and schema.get('fields'):
            # This might indicate a schema evolution
            fields = schema.get('fields', [])
            return {
                'timestamp': payload.get('ts_ms', 0) if payload else None,
                'schema_version': schema.get('version'),
                'field_count': len(fields),
                'fields': [f.get('field') for f in fields if f.get('field')]
            }
            
    except Exception as e:
        logger.warning(f"Failed to parse schema change: {e}")
    
    return None


def get_schema_changes_from_kafka(topic: str, limit: int = 50) -> List[Dict[str, Any]]:
    """
    Read schema change events from Kafka topic
    Supports both Debezium schema change topics and regular CDC topics
    """
    changes = []
    
    try:
        # Try to find schema history topic
        # Debezium creates schema change topics with pattern: <connector>.schema-changes.<database>
        schema_topics = [
            f'{topic}.schema-changes',
            f'schema-changes.{topic}',
            topic  # Fallback to original topic
        ]
        
        consumer = KafkaConsumer(
            bootstrap_servers=KAFKA_BROKER,
            auto_offset_reset='earliest',
            enable_auto_commit=False,
            consumer_timeout_ms=5000,  # Stop after 5 seconds if no messages
            value_deserializer=lambda m: json.loads(m.decode('utf-8')) if m else None
        )
        
        # Try each potential schema topic
        found_topic = None
        for schema_topic in schema_topics:
            try:
                consumer.subscribe([schema_topic])
                # Check if topic exists by attempting to get partitions
                partitions = consumer.partitions_for_topic(schema_topic)
                if partitions:
                    found_topic = schema_topic
                    logger.info(f"Found schema history topic: {schema_topic}")
                    break
            except Exception as e:
                logger.debug(f"Topic {schema_topic} not found: {e}")
                continue
        
        if not found_topic:
            logger.warning(f"No schema history topic found for {topic}")
            return []
        
        # Read messages from the topic
        count = 0
        for message in consumer:
            if count >= limit:
                break
            
            try:
                value = message.value
                if value:
                    # Try to parse as schema change event
                    change = parse_debezium_schema_change(value)
                    
                    if change:
                        # Convert timestamp to readable format
                        ts = change.get('timestamp')
                        if ts and isinstance(ts, int):
                            change['timestamp_readable'] = datetime.fromtimestamp(ts / 1000).isoformat()
                        
                        changes.append(change)
                        count += 1
            except Exception as e:
                logger.warning(f"Failed to process message: {e}")
                continue
        
        consumer.close()
        
    except KafkaError as e:
        logger.error(f"Kafka error while reading schema changes: {e}")
    except Exception as e:
        logger.error(f"Error reading schema changes from Kafka: {e}")
    
    return changes


@router.get("/schema-history/{table_name}")
async def get_schema_history(
    table_name: str,
    domain: Optional[str] = Query('finance', description="Domain name"),
    source: Optional[str] = Query('postgres', description="Source type"),
    limit: int = Query(10, ge=1, le=100, description="Max number of history entries")
):
    """
    Get schema history for a table from Kafka CDC topics
    
    This endpoint tries to read from:
    1. Debezium schema change topics
    2. Regular CDC topics to detect schema evolution
    3. Falls back to database metadata if Kafka unavailable
    """
    try:
        # Construct possible Kafka topic names
        # Common patterns: <connector>.<database>.<table>, <database>.<table>, <connector>-<table>
        possible_topics = [
            f"{source}.{domain}.{table_name}",
            f"{domain}.{table_name}",
            f"{source}-{table_name}",
            f"debezium.{source}.{table_name}",
            table_name
        ]
        
        all_changes = []
        
        # Try each topic pattern
        for topic in possible_topics:
            try:
                changes = get_schema_changes_from_kafka(topic, limit)
                if changes:
                    all_changes.extend(changes)
                    logger.info(f"Found {len(changes)} schema changes in topic {topic}")
            except Exception as e:
                logger.debug(f"Failed to read from topic {topic}: {e}")
                continue
        
        # Deduplicate and sort by timestamp
        if all_changes:
            # Sort by timestamp descending (newest first)
            all_changes.sort(key=lambda x: x.get('timestamp', 0), reverse=True)
            all_changes = all_changes[:limit]
        
        # Format response
        history_entries = []
        for idx, change in enumerate(all_changes):
            entry = {
                'version': f"v{len(all_changes) - idx}.0",
                'date': change.get('timestamp_readable', 'Unknown'),
                'changes': change.get('description', change.get('ddl', 'Schema modification')),
                'author': f"{change.get('database', '')} Team".strip() or 'System',
                'change_type': change.get('change_type', 'unknown'),
                'raw_event': change  # Include raw event for debugging
            }
            history_entries.append(entry)
        
        # If no Kafka data found, return a helpful message
        if not history_entries:
            history_entries = [{
                'version': 'N/A',
                'date': datetime.now().isoformat(),
                'changes': 'No schema history found in Kafka topics. This could mean: 1) Table is new, 2) CDC not enabled, or 3) Schema changes not being tracked',
                'author': 'System',
                'change_type': 'info'
            }]
        
        return {
            'success': True,
            'table_name': table_name,
            'domain': domain,
            'source': source,
            'history': history_entries,
            'count': len(history_entries),
            'topics_checked': possible_topics
        }
        
    except Exception as e:
        logger.error(f"Failed to get schema history: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/kafka-topics")
async def list_kafka_topics():
    """List all available Kafka topics (for debugging)"""
    try:
        consumer = KafkaConsumer(
            bootstrap_servers=KAFKA_BROKER,
            consumer_timeout_ms=3000
        )
        
        topics = consumer.topics()
        consumer.close()
        
        return {
            'success': True,
            'topics': sorted(list(topics)),
            'count': len(topics)
        }
    except Exception as e:
        logger.error(f"Failed to list Kafka topics: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/kafka-topic-info/{topic_name}")
async def get_topic_info(topic_name: str):
    """Get information about a specific Kafka topic"""
    try:
        consumer = KafkaConsumer(
            topic_name,
            bootstrap_servers=KAFKA_BROKER,
            auto_offset_reset='earliest',
            enable_auto_commit=False,
            consumer_timeout_ms=3000
        )
        
        partitions = consumer.partitions_for_topic(topic_name)
        
        # Get message count (approximate)
        message_count = 0
        sample_messages = []
        
        for message in consumer:
            message_count += 1
            if len(sample_messages) < 5:
                try:
                    sample_messages.append({
                        'offset': message.offset,
                        'timestamp': message.timestamp,
                        'key': message.key.decode('utf-8') if message.key else None,
                        'value_preview': str(message.value)[:200] if message.value else None
                    })
                except:
                    pass
            
            if message_count >= 100:  # Limit scan
                break
        
        consumer.close()
        
        return {
            'success': True,
            'topic': topic_name,
            'partitions': list(partitions) if partitions else [],
            'message_count_scanned': message_count,
            'sample_messages': sample_messages
        }
    except Exception as e:
        logger.error(f"Failed to get topic info: {e}")
        raise HTTPException(status_code=500, detail=str(e))
