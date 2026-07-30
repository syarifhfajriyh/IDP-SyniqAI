"""
Debug script to see raw Kafka CDC message structure
"""
from kafka import KafkaConsumer
import json

topic = 'cdc.syniq_postgres.public.march_user_transactions'

consumer = KafkaConsumer(
    topic,
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    enable_auto_commit=False,
    consumer_timeout_ms=5000,
    value_deserializer=lambda x: json.loads(x.decode('utf-8')) if x else None
)

print(f"Reading from topic: {topic}")
print("=" * 80)

for msg in consumer:
    if msg.value:
        print("\n### RAW MESSAGE STRUCTURE ###")
        print(json.dumps(msg.value, indent=2))
        print("\nTop-level keys:", list(msg.value.keys()))
        
        if 'payload' in msg.value:
            print("\nPayload keys:", list(msg.value['payload'].keys()))
            if 'op' in msg.value['payload']:
                print(f"Operation: {msg.value['payload']['op']}")
            if 'source' in msg.value['payload']:
                print(f"Source: {msg.value['payload']['source']}")
        
        break  # Just show first message

consumer.close()
