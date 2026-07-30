#!/usr/bin/env python3
"""
Read and display all messages from Kafka topic
"""
from kafka import KafkaConsumer
import json

print("\n" + "="*70)
print("  Reading ALL messages from CDC topic")
print("="*70)

consumer = KafkaConsumer(
    'cdc.syniq_postgres.public.loan_applications',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',  # Read from beginning
    enable_auto_commit=False,
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

print(f"\nFetching messages...\n")

messages = []
for message in consumer:
    messages.append(message)
    if len(messages) >= 100:  # Limit to prevent too much output
        break

consumer.close()

print(f"Total messages: {len(messages)}\n")
print("="*70)

for idx, msg in enumerate(messages, 1):
    data = msg.value
    op = data.get('op', '?')
    
    op_names = {'c': 'INSERT', 'u': 'UPDATE', 'd': 'DELETE', 'r': 'READ'}
    operation = op_names.get(op, f'UNKNOWN({op})')
    
    print(f"\n[{idx}] Offset={msg.offset} | Operation={operation}")
    print(f"    loan_id: {data.get('loan_id')}")
    print(f"    name: {data.get('applicant_name')}")
    print(f"    amount: {data.get('loan_amount')}")
    print(f"    status: {data.get('application_status')}")

print(f"\n{'='*70}\n")
