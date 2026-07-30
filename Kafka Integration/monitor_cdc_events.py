#!/usr/bin/env python3
"""
Monitor Kafka topic for CDC events in real-time
Run this BEFORE making changes on the PostgreSQL laptop
"""
from kafka import KafkaConsumer
import json
from datetime import datetime

print("\n" + "="*70)
print("  CDC Event Monitor - Listening for changes...")
print("="*70)
print("\nWaiting for CDC events from PostgreSQL...")
print("Press Ctrl+C to stop\n")

consumer = KafkaConsumer(
    'cdc.syniq_postgres.public.loan_applications',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='latest',  # Only show NEW events
    enable_auto_commit=True,
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

event_count = 0

try:
    for message in consumer:
        event_count += 1
        data = message.value
        
        # Extract CDC metadata
        op = data.get('op', 'unknown')
        ts = data.get('source_ts_ms', 0)
        
        operation_names = {
            'c': 'CREATE (INSERT)',
            'u': 'UPDATE',
            'd': 'DELETE',
            'r': 'READ (Snapshot)'
        }
        
        op_name = operation_names.get(op, f'UNKNOWN ({op})')
        timestamp = datetime.fromtimestamp(ts/1000).strftime('%Y-%m-%d %H:%M:%S') if ts else 'N/A'
        
        print(f"\n{'='*70}")
        print(f"Event #{event_count} - {op_name}")
        print(f"{'='*70}")
        print(f"Timestamp: {timestamp}")
        print(f"Offset: {message.offset}")
        
        # Show relevant fields
        if 'loan_id' in data:
            print(f"\nLoan Details:")
            print(f"  loan_id:            {data.get('loan_id')}")
            print(f"  applicant_name:     {data.get('applicant_name')}")
            
            # Handle loan_amount (could be string or number)
            loan_amt = data.get('loan_amount')
            if loan_amt:
                try:
                    print(f"  loan_amount:        ${float(loan_amt):,.2f}")
                except:
                    print(f"  loan_amount:        {loan_amt}")
            
            print(f"  application_status: {data.get('application_status')}")
            print(f"  approval_date:      {data.get('approval_date')}")
        
        print(f"\nCDC Metadata:")
        print(f"  source_db:    {data.get('source_db', 'N/A')}")
        print(f"  source_table: {data.get('source_table', 'N/A')}")
        print(f"  operation:    {op_name}")
        
        print(f"\n✓ Event captured successfully!")
        
except KeyboardInterrupt:
    print(f"\n\nStopped. Total events captured: {event_count}\n")
except Exception as e:
    print(f"\nError: {e}\n")
finally:
    consumer.close()
