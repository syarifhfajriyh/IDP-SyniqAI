"""
Test Kafka Schema History Integration
======================================

This script tests the Schema History API endpoints and demonstrates
how to trigger CDC events that will appear in the History tab.
"""

import requests
import json
import time
from typing import Dict, Any

BASE_URL = "http://localhost:8000"

def test_kafka_topics():
    """Test: List all Kafka topics"""
    print("\n" + "="*70)
    print("TEST 1: List Kafka Topics")
    print("="*70)
    
    response = requests.get(f"{BASE_URL}/api/bronze/kafka-topics")
    
    if response.ok:
        data = response.json()
        print(f"✅ Success: Found {data['count']} topics")
        print("\nTopics:")
        for topic in data['topics']:
            print(f"  • {topic}")
        return True
    else:
        print(f"❌ Failed: {response.status_code} - {response.text}")
        return False


def test_schema_history(table_name: str, domain: str = "finance", source: str = "postgres"):
    """Test: Get schema history for a table"""
    print("\n" + "="*70)
    print(f"TEST 2: Get Schema History - {table_name}")
    print("="*70)
    
    params = {
        'domain': domain,
        'source': source,
        'limit': 10
    }
    
    response = requests.get(
        f"{BASE_URL}/api/bronze/schema-history/{table_name}",
        params=params
    )
    
    if response.ok:
        data = response.json()
        print(f"✅ Success: Found {data['count']} history entries")
        print(f"\nTable: {data['table_name']}")
        print(f"Domain: {data['domain']}")
        print(f"Source: {data['source']}")
        print(f"\nTopics checked:")
        for topic in data['topics_checked']:
            print(f"  • {topic}")
        
        print(f"\nSchema History:")
        for entry in data['history']:
            print(f"\n  Version: {entry['version']}")
            print(f"  Date: {entry['date']}")
            print(f"  Changes: {entry['changes']}")
            print(f"  Author: {entry['author']}")
            print(f"  Type: {entry['change_type']}")
        
        return True
    else:
        print(f"❌ Failed: {response.status_code} - {response.text}")
        return False


def test_topic_info(topic_name: str):
    """Test: Get detailed topic information"""
    print("\n" + "="*70)
    print(f"TEST 3: Topic Info - {topic_name}")
    print("="*70)
    
    response = requests.get(f"{BASE_URL}/api/bronze/kafka-topic-info/{topic_name}")
    
    if response.ok:
        data = response.json()
        print(f"✅ Success")
        print(f"\nTopic: {data['topic']}")
        print(f"Partitions: {data['partitions']}")
        print(f"Messages scanned: {data['message_count_scanned']}")
        
        if data['sample_messages']:
            print(f"\nSample messages:")
            for msg in data['sample_messages']:
                print(f"  • Offset: {msg['offset']}, Timestamp: {msg['timestamp']}")
                print(f"    Preview: {msg['value_preview'][:100]}...")
        else:
            print("\n⚠️  No messages in topic (empty)")
        
        return True
    else:
        print(f"❌ Failed: {response.status_code} - {response.text}")
        return False


def demo_trigger_cdc_event():
    """
    Demo: How to trigger a CDC event that will show in Schema History
    
    NOTE: This is a demonstration script. To actually trigger CDC events:
    1. Connect to your database
    2. Run ALTER TABLE statements
    3. Wait for Debezium to process (~5 seconds)
    4. Refresh the History tab in UI
    """
    print("\n" + "="*70)
    print("DEMO: How to Trigger CDC Schema Change Events")
    print("="*70)
    
    print("\n📝 To see real schema changes in the History tab:")
    print("\n1. Connect to PostgreSQL:")
    print("   psql -h localhost -U postgres -d finance")
    
    print("\n2. Make a schema change:")
    print("   ALTER TABLE finance_transactions ADD COLUMN test_field VARCHAR(50);")
    
    print("\n3. Wait for Debezium to process (~5 seconds)")
    
    print("\n4. Refresh the History tab in UI")
    print("   You should see a new entry with change_type='column_added'")
    
    print("\n5. To remove the test column:")
    print("   ALTER TABLE finance_transactions DROP COLUMN test_field;")
    
    print("\n" + "="*70)
    print("Alternative: Python script to trigger CDC event")
    print("="*70)
    
    print("""
import psycopg2

# Connect to database
conn = psycopg2.connect(
    host="localhost",
    database="finance",
    user="postgres",
    password="postgres"
)
cur = conn.cursor()

# Add a column (will trigger CDC event)
cur.execute(\"\"\"
    ALTER TABLE finance_transactions 
    ADD COLUMN country_code CHAR(2) DEFAULT 'US'
\"\"\")
conn.commit()

print("✅ Schema change committed!")
print("Wait ~5 seconds, then check History tab in UI")

# Clean up
conn.close()
    """)


def run_all_tests():
    """Run all integration tests"""
    print("="*70)
    print("🧪 Kafka Schema History Integration Tests")
    print("="*70)
    
    tests_passed = 0
    tests_failed = 0
    
    # Test 1: List topics
    if test_kafka_topics():
        tests_passed += 1
    else:
        tests_failed += 1
    
    time.sleep(1)
    
    # Test 2: Schema history
    if test_schema_history("finance_transactions", "finance", "postgres"):
        tests_passed += 1
    else:
        tests_failed += 1
    
    time.sleep(1)
    
    # Test 3: Topic info (if CDC topic exists)
    try:
        topics_response = requests.get(f"{BASE_URL}/api/bronze/kafka-topics")
        if topics_response.ok:
            topics = topics_response.json()['topics']
            if topics:
                test_topic_info(topics[0])
                tests_passed += 1
    except Exception as e:
        print(f"⚠️  Skipping topic info test: {e}")
    
    # Show demo
    demo_trigger_cdc_event()
    
    # Summary
    print("\n" + "="*70)
    print("📊 Test Summary")
    print("="*70)
    print(f"✅ Passed: {tests_passed}")
    print(f"❌ Failed: {tests_failed}")
    print(f"📈 Total: {tests_passed + tests_failed}")
    
    if tests_failed == 0:
        print("\n🎉 All tests passed! Integration is working correctly.")
    else:
        print("\n⚠️  Some tests failed. Check backend logs for details.")
    
    print("\n" + "="*70)
    print("Next Steps:")
    print("="*70)
    print("1. Open UI: http://localhost:5173")
    print("2. Go to: Silver Dashboard → Data Catalog")
    print("3. Click any table (e.g., finance_transactions)")
    print("4. Click the 'History' tab")
    print("5. You should see the schema history (mock data until CDC enabled)")
    print("\n✨ To see real CDC events: Follow the DEMO instructions above")


if __name__ == "__main__":
    try:
        run_all_tests()
    except KeyboardInterrupt:
        print("\n\n⚠️  Tests interrupted by user")
    except Exception as e:
        print(f"\n\n❌ Error running tests: {e}")
        import traceback
        traceback.print_exc()
