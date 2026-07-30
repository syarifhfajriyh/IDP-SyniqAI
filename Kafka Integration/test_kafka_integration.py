"""
Test Kafka Integration
======================
Verify that the Kafka integration is working correctly.

Run this after starting the backend:
    python test_kafka_integration.py
"""

import requests
import json
import time
import sys
from datetime import datetime

BASE_URL = "http://localhost:8000"
KAFKA_API = f"{BASE_URL}/api/kafka"

def print_header(text):
    print("\n" + "=" * 70)
    print(f"  {text}")
    print("=" * 70)

def print_result(success, message):
    icon = "[PASS]" if success else "[FAIL]"
    print(f"{icon} {message}")

def test_backend_health():
    """Test that the backend is running"""
    print_header("Test 1: Backend Health")
    try:
        response = requests.get(f"{BASE_URL}/", timeout=5)
        if response.status_code == 200:
            data = response.json()
            print_result(True, f"Backend is running (v{data.get('version', 'unknown')})")
            return True
        else:
            print_result(False, f"Backend returned status {response.status_code}")
            return False
    except Exception as e:
        print_result(False, f"Cannot connect to backend: {e}")
        return False

def test_kafka_health():
    """Test Kafka broker and Kafka Connect connectivity"""
    print_header("Test 2: Kafka Services Health")
    try:
        response = requests.get(f"{KAFKA_API}/health", timeout=10)
        if response.status_code == 200:
            data = response.json()
            
            # Check broker
            broker_status = data.get("kafka_broker", {}).get("status")
            broker_ok = broker_status == "ok"
            print_result(broker_ok, f"Kafka Broker: {data.get('kafka_broker', {}).get('message', 'Unknown')}")
            
            # Check connect
            connect_status = data.get("kafka_connect", {}).get("status")
            connect_ok = connect_status == "ok"
            print_result(connect_ok, f"Kafka Connect: {data.get('kafka_connect', {}).get('message', 'Unknown')}")
            
            return broker_ok and connect_ok
        else:
            print_result(False, f"Health check returned status {response.status_code}")
            return False
    except Exception as e:
        print_result(False, f"Health check failed: {e}")
        return False

def test_bridge_status():
    """Test KafkaBridge in-process status"""
    print_header("Test 3: KafkaBridge Status")
    try:
        response = requests.get(f"{KAFKA_API}/bridge", timeout=5)
        if response.status_code == 200:
            data = response.json()
            available = data.get("available", False)
            print_result(available, f"Bridge Available: {available}")
            
            if available:
                print(f"  • Bootstrap Servers: {data.get('bootstrap_servers')}")
                print(f"  • Topics: {', '.join(data.get('topics', []))}")
                print(f"  • DLQ Topic: {data.get('dlq_topic')}")
                print(f"  • Max Rows/Message: {data.get('max_rows_per_message')}")
            
            return available
        else:
            print_result(False, f"Bridge status returned {response.status_code}")
            return False
    except Exception as e:
        print_result(False, f"Bridge status check failed: {e}")
        return False

def test_list_topics():
    """Test listing Kafka topics"""
    print_header("Test 4: List Kafka Topics")
    try:
        response = requests.get(f"{KAFKA_API}/topics", timeout=10)
        if response.status_code == 200:
            data = response.json()
            topics = data.get("topics", [])
            total = data.get("total", 0)
            
            print_result(True, f"Found {total} topics")
            
            for topic in topics[:5]:  # Show first 5
                name = topic.get("name")
                source = topic.get("source_type", "N/A")
                partitions = topic.get("partitions", 0)
                print(f"  • {name} (source: {source}, partitions: {partitions})")
            
            return True
        else:
            print_result(False, f"Topics list returned {response.status_code}")
            return False
    except Exception as e:
        print_result(False, f"Failed to list topics: {e}")
        return False

def test_kafka_status():
    """Test full Kafka dashboard status"""
    print_header("Test 5: Kafka Dashboard Status")
    try:
        response = requests.get(f"{KAFKA_API}/status", timeout=10)
        if response.status_code == 200:
            data = response.json()
            
            # Health
            health = data.get("health", {})
            broker_ok = health.get("kafka_broker", {}).get("status") == "ok"
            print_result(broker_ok, f"Broker: {'OK' if broker_ok else 'Error'}")
            
            # Topics
            topics_info = data.get("topics", {})
            total_topics = topics_info.get("total", 0)
            total_messages = topics_info.get("total_messages", 0)
            print_result(True, f"Topics: {total_topics}, Total Messages: {total_messages}")
            
            # Connectors
            connectors_info = data.get("connectors", {})
            total_connectors = connectors_info.get("total", 0)
            connector_states = connectors_info.get("by_state", {})
            print_result(True, f"Connectors: {total_connectors} ({connector_states})")
            
            return broker_ok
        else:
            print_result(False, f"Status returned {response.status_code}")
            return False
    except Exception as e:
        print_result(False, f"Status check failed: {e}")
        return False

def main():
    print("\n" + "=" * 70)
    print("=" + " " * 68 + "=")
    print("=  SYINIQ Phase 5 - Kafka Integration Test Suite".ljust(69) + "=")
    print("=" + " " * 68 + "=")
    print("=" * 70)
    
    print(f"\nTest Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Target: {BASE_URL}")
    
    # Run tests
    results = []
    
    results.append(("Backend Health", test_backend_health()))
    
    if results[-1][1]:  # Only continue if backend is healthy
        results.append(("Kafka Health", test_kafka_health()))
        results.append(("Bridge Status", test_bridge_status()))
        results.append(("List Topics", test_list_topics()))
        results.append(("Dashboard Status", test_kafka_status()))
    else:
        print("\n[!] Backend is not running. Skipping Kafka tests.")
        print("  Start the backend with: python gui/api/backend.py")
    
    # Summary
    print_header("Test Summary")
    
    passed = sum(1 for _, result in results if result)
    total = len(results)
    
    for test_name, result in results:
        print_result(result, test_name)
    
    print(f"\n{passed}/{total} tests passed")
    
    if passed == total:
        print("\n*** All tests passed! Kafka integration is working correctly. ***")
        sys.exit(0)
    else:
        print("\n[!] Some tests failed. Check the output above for details.")
        sys.exit(1)

if __name__ == "__main__":
    main()
