"""
End-to-End Integration Test Suite
==================================
Tests the complete data pipeline:
    PostgreSQL → Kafka → CDC Consumer → Bronze → Spark Silver → Quality Metrics → API

Scenarios tested:
1. CDC Bronze Ingestion (< 30s latency)
2. Spark Silver Processing (quality validation)
3. Quality metrics storage (PostgreSQL)
4. API endpoint accessibility
5. GUI data retrieval

Usage:
    # Run all tests
    python tests/integration_test_suite.py
    
    # Run specific scenario
    python tests/integration_test_suite.py --scenario bronze
    python tests/integration_test_suite.py --scenario silver
    python tests/integration_test_suite.py --scenario api

Requirements:
    - PostgreSQL running (source + metadata)
    - Kafka + Debezium running
    - CDC consumer running
    - MinIO running
    - FastAPI backend running (port 8000)
"""

import sys
import time
import json
import argparse
import logging
import psycopg2
import requests
from pathlib import Path
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple

# Add parent directory to path
sys.path.append(str(Path(__file__).parent.parent))

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


# ============================================================================
# Test Configuration
# ============================================================================

class TestConfig:
    """Configuration for integration tests."""
    
    # Database connections
    SOURCE_DB_HOST = "localhost"
    SOURCE_DB_PORT = 5432
    SOURCE_DB_NAME = "syniq_postgres"
    SOURCE_DB_USER = "postgres"
    SOURCE_DB_PASS = "postgres"
    
    METADATA_DB_HOST = "localhost"
    METADATA_DB_PORT = 5432
    METADATA_DB_NAME = "syniqai_metadata"
    METADATA_DB_USER = "syniqai_user"
    METADATA_DB_PASS = "syniqai_pass"
    
    # API configuration
    API_BASE_URL = "http://localhost:8000"
    
    # Test thresholds
    BRONZE_LATENCY_THRESHOLD = 30  # seconds
    SILVER_LATENCY_THRESHOLD = 120  # seconds
    MIN_QUALITY_SCORE = 0.95  # 95%
    
    # Test data
    TEST_TABLE = "transactions"
    TEST_TRANSACTION_ID = f"TEST_{int(time.time())}"


# ============================================================================
# Helper Functions
# ============================================================================

def get_source_db_connection():
    """Connect to source PostgreSQL database."""
    return psycopg2.connect(
        host=TestConfig.SOURCE_DB_HOST,
        port=TestConfig.SOURCE_DB_PORT,
        database=TestConfig.SOURCE_DB_NAME,
        user=TestConfig.SOURCE_DB_USER,
        password=TestConfig.SOURCE_DB_PASS
    )


def get_metadata_db_connection():
    """Connect to metadata PostgreSQL database."""
    return psycopg2.connect(
        host=TestConfig.METADATA_DB_HOST,
        port=TestConfig.METADATA_DB_PORT,
        database=TestConfig.METADATA_DB_NAME,
        user=TestConfig.METADATA_DB_USER,
        password=TestConfig.METADATA_DB_PASS
    )


def api_request(endpoint: str, method: str = "GET", **kwargs) -> Dict:
    """Make API request."""
    url = f"{TestConfig.API_BASE_URL}{endpoint}"
    
    try:
        if method == "GET":
            response = requests.get(url, **kwargs)
        elif method == "POST":
            response = requests.post(url, **kwargs)
        else:
            raise ValueError(f"Unsupported method: {method}")
        
        response.raise_for_status()
        return response.json()
        
    except Exception as e:
        logger.error(f"API request failed: {url} - {e}")
        raise


def print_section_header(title: str):
    """Print formatted section header."""
    print("\n" + "=" * 70)
    print(f" {title}")
    print("=" * 70)


def print_test_result(test_name: str, passed: bool, details: str = ""):
    """Print test result."""
    status = "✅ PASS" if passed else "❌ FAIL"
    print(f"{status} - {test_name}")
    if details:
        print(f"      {details}")


# ============================================================================
# Test Scenarios
# ============================================================================

class IntegrationTestSuite:
    """Main test suite class."""
    
    def __init__(self):
        self.results = {
            "total": 0,
            "passed": 0,
            "failed": 0,
            "tests": []
        }
        self.start_time = time.time()
    
    
    def record_test(self, name: str, passed: bool, duration: float, details: str = ""):
        """Record test result."""
        self.results["total"] += 1
        if passed:
            self.results["passed"] += 1
        else:
            self.results["failed"] += 1
        
        self.results["tests"].append({
            "name": name,
            "passed": passed,
            "duration": duration,
            "details": details
        })
        
        print_test_result(name, passed, details)
    
    
    def test_prerequisites(self) -> bool:
        """Test 0: Check all prerequisites are running."""
        print_section_header("TEST 0: Prerequisites Check")
        
        all_passed = True
        
        # Test PostgreSQL source
        test_start = time.time()
        try:
            conn = get_source_db_connection()
            conn.close()
            self.record_test(
                "PostgreSQL Source Database",
                True,
                time.time() - test_start,
                f"Connected to {TestConfig.SOURCE_DB_NAME}"
            )
        except Exception as e:
            self.record_test(
                "PostgreSQL Source Database",
                False,
                time.time() - test_start,
                f"Connection failed: {e}"
            )
            all_passed = False
        
        # Test PostgreSQL metadata
        test_start = time.time()
        try:
            conn = get_metadata_db_connection()
            conn.close()
            self.record_test(
                "PostgreSQL Metadata Database",
                True,
                time.time() - test_start,
                f"Connected to {TestConfig.METADATA_DB_NAME}"
            )
        except Exception as e:
            self.record_test(
                "PostgreSQL Metadata Database",
                False,
                time.time() - test_start,
                f"Connection failed: {e}"
            )
            all_passed = False
        
        # Test MinIO
        test_start = time.time()
        try:
            resp = requests.get("http://localhost:9000/minio/health/live", timeout=5)
            passed = resp.status_code == 200
            self.record_test(
                "MinIO Object Storage",
                passed,
                time.time() - test_start,
                "MinIO is accessible" if passed else f"Status code: {resp.status_code}"
            )
            all_passed = all_passed and passed
        except Exception as e:
            self.record_test(
                "MinIO Object Storage",
                False,
                time.time() - test_start,
                f"Connection failed: {e}"
            )
            all_passed = False
        
        # Test API
        test_start = time.time()
        try:
            data = api_request("/")
            passed = True
            self.record_test(
                "FastAPI Backend",
                passed,
                time.time() - test_start,
                f"API running on {TestConfig.API_BASE_URL}"
            )
        except Exception as e:
            self.record_test(
                "FastAPI Backend",
                False,
                time.time() - test_start,
                f"API not accessible: {e}"
            )
            all_passed = False
        
        # Test CDC Consumer
        test_start = time.time()
        try:
            data = api_request("/api/cdc/consumer/status")
            is_running = data.get("consumer", {}).get("running", False)
            self.record_test(
                "CDC Streaming Consumer",
                is_running,
                time.time() - test_start,
                f"PID: {data['consumer'].get('pid')}" if is_running else "Not running - start with start_cdc_streaming.ps1"
            )
            if not is_running:
                logger.warning("⚠️  CDC consumer not running. Some tests will fail.")
        except Exception as e:
            self.record_test(
                "CDC Streaming Consumer",
                False,
                time.time() - test_start,
                f"Status check failed: {e}"
            )
        
        return all_passed
    
    
    def test_bronze_ingestion(self) -> Tuple[bool, str]:
        """Test 1: CDC Bronze ingestion latency."""
        print_section_header("TEST 1: CDC Bronze Ingestion (< 30s latency)")
        
        test_start = time.time()
        test_transaction_id = TestConfig.TEST_TRANSACTION_ID
        
        try:
            # Step 1: Insert test record into source database
            logger.info(f"Inserting test transaction: {test_transaction_id}")
            conn = get_source_db_connection()
            cursor = conn.cursor()
            
            cursor.execute(f"""
                INSERT INTO {TestConfig.TEST_TABLE} 
                (transaction_id, user_id, amount, transaction_date, merchant_name, status)
                VALUES (%s, %s, %s, NOW(), %s, %s)
            """, (test_transaction_id, "TEST_USER_001", 99.99, "Test Merchant", "completed"))
            
            conn.commit()
            insert_time = time.time()
            cursor.close()
            conn.close()
            
            logger.info(f"✓ Test record inserted at {datetime.now().isoformat()}")
            
            # Step 2: Wait and check Bronze Iceberg table
            logger.info("Waiting for CDC to propagate to Bronze...")
            
            max_wait = TestConfig.BRONZE_LATENCY_THRESHOLD
            check_interval = 2  # seconds
            elapsed = 0
            found = False
            
            sys.path.append(str(Path(__file__).parent.parent / "data lakehouse" / "syniq_project"))
            from utils.spark_iceberg_config import create_spark_session_with_iceberg
            
            spark = create_spark_session_with_iceberg()
            
            while elapsed < max_wait and not found:
                try:
                    # Query Bronze table
                    bronze_df = spark.read.format("iceberg").table(
                        f"syniq_iceberg.bronze.postgres_{TestConfig.TEST_TABLE}"
                    )
                    
                    # Look for our test transaction
                    result = bronze_df.filter(
                        bronze_df.transaction_id == test_transaction_id
                    ).count()
                    
                    if result > 0:
                        found = True
                        latency = time.time() - insert_time
                        logger.info(f"✓ Test record found in Bronze! Latency: {latency:.2f}s")
                        break
                    
                except Exception as query_error:
                    logger.warning(f"Query attempt failed: {query_error}")
                
                time.sleep(check_interval)
                elapsed += check_interval
                logger.info(f"  Waiting... {elapsed}s / {max_wait}s")
            
            spark.stop()
            
            # Step 3: Verify result
            if found:
                latency = time.time() - insert_time
                passed = latency < TestConfig.BRONZE_LATENCY_THRESHOLD
                self.record_test(
                    "Bronze CDC Ingestion Latency",
                    passed,
                    time.time() - test_start,
                    f"Latency: {latency:.2f}s (threshold: {TestConfig.BRONZE_LATENCY_THRESHOLD}s)"
                )
                return passed, test_transaction_id
            else:
                self.record_test(
                    "Bronze CDC Ingestion Latency",
                    False,
                    time.time() - test_start,
                    f"Test record not found in Bronze after {max_wait}s"
                )
                return False, test_transaction_id
        
        except Exception as e:
            self.record_test(
                "Bronze CDC Ingestion Latency",
                False,
                time.time() - test_start,
                f"Test failed: {e}"
            )
            logger.error(f"Bronze ingestion test failed: {e}")
            import traceback
            traceback.print_exc()
            return False, test_transaction_id
    
    
    def test_silver_processing(self, test_transaction_id: str) -> bool:
        """Test 2: Spark Silver processing with quality validation."""
        print_section_header("TEST 2: Spark Silver Processing")
        
        test_start = time.time()
        
        try:
            # Trigger Silver processing via API
            logger.info("Triggering Spark Silver processing via API...")
            
            response = api_request(
                "/api/silver/process-spark",
                method="POST",
                json={
                    "source": "postgres",
                    "entity": TestConfig.TEST_TABLE,
                    "domain": "finance",
                    "execution_mode": "incremental",
                    "data_type": "structured"
                },
                timeout=TestConfig.SILVER_LATENCY_THRESHOLD
            )
            
            if not response.get("success"):
                self.record_test(
                    "Silver Processing Trigger",
                    False,
                    time.time() - test_start,
                    f"API returned error: {response.get('error')}"
                )
                return False
            
            job_id = response.get("job_id")
            logger.info(f"✓ Silver processing started (Job ID: {job_id})")
            
            # Wait for job completion
            max_wait = TestConfig.SILVER_LATENCY_THRESHOLD
            check_interval = 5
            elapsed = 0
            completed = False
            
            while elapsed < max_wait:
                status_response = api_request(f"/api/silver/status/{job_id}")
                
                status = status_response.get("job", {}).get("status")
                
                if status == "completed":
                    completed = True
                    break
                elif status == "failed":
                    self.record_test(
                        "Silver Processing Execution",
                        False,
                        time.time() - test_start,
                        f"Job failed: {status_response.get('error')}"
                    )
                    return False
                
                time.sleep(check_interval)
                elapsed += check_interval
                logger.info(f"  Job status: {status} ({elapsed}s / {max_wait}s)")
            
            if completed:
                duration = time.time() - test_start
                self.record_test(
                    "Silver Processing Execution",
                    True,
                    duration,
                    f"Processing completed in {duration:.2f}s"
                )
                return True
            else:
                self.record_test(
                    "Silver Processing Execution",
                    False,
                    time.time() - test_start,
                    f"Job did not complete within {max_wait}s"
                )
                return False
        
        except Exception as e:
            self.record_test(
                "Silver Processing Execution",
                False,
                time.time() - test_start,
                f"Test failed: {e}"
            )
            logger.error(f"Silver processing test failed: {e}")
            import traceback
            traceback.print_exc()
            return False
    
    
    def test_quality_metrics(self) -> bool:
        """Test 3: Quality metrics storage and retrieval."""
        print_section_header("TEST 3: Quality Metrics Storage")
        
        test_start = time.time()
        
        try:
            # Query quality metrics from PostgreSQL
            conn = get_metadata_db_connection()
            cursor = conn.cursor()
            
            cursor.execute(f"""
                SELECT source, table_name, quality_score, passed, validation_timestamp
                FROM quality_metrics
                WHERE source = 'postgres' AND table_name = %s
                ORDER BY validation_timestamp DESC
                LIMIT 1
            """, (TestConfig.TEST_TABLE,))
            
            row = cursor.fetchone()
            cursor.close()
            conn.close()
            
            if row:
                source, table_name, quality_score, passed, validation_timestamp = row
                
                details = f"Score: {quality_score:.2%}, Passed: {passed}, Time: {validation_timestamp}"
                
                score_ok = float(quality_score) >= TestConfig.MIN_QUALITY_SCORE
                
                self.record_test(
                    "Quality Metrics Stored",
                    True,
                    time.time() - test_start,
                    details
                )
                
                self.record_test(
                    "Quality Score Threshold",
                    score_ok,
                    0,
                    f"Score {quality_score:.2%} {'≥' if score_ok else '<'} {TestConfig.MIN_QUALITY_SCORE:.0%}"
                )
                
                return True
            else:
                self.record_test(
                    "Quality Metrics Stored",
                    False,
                    time.time() - test_start,
                    "No quality metrics found in database"
                )
                return False
        
        except Exception as e:
            self.record_test(
                "Quality Metrics Stored",
                False,
                time.time() - test_start,
                f"Test failed: {e}"
            )
            logger.error(f"Quality metrics test failed: {e}")
            return False
    
    
    def test_api_endpoints(self) -> bool:
        """Test 4: API endpoint accessibility."""
        print_section_header("TEST 4: API Endpoints")
        
        endpoints_to_test = [
            ("/api/cdc/consumer/status", "GET", "CDC Consumer Status"),
            ("/api/cdc/consumer/health", "GET", "CDC Health Check"),
            ("/api/cdc/consumer/metrics", "GET", "CDC Metrics"),
            ("/api/silver/tables", "GET", "Silver Tables List"),
            ("/api/silver/iceberg-tables", "GET", "Iceberg Tables List"),
        ]
        
        all_passed = True
        
        for endpoint, method, name in endpoints_to_test:
            test_start = time.time()
            try:
                data = api_request(endpoint, method=method, timeout=10)
                passed = data.get("success", False) or "tables" in data or "consumer" in data
                
                self.record_test(
                    f"API: {name}",
                    passed,
                    time.time() - test_start,
                    f"{endpoint}"
                )
                
                all_passed = all_passed and passed
                
            except Exception as e:
                self.record_test(
                    f"API: {name}",
                    False,
                    time.time() - test_start,
                    f"Failed: {e}"
                )
                all_passed = False
        
        return all_passed
    
    
    def run_all_tests(self):
        """Run all integration tests."""
        print("\n" + "=" * 70)
        print(" SYNIQAI INTEGRATION TEST SUITE")
        print("=" * 70)
        print(f" Started: {datetime.now().isoformat()}")
        print("=" * 70)
        
        # Run tests in sequence
        prereqs_ok = self.test_prerequisites()
        
        if not prereqs_ok:
            logger.error("Prerequisites check failed. Some tests may not run correctly.")
        
        # Bronze ingestion test
        bronze_ok, test_transaction_id = self.test_bronze_ingestion()
        
        # Silver processing test
        if bronze_ok:
            silver_ok = self.test_silver_processing(test_transaction_id)
            
            # Quality metrics test
            if silver_ok:
                self.test_quality_metrics()
        
        # API tests (independent)
        self.test_api_endpoints()
        
        # Print summary
        self.print_summary()
    
    
    def print_summary(self):
        """Print test summary."""
        duration = time.time() - self.start_time
        
        print("\n" + "=" * 70)
        print(" TEST SUMMARY")
        print("=" * 70)
        print(f" Total Tests:  {self.results['total']}")
        print(f" Passed:       {self.results['passed']} ✅")
        print(f" Failed:       {self.results['failed']} ❌")
        print(f" Success Rate: {self.results['passed'] / self.results['total'] * 100:.1f}%")
        print(f" Duration:     {duration:.2f}s")
        print("=" * 70)
        
        # Generate report file
        self.generate_report()
        
        return self.results['failed'] == 0
    
    
    def generate_report(self):
        """Generate markdown report."""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        report_path = Path(__file__).parent / f"INTEGRATION_TEST_REPORT_{timestamp}.md"
        
        with open(report_path, 'w') as f:
            f.write("# Integration Test Report\n\n")
            f.write(f"**Date:** {datetime.now().isoformat()}\n\n")
            f.write(f"**Duration:** {time.time() - self.start_time:.2f}s\n\n")
            f.write(f"## Summary\n\n")
            f.write(f"- Total Tests: {self.results['total']}\n")
            f.write(f"- Passed: {self.results['passed']} ✅\n")
            f.write(f"- Failed: {self.results['failed']} ❌\n")
            f.write(f"- Success Rate: {self.results['passed'] / self.results['total'] * 100:.1f}%\n\n")
            
            f.write("## Test Results\n\n")
            f.write("| Test | Status | Duration | Details |\n")
            f.write("|------|--------|---------|----------|\n")
            
            for test in self.results['tests']:
                status = "✅ PASS" if test['passed'] else "❌ FAIL"
                f.write(f"| {test['name']} | {status} | {test['duration']:.2f}s | {test['details']} |\n")
        
        logger.info(f"✓ Test report saved: {report_path}")


# ============================================================================
# Main
# ============================================================================

def main():
    parser = argparse.ArgumentParser(description="Run SyniqAI integration tests")
    parser.add_argument(
        "--scenario",
        choices=["all", "prerequisites", "bronze", "silver", "api"],
        default="all",
        help="Test scenario to run (default: all)"
    )
    
    args = parser.parse_args()
    
    suite = IntegrationTestSuite()
    
    if args.scenario == "all":
        success = suite.run_all_tests()
    elif args.scenario == "prerequisites":
        success = suite.test_prerequisites()
        suite.print_summary()
    elif args.scenario == "bronze":
        suite.test_prerequisites()
        bronze_ok, _ = suite.test_bronze_ingestion()
        suite.print_summary()
        success = bronze_ok
    elif args.scenario == "silver":
        suite.test_prerequisites()
        bronze_ok, test_id = suite.test_bronze_ingestion()
        if bronze_ok:
            success = suite.test_silver_processing(test_id)
        suite.print_summary()
    elif args.scenario == "api":
        success = suite.test_api_endpoints()
        suite.print_summary()
    
    return 0 if success else 1


if __name__ == "__main__":
    sys.exit(main())
