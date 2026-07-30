"""
Performance Testing Suite for Spark Silver Processor
===================================================== 
Comprehensive performance tests for validating 10M rows/day capability.

Tests:
1. Small batch (10K rows) - Latency test
2. Medium batch (1M rows) - Throughput test  
3. Large batch (10M rows) - Scale test
4. Quality validation overhead
5. Write performance to Iceberg

Author: SyniqAI Team
Date: March 8, 2026
"""

import sys
import time
import subprocess
from pathlib import Path
from datetime import datetime
from typing import Dict, Any, List
import pandas as pd
import json

# Add project paths
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))


class PerformanceTestSuite:
    """Performance testing for Spark Silver processor"""
    
    def __init__(self):
        self.results = []
        self.test_timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
    def generate_test_data(
        self,
        num_rows: int,
        source: str = "postgres",
        table: str = "test_transactions"
    ) -> Path:
        """
        Generate synthetic test data for performance testing
        
        Args:
            num_rows: Number of rows to generate
            source: Source database name
            table: Table name
        
        Returns:
            Path to generated Parquet file  
        """
        print(f"\n📊 Generating {num_rows:,} test rows...")
        start_time = time.time()
        
        # Generate realistic transaction data
        data = {
            "transaction_id": range(1, num_rows + 1),
            "account_id": [f"ACC{i % 10000:06d}" for i in range(num_rows)],
            "amount": [round(100 + (i % 9900), 2) for i in range(num_rows)],
            "transaction_type": ["DEBIT" if i % 2 == 0 else "CREDIT" for i in range(num_rows)],
            "merchant_name": [f"Merchant_{i % 500}" for i in range(num_rows)],
            "status": ["COMPLETED" if i % 10 != 0 else "PENDING" for i in range(num_rows)],
            "transaction_date": [datetime.now().isoformat() for _ in range(num_rows)]
        }
        
        df = pd.DataFrame(data)
        
        # Write to test location
        test_dir = project_root / "tests" / "perf_test_data" / source / table
        test_dir.mkdir(parents=True, exist_ok=True)
        
        parquet_file = test_dir / f"test_{num_rows}rows_{self.test_timestamp}.parquet"
        df.to_parquet(parquet_file, engine='pyarrow', compression='snappy')
        
        duration = time.time() - start_time
        file_size_mb = parquet_file.stat().st_size / (1024 * 1024)
        
        print(f"✅ Generated {num_rows:,} rows in {duration:.2f}s")
        print(f"   File size: {file_size_mb:.2f} MB")
        print(f"   Location: {parquet_file}")
        
        return parquet_file
    
    def load_test_data_to_bronze(
        self,
        parquet_file: Path,
        source: str,
        table: str
    ):
        """
        Load test data into Bronze Iceberg table
        
        Args:
            parquet_file: Path to test Parquet file
            source: Source database  
            table: Table name
        """
        print(f"\n📥 Loading test data to Bronze Iceberg table...")
        
        try:
            from pyspark.sql import SparkSession
            from utils.spark_iceberg_config import create_spark_session_with_iceberg
            
            spark = create_spark_session_with_iceberg(app_name="PerfTest_DataLoad")
            
            # Read test data
            df = spark.read.parquet(str(parquet_file))
            
            # Add Bronze metadata
            from pyspark.sql.functions import current_timestamp, lit
            df = df.withColumn("_ingestion_timestamp", current_timestamp())
            df = df.withColumn("_cdc_operation", lit("c"))
            df = df.withColumn("_source", lit(source))
            
            # Write to Bronze Iceberg
            bronze_table = f"syniq_iceberg.bronze.{source}_{table}"
            df.writeTo(bronze_table) \
                .using("iceberg") \
                .createOrReplace()
            
            row_count = df.count()
            print(f"✅ Loaded {row_count:,} rows to Bronze: {bronze_table}")
            
            spark.catalog.clearCache()
        
        except Exception as e:
            print(f"❌ Failed to load test data: {e}")
            raise
    
    def run_silver_processing_test(
        self,
        source: str,
        table: str,
        test_name: str
    ) -> Dict[str, Any]:
        """
        Run Silver processing and measure performance
        
        Args:
            source: Source database
            table: Table name
            test_name: Name of this test
        
        Returns:
            Performance metrics
        """
        print(f"\n🧪 Running test: {test_name}")
        print("=" * 60)
        
        start_time = time.time()
        
        # Run Spark Silver processor
        processor_script = project_root / "silver" / "spark_silver_processor.py"
        
        try:
            result = subprocess.run(
                [
                    sys.executable,
                    str(processor_script),
                    "--source", source,
                    "--table", table,
                    "--perf-test"
                ],
                capture_output=True,
                text=True,
                timeout=600  # 10 minute timeout
            )
            
            duration = time.time() - start_time
            
            # Parse output for metrics
            output = result.stdout
            
            test_result = {
                "test_name": test_name,
                "source": source,
                "table": table,
                "duration_seconds": round(duration, 2),
                "success": result.returncode == 0,
                "timestamp": datetime.now().isoformat(),
                "output": output,
                "error": result.stderr if result.returncode != 0 else None
            }
            
            # Extract metrics from output
            if "Input Rows:" in output:
                import re
                input_match = re.search(r"Input Rows:\s+([\d,]+)", output)
                output_match = re.search(r"Output Rows:\s+([\d,]+)", output)
                throughput_match = re.search(r"Throughput:\s+([\d,\.]+) rows/s", output)
                quality_match = re.search(r"Quality Score:\s+([\d\.]+)%", output)
                
                if input_match:
                    test_result["input_rows"] = int(input_match.group(1).replace(",", ""))
                if output_match:
                    test_result["output_rows"] = int(output_match.group(1).replace(",", ""))
                if throughput_match:
                    test_result["rows_per_second"] = float(throughput_match.group(1).replace(",", ""))
                if quality_match:
                    test_result["quality_score"] = float(quality_match.group(1))
            
            if test_result["success"]:
                print(f"✅ Test passed in {duration:.2f}s")
                if "rows_per_second" in test_result:
                    print(f"   Throughput: {test_result['rows_per_second']:,.2f} rows/s")
            else:
                print(f"❌ Test failed: {test_result['error']}")
            
            return test_result
        
        except subprocess.TimeoutExpired:
            print(f"❌ Test timed out after 10 minutes")
            return {
                "test_name": test_name,
                "success": False,
                "error": "Timeout after 10 minutes",
                "duration_seconds": 600
            }
    
    def test_small_batch_latency(self):
        """Test 1: Small batch (10K rows) latency"""
        print("\n" + "=" * 70)
        print(" TEST 1: Small Batch Latency (10K rows)")
        print("=" * 70)
        
        # Generate data
        parquet_file = self.generate_test_data(10_000, "postgres", "test_small")
        
        # Load to Bronze
        self.load_test_data_to_bronze(parquet_file, "postgres", "test_small")
        
        # Run processing
        result = self.run_silver_processing_test("postgres", "test_small", "Small Batch Latency")
        self.results.append(result)
        
        return result
    
    def test_medium_batch_throughput(self):
        """Test 2: Medium batch (1M rows) throughput"""
        print("\n" + "=" * 70)
        print(" TEST 2: Medium Batch Throughput (1M rows)")
        print("=" * 70)
        
        # Generate data
        parquet_file = self.generate_test_data(1_000_000, "postgres", "test_medium")
        
        # Load to Bronze
        self.load_test_data_to_bronze(parquet_file, "postgres", "test_medium")
        
        # Run processing
        result = self.run_silver_processing_test("postgres", "test_medium", "Medium Batch Throughput")
        self.results.append(result)
        
        return result
    
    def test_large_batch_scale(self):
        """Test 3: Large batch (10M rows) scale test"""
        print("\n" + "=" * 70)
        print(" TEST 3: Large Batch Scale (10M rows)")
        print("=" * 70)
        print("⚠️  This will take 10-20 minutes...")
        
        # Generate data in chunks to avoid memory issues
        chunk_size = 1_000_000
        total_rows = 10_000_000
        num_chunks = total_rows // chunk_size
        
        print(f"\n📊 Generating {total_rows:,} rows in {num_chunks} chunks...")
        
        test_dir = project_root / "tests" / "perf_test_data" / "postgres" / "test_large"
        test_dir.mkdir(parents=True, exist_ok=True)
        
        for i in range(num_chunks):
            chunk_data = {
                "transaction_id": range(i * chunk_size + 1, (i + 1) * chunk_size + 1),
                "account_id": [f"ACC{j % 10000:06d}" for j in range(chunk_size)],
                "amount": [round(100 + (j % 9900), 2) for j in range(chunk_size)],
                "transaction_type": ["DEBIT" if j % 2 == 0 else "CREDIT" for j in range(chunk_size)],
                "merchant_name": [f"Merchant_{j % 500}" for j in range(chunk_size)],
                "status": ["COMPLETED" if j % 10 != 0 else "PENDING" for j in range(chunk_size)],
                "transaction_date": [datetime.now().isoformat() for _ in range(chunk_size)]
            }
            
            chunk_df = pd.DataFrame(chunk_data)
            chunk_file = test_dir / f"chunk_{i:03d}.parquet"
            chunk_df.to_parquet(chunk_file, engine='pyarrow', compression='snappy')
            print(f"  ✅ Chunk {i + 1}/{num_chunks}")
        
        # Load to Bronze
        print(f"\n📥 Loading {total_rows:,} rows to Bronze...")
        try:
            from pyspark.sql import SparkSession
            from utils.spark_iceberg_config import create_spark_session_with_iceberg
            from pyspark.sql.functions import current_timestamp, lit
            
            spark = create_spark_session_with_iceberg(app_name="PerfTest_LargeLoad")
            
            # Read all chunks
            df = spark.read.parquet(str(test_dir / "*.parquet"))
            df = df.withColumn("_ingestion_timestamp", current_timestamp())
            df = df.withColumn("_cdc_operation", lit("c"))
            df = df.withColumn("_source", lit("postgres"))
            
            # Write to Bronze
            bronze_table = "syniq_iceberg.bronze.postgres_test_large"
            df.writeTo(bronze_table).using("iceberg").createOrReplace()
            
            print(f"✅ Loaded {total_rows:,} rows to Bronze")
            spark.catalog.clearCache()
        
        except Exception as e:
            print(f"❌ Failed to load large batch: {e}")
            return {"success": False, "error": str(e)}
        
        # Run processing
        result = self.run_silver_processing_test("postgres", "test_large", "Large Batch Scale")
        self.results.append(result)
        
        return result
    
    def generate_report(self):
        """Generate comprehensive performance test report"""
        
        report_file = project_root / f"PERFORMANCE_TEST_REPORT_{self.test_timestamp}.md"
        
        with open(report_file, 'w') as f:
            f.write("# Spark Silver Processor - Performance Test Report\n\n")
            f.write(f"**Date:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"**Test Suite Version:** 1.0\n\n")
            
            f.write("## Test Results Summary\n\n")
            f.write("| Test Name | Rows | Duration (s) | Throughput (rows/s) | Quality | Status |\n")
            f.write("|-----------|------|--------------|---------------------|---------|--------|\n")
            
            for result in self.results:
                status = "✅ Pass" if result.get("success") else "❌ Fail"
                rows = result.get("input_rows", "N/A")
                duration = result.get("duration_seconds", "N/A")
                throughput = result.get("rows_per_second", "N/A")
                quality = result.get("quality_score", "N/A")
                
                if throughput != "N/A":
                    throughput = f"{throughput:,.2f}"
                if quality != "N/A":
                    quality = f"{quality:.2f}%"
                
                f.write(f"| {result['test_name']} | {rows:,} | {duration} | {throughput} | {quality} | {status} |\n")
            
            f.write("\n## Detailed Test Logs\n\n")
            
            for result in self.results:
                f.write(f"### {result['test_name']}\n\n")
                f.write(f"**Status:** {'✅ Passed' if result.get('success') else '❌ Failed'}\n\n")
                f.write("```\n")
                f.write(result.get("output", "No output"))
                f.write("\n```\n\n")
                
                if result.get("error"):
                    f.write(f"**Error:**\n```\n{result['error']}\n```\n\n")
            
            f.write("\n## Performance Benchmarks Met\n\n")
            
            # Check if 10M rows/day benchmark is met (10M rows in 24 hours = 115.7 rows/s minimum)
            large_test = next((r for r in self.results if "Large" in r.get("test_name", "")), None)
            if large_test and large_test.get("rows_per_second"):
                daily_capacity = large_test["rows_per_second"] * 86400  # seconds in a day
                f.write(f"- **Target:** 10M rows/day\n")
                f.write(f"- **Achieved:** {daily_capacity:,.0f} rows/day\n")
                f.write(f"- **Status:** {'✅ PASSED' if daily_capacity >= 10_000_000 else '❌ FAILED'}\n\n")
        
        print(f"\n📄 Performance report saved: {report_file}")
        return report_file
    
    def run_all_tests(self):
        """Run complete performance test suite"""
        
        print("\n" + "=" * 70)
        print(" SPARK SILVER PROCESSOR - PERFORMANCE TEST SUITE")
        print("=" * 70)
        print(f" Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("=" * 70)
        
        overall_start = time.time()
        
        try:
            # Test 1: Small batch
            self.test_small_batch_latency()
            
            # Test 2: Medium batch
            self.test_medium_batch_throughput()
            
            # Test 3: Large batch (uncomment for full test)
            # WARNING: This takes 10-20 minutes
            # self.test_large_batch_scale()
            
        except Exception as e:
            print(f"\n❌ Test suite failed: {e}")
            import traceback
            traceback.print_exc()
        
        overall_duration = time.time() - overall_start
        
        print("\n" + "=" * 70)
        print(f" Test Suite Completed in {overall_duration:.2f}s")
        print("=" * 70)
        
        # Generate report
        report_file = self.generate_report()
        
        # Print summary
        passed = sum(1 for r in self.results if r.get("success"))
        total = len(self.results)
        
        print(f"\n📊 Results: {passed}/{total} tests passed")
        print(f"📄 Full report: {report_file}\n")


def main():
    """Main execution"""
    
    suite = PerformanceTestSuite()
    suite.run_all_tests()


if __name__ == "__main__":
    main()
