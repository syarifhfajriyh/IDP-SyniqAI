"""
CDC Merge Correctness Tests
============================
Exercises SparkCDCConsumer._write_to_iceberg_bronze / _merge_into_iceberg
directly against a local (file-based) Iceberg warehouse -- no live Kafka or
MinIO required. Covers the correctness fixes from the demo-hardening plan:

1. A merge failure routes the batch to the DLQ (_cdc_errors table) and does
   NOT fall back to a blind append (no duplication/corruption on failure).
2. A table with a non-default primary key merges correctly via PK_REGISTRY.
3. An out-of-order update (older _cdc_timestamp) does not overwrite newer
   data already in the table (recency guard).
4. A malformed JSON record is quarantined to the DLQ instead of crashing
   the batch.

Run directly (matches the repo's existing test convention, e.g.
data lakehouse/syniq_project/tests/test_spark_iceberg_setup.py):

    ".venv-run/Scripts/python.exe" "Kafka Integration/tests/test_cdc_merge.py"
"""
import os
import sys
import shutil
import tempfile
from pathlib import Path

if os.name == 'nt' and not os.getenv('HADOOP_HOME'):
    hadoop_home = Path.home() / ".hadoop_home"
    (hadoop_home / "bin").mkdir(parents=True, exist_ok=True)
    os.environ['HADOOP_HOME'] = str(hadoop_home)

# Force Spark's local driver/worker sockets onto loopback. Without this, Spark
# auto-detects the machine's hostname (which can be a VPN/Tailscale name in
# some environments) and local Python workers fail to connect back to it.
os.environ.setdefault('SPARK_LOCAL_HOSTNAME', 'localhost')
os.environ.setdefault('SPARK_LOCAL_IP', '127.0.0.1')

_TESTS_DIR = Path(__file__).resolve().parent
_KAFKA_INTEGRATION_DIR = _TESTS_DIR.parent
if str(_KAFKA_INTEGRATION_DIR) not in sys.path:
    sys.path.insert(0, str(_KAFKA_INTEGRATION_DIR))

from spark_cdc_consumer import SparkCDCConsumer  # noqa: E402


def _cdc_row(spark, key, op, ts_ms, source_db, source_table, data: dict):
    """Build one row matching the schema _write_to_iceberg_bronze expects
    (i.e. the shape df_parsed has in consume_postgres_cdc, before parsing)."""
    import json
    from pyspark.sql import Row
    full_record = json.dumps({**data, "__op": op, "__source_ts_ms": ts_ms})
    return Row(
        key=key,
        op=op,
        source_ts_ms=ts_ms,
        source_db=source_db,
        source_table=source_table,
        full_record=full_record,
        topic=f"cdc.{source_db}.public.{source_table}",
        partition=0,
        offset=0,
        kafka_timestamp=None,
        ingestion_timestamp=None,
    )


def _make_batch_df(spark, rows):
    from pyspark.sql.types import (
        StructType, StructField, StringType, LongType, IntegerType, TimestampType
    )
    schema = StructType([
        StructField("key", StringType(), True),
        StructField("op", StringType(), True),
        StructField("source_ts_ms", LongType(), True),
        StructField("source_db", StringType(), True),
        StructField("source_table", StringType(), True),
        StructField("full_record", StringType(), True),
        StructField("topic", StringType(), True),
        StructField("partition", IntegerType(), True),
        StructField("offset", IntegerType(), True),
        StructField("kafka_timestamp", TimestampType(), True),
        StructField("ingestion_timestamp", TimestampType(), True),
    ])
    from pyspark.sql.functions import current_timestamp
    return spark.createDataFrame(rows, schema).withColumn(
        "ingestion_timestamp", current_timestamp()
    )


def _error_table_count(spark):
    try:
        return spark.table("local.bronze._cdc_errors").count()
    except Exception:
        return 0


def test_malformed_json_routes_to_dlq(consumer, spark):
    from pyspark.sql import Row
    from pyspark.sql.types import (
        StructType, StructField, StringType, LongType, IntegerType, TimestampType
    )
    schema = StructType([
        StructField("key", StringType(), True),
        StructField("op", StringType(), True),
        StructField("source_ts_ms", LongType(), True),
        StructField("source_db", StringType(), True),
        StructField("source_table", StringType(), True),
        StructField("full_record", StringType(), True),
        StructField("topic", StringType(), True),
        StructField("partition", IntegerType(), True),
        StructField("offset", IntegerType(), True),
        StructField("kafka_timestamp", TimestampType(), True),
        StructField("ingestion_timestamp", TimestampType(), True),
    ])
    row = Row(
        key="1", op="c", source_ts_ms=1000, source_db="testdb",
        source_table="badjson", full_record="{not valid json", topic="t",
        partition=0, offset=0, kafka_timestamp=None, ingestion_timestamp=None,
    )
    batch_df = spark.createDataFrame([row], schema)

    before = _error_table_count(spark)
    consumer._write_to_iceberg_bronze(batch_df, batch_id=1)  # must not raise
    after = _error_table_count(spark)

    assert after > before, "malformed JSON batch was not routed to the DLQ"
    assert not consumer._iceberg_table_exists("local.bronze.testdb_badjson"), \
        "a bronze table should not have been created from a malformed batch"


def test_pk_registry_merges_composite_key(consumer, spark):
    consumer.PK_REGISTRY["testdb.custom_pk_table"] = ["order_id", "product_id"]
    try:
        rows = [
            _cdc_row(spark, "1", "c", 1000, "testdb", "custom_pk_table",
                     {"order_id": 1, "product_id": 100, "qty": 5}),
            _cdc_row(spark, "2", "c", 1000, "testdb", "custom_pk_table",
                     {"order_id": 1, "product_id": 200, "qty": 2}),
        ]
        batch_df = _make_batch_df(spark, rows)
        before_errors = _error_table_count(spark)
        consumer._write_to_iceberg_bronze(batch_df, batch_id=2)
        after_errors = _error_table_count(spark)

        assert after_errors == before_errors, "composite-PK merge should not fail"
        count = spark.table("local.bronze.testdb_custom_pk_table").count()
        assert count == 2, f"expected 2 distinct rows for composite PK, got {count}"
    finally:
        consumer.PK_REGISTRY.pop("testdb.custom_pk_table", None)


def test_recency_guard_ignores_stale_update(consumer, spark):
    table = "testdb.recency_table"
    iceberg_table = "local.bronze.testdb_recency_table"

    # Seed the table with a row at t=2000
    seed = _make_batch_df(spark, [
        _cdc_row(spark, "1", "c", 2000, "testdb", "recency_table",
                 {"id": 1, "name": "Alice"}),
    ])
    consumer._write_to_iceberg_bronze(seed, batch_id=3)
    assert spark.table(iceberg_table).filter("id = 1").first()["name"] == "Alice"

    # Out-of-order stale update at t=1000 (older) must NOT overwrite
    stale = _make_batch_df(spark, [
        _cdc_row(spark, "1", "u", 1000, "testdb", "recency_table",
                 {"id": 1, "name": "STALE"}),
    ])
    consumer._write_to_iceberg_bronze(stale, batch_id=4)
    name_after_stale = spark.table(iceberg_table).filter("id = 1").first()["name"]
    assert name_after_stale == "Alice", (
        f"recency guard failed: stale update overwrote newer data (got {name_after_stale!r})"
    )

    # A genuinely newer update at t=3000 must be applied
    fresh = _make_batch_df(spark, [
        _cdc_row(spark, "1", "u", 3000, "testdb", "recency_table",
                 {"id": 1, "name": "NewAlice"}),
    ])
    consumer._write_to_iceberg_bronze(fresh, batch_id=5)
    name_after_fresh = spark.table(iceberg_table).filter("id = 1").first()["name"]
    assert name_after_fresh == "NewAlice", "a genuinely newer update should be applied"


def test_merge_failure_routes_to_dlq_not_append(consumer, spark):
    iceberg_table = "local.bronze.testdb_broken_pk_table"

    # Create the table first (insert path), then poison the PK registry so
    # the follow-up MERGE references a column that doesn't exist -> failure.
    seed = _make_batch_df(spark, [
        _cdc_row(spark, "1", "c", 1000, "testdb", "broken_pk_table",
                 {"id": 1, "name": "Alice"}),
    ])
    consumer._write_to_iceberg_bronze(seed, batch_id=6)
    row_count_before = spark.table(iceberg_table).count()

    consumer.PK_REGISTRY["testdb.broken_pk_table"] = ["does_not_exist"]
    try:
        before_errors = _error_table_count(spark)
        update = _make_batch_df(spark, [
            _cdc_row(spark, "1", "u", 2000, "testdb", "broken_pk_table",
                     {"id": 1, "name": "ShouldNotLand"}),
        ])
        consumer._write_to_iceberg_bronze(update, batch_id=7)  # must not raise
        after_errors = _error_table_count(spark)

        assert after_errors > before_errors, "merge failure was not routed to the DLQ"
        row_count_after = spark.table(iceberg_table).count()
        assert row_count_after == row_count_before, (
            "merge failure must not fall back to a blind append (row count changed: "
            f"{row_count_before} -> {row_count_after})"
        )
        landed = spark.table(iceberg_table).filter("id = 1").first()
        assert landed["name"] == "Alice", "blind-append fallback would have corrupted the row"
    finally:
        consumer.PK_REGISTRY.pop("testdb.broken_pk_table", None)


def main():
    tmp_root = Path(tempfile.mkdtemp(prefix="syniqai_cdc_test_"))
    warehouse = (tmp_root / "iceberg").as_uri()
    checkpoint = str(tmp_root / "checkpoints")

    print(f"Using temp warehouse: {warehouse}")
    consumer = SparkCDCConsumer(
        warehouse_path=warehouse,
        checkpoint_location=checkpoint,
    )

    # Build the session directly (rather than via consumer.create_spark_session())
    # so we can add Windows-local-worker stability settings: local[1] avoids
    # concurrent python-worker socket flakiness, and disabling worker reuse
    # avoids stale daemon sockets across batches in this short-lived process.
    from pyspark.sql import SparkSession
    iceberg_runtime = "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1"
    spark = (SparkSession.builder
        .appName("SYNIQAI-CDC-Merge-Test")
        .config("spark.jars.packages", iceberg_runtime)
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.local.type", "hadoop")
        .config("spark.sql.catalog.local.warehouse", warehouse)
        .config("spark.hadoop.io.native.lib.available", "false")
        .config("spark.python.worker.reuse", "false")
        .config("spark.ui.enabled", "false")
        .master("local[1]")
        .getOrCreate()
    )
    consumer.spark = spark
    spark.sparkContext.setLogLevel("ERROR")

    tests = [
        test_malformed_json_routes_to_dlq,
        test_pk_registry_merges_composite_key,
        test_recency_guard_ignores_stale_update,
        test_merge_failure_routes_to_dlq_not_append,
    ]

    failures = []
    for test_fn in tests:
        name = test_fn.__name__
        try:
            test_fn(consumer, spark)
            print(f"PASS: {name}")
        except AssertionError as e:
            print(f"FAIL: {name}: {e}")
            failures.append(name)
        except Exception as e:
            print(f"ERROR: {name}: {e}")
            failures.append(name)

    spark.stop()
    shutil.rmtree(tmp_root, ignore_errors=True)

    print("\n" + "=" * 60)
    if failures:
        print(f"{len(failures)}/{len(tests)} tests FAILED: {failures}")
        return 1
    print(f"All {len(tests)} tests passed")
    return 0


if __name__ == "__main__":
    sys.exit(main())
