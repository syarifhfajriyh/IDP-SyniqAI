"""
Gold Layer Transformation Engine with PySpark + Iceberg
Handles aggregations, joins, and dimensional modeling for Silver → Gold
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, sum as spark_sum, count, avg, max as spark_max, min as spark_min, countDistinct
from pyspark.sql.functions import current_timestamp, datediff, current_date, when, lit
from typing import Dict, List, Optional, Any, Tuple
import logging
from datetime import datetime

logger = logging.getLogger(__name__)


class GoldTransformationEngine:
    """Enterprise Gold transformation engine using PySpark + Iceberg"""
    
    def __init__(self, spark: SparkSession,
                 minio_endpoint: str = "localhost:9000",
                 minio_access_key: str = "admin",
                 minio_secret_key: str = "password123"):
        self.spark = spark
        self.minio_endpoint = minio_endpoint
        self.minio_access_key = minio_access_key
        self.minio_secret_key = minio_secret_key
        
        # Configure Spark for Iceberg + MinIO
        self._configure_spark()
    
    def _configure_spark(self):
        """Configure Spark for Iceberg and S3/MinIO access"""
        spark_conf = self.spark.sparkContext.getConf()
        
        # S3A configurations for MinIO
        spark_conf.set("spark.hadoop.fs.s3a.endpoint", f"http://{self.minio_endpoint}")
        spark_conf.set("spark.hadoop.fs.s3a.access.key", self.minio_access_key)
        spark_conf.set("spark.hadoop.fs.s3a.secret.key", self.minio_secret_key)
        spark_conf.set("spark.hadoop.fs.s3a.path.style.access", "true")
        spark_conf.set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        spark_conf.set("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        
        logger.info("✓ Spark configured for Iceberg + MinIO (Gold Engine)")
    
    def execute_aggregation(self, 
                           silver_table: str,
                           gold_table: str,
                           group_by_cols: List[str],
                           aggregations: List[Dict[str, Any]],
                           filters: Optional[List[Dict[str, Any]]] = None) -> Dict[str, Any]:
        """
        Execute aggregation transformation: Silver → Gold
        
        Args:
            silver_table: Source Silver Iceberg table (e.g., 'syniqai_silver.finance.finance_transactions')
            gold_table: Target Gold Iceberg table
            group_by_cols: Columns to group by
            aggregations: [{"column": "amount", "function": "sum", "alias": "total_revenue"}]
            filters: Optional pre-aggregation filters
        
        Returns:
            Transformation report
        """
        start_time = datetime.now()
        
        logger.info(f"🏆 Starting Gold aggregation: {silver_table} → {gold_table}")
        
        try:
            # 1. Read from Silver Iceberg table
            silver_df = self.spark.table(silver_table)
            input_rows = silver_df.count()
            
            logger.info(f"📊 Read {input_rows} rows from Silver")
            
            # 2. Apply filters if provided
            if filters:
                for f in filters:
                    col_name = f["column"]
                    operator = f["operator"]
                    value = f["value"]
                    
                    if operator == "=":
                        silver_df = silver_df.filter(col(col_name) == value)
                    elif operator == ">":
                        silver_df = silver_df.filter(col(col_name) > value)
                    elif operator == "<":
                        silver_df = silver_df.filter(col(col_name) < value)
                    elif operator == ">=":
                        silver_df = silver_df.filter(col(col_name) >= value)
                    elif operator == "<=":
                        silver_df = silver_df.filter(col(col_name) <= value)
                    elif operator == "!=":
                        silver_df = silver_df.filter(col(col_name) != value)
                
                logger.info(f"✓ Applied {len(filters)} filter(s)")
            
            # 3. Build aggregation expressions
            agg_exprs = []
            for agg in aggregations:
                col_name = agg["column"]
                func = agg["function"].lower()
                alias = agg.get("alias", f"{func}_{col_name}")
                
                if func == "sum":
                    agg_exprs.append(spark_sum(col(col_name)).alias(alias))
                elif func == "count":
                    agg_exprs.append(count(col(col_name)).alias(alias))
                elif func == "avg":
                    agg_exprs.append(avg(col(col_name)).alias(alias))
                elif func == "max":
                    agg_exprs.append(spark_max(col(col_name)).alias(alias))
                elif func == "min":
                    agg_exprs.append(spark_min(col(col_name)).alias(alias))
                elif func == "count_distinct":
                    agg_exprs.append(countDistinct(col(col_name)).alias(alias))
                else:
                    logger.warning(f"Unknown aggregation function: {func}")
            
            # 4. Perform GROUP BY aggregation
            gold_df = silver_df.groupBy(*group_by_cols).agg(*agg_exprs)
            
            # 5. Add Gold metadata
            gold_df = gold_df.withColumn("_gold_created_at", current_timestamp())
            
            output_rows = gold_df.count()
            
            logger.info(f"✓ Aggregation complete: {input_rows} → {output_rows} rows")
            
            # 6. Write to Gold Iceberg table
            gold_df.writeTo(gold_table).using("iceberg").createOrReplace()
            
            logger.info(f"✅ Gold table created: {gold_table}")
            
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            
            return {
                "success": True,
                "silver_table": silver_table,
                "gold_table": gold_table,
                "transformation_type": "aggregation",
                "input_rows": input_rows,
                "output_rows": output_rows,
                "group_by_columns": group_by_cols,
                "aggregations_count": len(aggregations),
                "duration_seconds": duration,
                "start_time": start_time.isoformat(),
                "end_time": end_time.isoformat()
            }
            
        except Exception as e:
            logger.error(f"❌ Aggregation failed: {e}")
            raise
    
    def execute_join(self,
                     tables: List[Dict[str, str]],
                     join_conditions: List[Dict[str, Any]],
                     select_columns: List[str],
                     gold_table: str) -> Dict[str, Any]:
        """
        Execute multi-table join transformation
        
        Args:
            tables: [{"table": "silver.customers", "alias": "c"}, {"table": "silver.orders", "alias": "o"}]
            join_conditions: [{"left": "c.customer_id", "right": "o.customer_id", "type": "INNER"}]
            select_columns: Columns to include in output (with aliases, e.g., "c.customer_id", "o.order_date")
            gold_table: Target Gold table
        
        Returns:
            Transformation report
        """
        start_time = datetime.now()
        
        logger.info(f"🏆 Starting Gold join transformation → {gold_table}")
        
        try:
            # 1. Load all tables with aliases
            dfs = {}
            source_tables = []
            
            for tbl_info in tables:
                table_name = tbl_info["table"]
                alias = tbl_info["alias"]
                
                df = self.spark.table(table_name)
                dfs[alias] = df
                source_tables.append(table_name)
                
                logger.info(f"  ✓ Loaded {table_name} as {alias} ({df.count()} rows)")
            
            # 2. Start with the first table
            base_alias = tables[0]["alias"]
            result_df = dfs[base_alias]
            
            # 3. Apply joins sequentially
            for join_cond in join_conditions:
                left_col = join_cond["left"]  # e.g., "c.customer_id"
                right_col = join_cond["right"]  # e.g., "o.customer_id"
                join_type = join_cond.get("type", "inner").lower()
                
                # Extract alias from right column
                right_alias = right_col.split(".")[0]
                right_df = dfs[right_alias]
                
                # Perform join
                result_df = result_df.join(
                    right_df,
                    col(left_col) == col(right_col),
                    join_type
                )
                
                logger.info(f"  ✓ Joined {right_alias} (type: {join_type})")
            
            # 4. Select specified columns
            result_df = result_df.selectExpr(*select_columns)
            
            # 5. Add Gold metadata
            result_df = result_df.withColumn("_gold_created_at", current_timestamp())
            
            output_rows = result_df.count()
            
            logger.info(f"✓ Join complete: {len(tables)} tables → {output_rows} rows")
            
            # 6. Write to Gold Iceberg table
            result_df.writeTo(gold_table).using("iceberg").createOrReplace()
            
            logger.info(f"✅ Gold table created: {gold_table}")
            
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            
            return {
                "success": True,
                "source_tables": source_tables,
                "gold_table": gold_table,
                "transformation_type": "join",
                "tables_joined": len(tables),
                "output_rows": output_rows,
                "join_conditions_count": len(join_conditions),
                "duration_seconds": duration,
                "start_time": start_time.isoformat(),
                "end_time": end_time.isoformat()
            }
            
        except Exception as e:
            logger.error(f"❌ Join transformation failed: {e}")
            raise
    
    def execute_customer_segmentation(self,
                                     silver_table: str,
                                     gold_table: str) -> Dict[str, Any]:
        """
        Execute RFM (Recency, Frequency, Monetary) customer segmentation
        
        Args:
            silver_table: Source Silver transaction table
            gold_table: Target Gold customer segments table
        
        Returns:
            Transformation report
        """
        start_time = datetime.now()
        
        logger.info(f"🏆 Starting customer segmentation: {silver_table} → {gold_table}")
        
        try:
            # 1. Read from Silver
            silver_df = self.spark.table(silver_table)
            input_rows = silver_df.count()
            
            # 2. Calculate RFM metrics per customer
            rfm_df = silver_df.groupBy("USER_ID").agg(
                spark_max("TRANSACTION_DATE").alias("last_purchase_date"),
                count("*").alias("purchase_frequency"),
                spark_sum("AMOUNT").alias("total_spent"),
                avg("AMOUNT").alias("avg_order_value")
            )
            
            # 3. Calculate days since last purchase
            rfm_df = rfm_df.withColumn(
                "days_since_last_purchase",
                datediff(current_date(), col("last_purchase_date"))
            )
            
            # 4. Assign customer segments based on RFM
            rfm_df = rfm_df.withColumn(
                "customer_segment",
                when((col("total_spent") > 1000) & (col("purchase_frequency") > 10), "VIP")
                .when((col("total_spent") > 500) & (col("purchase_frequency") > 5), "Premium")
                .when((col("total_spent") > 100) & (col("purchase_frequency") > 2), "Regular")
                .otherwise("New")
            )
            
            # 5. Add Gold metadata
            rfm_df = rfm_df.withColumn("_gold_created_at", current_timestamp())
            
            output_rows = rfm_df.count()
            
            logger.info(f"✓ Segmentation complete: {input_rows} transactions → {output_rows} customers")
            
            # 6. Write to Gold Iceberg table
            rfm_df.writeTo(gold_table).using("iceberg").createOrReplace()
            
            logger.info(f"✅ Gold table created: {gold_table}")
            
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            
            return {
                "success": True,
                "silver_table": silver_table,
                "gold_table": gold_table,
                "transformation_type": "customer_segmentation",
                "input_rows": input_rows,
                "output_rows": output_rows,
                "segments": ["VIP", "Premium", "Regular", "New"],
                "duration_seconds": duration,
                "start_time": start_time.isoformat(),
                "end_time": end_time.isoformat()
            }
            
        except Exception as e:
            logger.error(f"❌ Customer segmentation failed: {e}")
            raise
