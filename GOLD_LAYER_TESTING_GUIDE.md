# Gold Layer Testing Guide

## Overview
This guide shows how to test the Gold Layer transformations (Aggrega

tions, Joins, Customer Segmentation) with proper lineage recording.

## Prerequisites

1. **Backend Running**: The backend must be running on `http://localhost:8000`
2. **Silver Tables Available**: You need at least one Silver Iceberg table in MinIO
3. **PostgreSQL**: `data_lineage` table should exist

---

## Test 1: Gold Aggregation (Product Performance)

### API Call

```bash
curl -X POST "http://localhost:8000/api/gold/transform/aggregation" \
-H "Content-Type: application/json" \
-d '{
  "silver_table": "syniqai_silver.finance.march_user_transactions",
  "gold_table": "syniqai_gold.finance.product_performance",
  "group_by": ["PRODUCT_NAME", "CATEGORY"],
  "aggregations": [
    {"column": "AMOUNT", "function": "sum", "alias": "total_revenue"},
    {"column": "*", "function": "count", "alias": "transaction_count"},
    {"column": "AMOUNT", "function": "avg", "alias": "avg_price"},
    {"column": "USER_ID", "function": "count_distinct", "alias": "unique_buyers"}
  ],
  "description": "Product performance analysis with revenue and transaction metrics"
}'
```

### Expected Response

```json
{
  "success": true,
  "job_id": "job_abc123xyz",
  "result": {
    "success": true,
    "silver_table": "syniqai_silver.finance.march_user_transactions",
    "gold_table": "syniqai_gold.finance.product_performance",
    "transformation_type": "aggregation",
    "input_rows": 1064,
    "output_rows": 20,
    "group_by_columns": ["PRODUCT_NAME", "CATEGORY"],
    "aggregations_count": 4,
    "duration_seconds": 12.5
  },
  "message": "✅ Gold table created: 20 rows"
}
```

### Verify Results

```sql
-- Check Gold table was created
SELECT * FROM syniqai_gold.finance.product_performance
ORDER BY total_revenue DESC
LIMIT 5;

-- Check lineage was recorded
SELECT source_table, target_table, transformation_type, created_at
FROM data_lineage
WHERE target_layer = 'gold'
  AND transformation_type = 'aggregation'
ORDER BY created_at DESC
LIMIT 1;

-- Check job was tracked
-- Visit: http://localhost:3000/finance/gold?tab=job-monitoring
```

---

## Test 2: Customer Segmentation (RFM Analysis)

### API Call

```bash
curl -X POST "http://localhost:8000/api/gold/transform/customer-segmentation" \
-H "Content-Type: application/json" \
-d '{
  "silver_table": "syniqai_silver.finance.march_user_transactions",
  "gold_table": "syniqai_gold.finance.customer_segments",
  "description": "RFM customer segmentation: VIP, Premium, Regular, New"
}'
```

### Expected Response

```json
{
  "success": true,
  "job_id": "job_def456abc",
  "result": {
    "success": true,
    "silver_table": "syniqai_silver.finance.march_user_transactions",
    "gold_table": "syniqai_gold.finance.customer_segments",
    "transformation_type": "customer_segmentation",
    "input_rows": 1064,
    "output_rows": 87,
    "segments": ["VIP", "Premium", "Regular", "New"],
    "duration_seconds": 9.2
  },
  "message": "✅ Customer segments created: 87 customers"
}
```

### Verify Results

```sql
-- Check customer segments distribution
SELECT customer_segment, COUNT(*) as customer_count, 
       SUM(total_spent) as total_revenue,
       AVG(purchase_frequency) as avg_frequency
FROM syniqai_gold.finance.customer_segments
GROUP BY customer_segment
ORDER BY total_revenue DESC;

-- Expected distribution:
-- VIP: 5-10 customers
-- Premium: 15-25 customers
-- Regular: 30-40 customers
-- New: 20-30 customers
```

---

## Test 3: Gold Join (Transaction Insights)

### API Call

```bash
curl -X POST "http://localhost:8000/api/gold/transform/join" \
-H "Content-Type: application/json" \
-d '{
  "tables": [
    {"table": "syniqai_silver.finance.march_user_transactions", "alias": "txn"},
    {"table": "syniqai_gold.finance.product_performance", "alias": "prod"}
  ],
  "join_conditions": [
    {"left": "txn.PRODUCT_NAME", "right": "prod.PRODUCT_NAME", "type": "INNER"}
  ],
  "select_columns": [
    "txn.TRANSACTION_ID",
    "txn.TRANSACTION_DATE",
    "txn.USER_ID",
    "prod.PRODUCT_NAME",
    "prod.CATEGORY",
    "txn.AMOUNT",
    "prod.total_revenue as product_total_revenue",
    "prod.transaction_count as product_transaction_count"
  ],
  "gold_table": "syniqai_gold.finance.transaction_insights",
  "description": "Enriched transactions with product performance metrics"
}'
```

### Expected Response

```json
{
  "success": true,
  "job_id": "job_ghi789def",
  "result": {
    "success": true,
    "source_tables": [
      "syniqai_silver.finance.march_user_transactions",
      "syniqai_gold.finance.product_performance"
    ],
    "gold_table": "syniqai_gold.finance.transaction_insights",
    "transformation_type": "join",
    "tables_joined": 2,
    "output_rows": 1064,
    "duration_seconds": 15.8
  },
  "message": "✅ Gold table created: 1064 rows"
}
```

### Verify Results

```sql
-- Check joined data
SELECT TRANSACTION_ID, PRODUCT_NAME, AMOUNT, 
       product_total_revenue, product_transaction_count
FROM syniqai_gold.finance.transaction_insights
LIMIT 10;

-- Verify join correctness: product_total_revenue should be same for all rows with same PRODUCT_NAME
SELECT PRODUCT_NAME, 
       COUNT(DISTINCT product_total_revenue) as unique_revenue_values
FROM syniqai_gold.finance.transaction_insights
GROUP BY PRODUCT_NAME
HAVING COUNT(DISTINCT product_total_revenue) > 1;
-- Expected: 0 rows (all products should have consistent revenue values)
```

---

## Complete Lineage Verification

### Check Full Data Lineage Chain

```sql
-- View complete Bronze → Silver → Gold lineage
SELECT 
    CONCAT(source_layer, '.', source_table) as source,
    CONCAT(target_layer, '.', target_table) as target,
    transformation_type,
    created_at
FROM data_lineage
WHERE domain = 'finance'
ORDER BY created_at ASC;

-- Expected output:
-- bronze → silver (4 transformations)
-- silver → gold (3 transformations for this test)
```

### Check Job Monitoring

Visit: `http://localhost:3000/finance/gold?tab=job-monitoring`

**Expected:**
- 3 completed jobs listed
- Each showing:
  - ✅ Status: Completed
  - Duration: 5-20 seconds
  - Input/Output row counts
  - Transformation type (aggregation, customer_segmentation, join)

### Check Data Lineage Graph

Visit: `http://localhost:3000/finance/gold?tab=data-lineage`

**Expected:**
- Bronze layer (left): `bronze.finance.march_user_transactions`
- Silver layer (middle): `silver.finance.march_user_transactions`
- Gold layer (right): 3 tables
  - `gold.finance.product_performance`
  - `gold.finance.customer_segments`
  - `gold.finance.transaction_insights`
- Arrows connecting: Bronze → Silver → Gold

---

## Troubleshooting

### Error: "Source table not found"
**Solution**: Ensure Silver table exists in MinIO. Check with:
```python
# In PySpark shell:
spark.catalog.listTables("syniqai_silver.finance")
```

### Error: "Failed to initialize Spark"
**Solution**: 
1. Check JAVA_HOME is set correctly
2. Verify MinIO is running: `http://localhost:9000`
3. Check Spark dependencies in `data lakehouse/syniq_project/`

### No data in lineage graph
**Solution**:
1. Check PostgreSQL is running
2. Verify `data_lineage` table exists
3. Check backend logs for lineage recording errors

### Jobs not appearing in Job Monitoring
**Solution**:
1. Restart backend (ensures singleton job_tracker is used)
2. Check logs for: `✓ Using silver_job_tracker singleton (TTL=24h)`
3. Verify `/api/silver/jobs` endpoint returns data

---

## Next Steps

After successful testing:

1. **Explore Gold Tables**: Use SQL Editor to query Gold tables
2. **Create Visualizations**: Use Gold tables for dashboards/reports  
3. **Schedule Transformations**: Set up periodic Gold layer refreshes
4. **Quality Monitoring**: Check Gold layer quality scores in Quality Monitoring tab

---

## API Endpoints Summary

| Endpoint | Method | Purpose |
|----------|--------|---------|
| `/api/gold/transform/aggregation` | POST | Create aggregated Gold table |
| `/api/gold/transform/join` | POST | Join multiple tables → Gold |
| `/api/gold/transform/customer-segmentation` | POST | RFM customer segmentation |
| `/api/gold/tables` | GET | List all Gold tables |
| `/api/silver/jobs` | GET | Get transformation jobs |
| `/api/lineage/graph` | GET | Get complete lineage graph |
