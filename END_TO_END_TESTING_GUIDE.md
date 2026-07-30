# SyniqAI End-to-End Testing Guide
**Comprehensive System Test: CDC → Bronze → Silver → Gold → Reports**

Date: March 19, 2026  
Test Duration: ~2-3 hours  
Tester: _____________

---

## 🎯 Test Objectives

1. Validate real-time CDC from PostgreSQL/MariaDB
2. Verify Bronze layer ingestion
3. Test Silver transformations and data quality
4. Validate Gold layer aggregations and analytics
5. Confirm lineage tracking across all layers
6. Verify comprehensive reporting

---

## 📋 Prerequisites Checklist

### Services Running
- [ ] PostgreSQL (port 5432) - Source database
- [ ] MariaDB (optional second source)
- [ ] Kafka + Zookeeper (ports 9092, 2181)
- [ ] Kafka Connect + Debezium (port 8083)
- [ ] Kafka UI (port 8080)
- [ ] MinIO (port 9000)
- [ ] Backend API (port 8000)
- [ ] Frontend (port 3000)
- [ ] Airflow (port 8081 - optional)

### Database Setup
- [ ] PostgreSQL lineage tables initialized (`.\setup_lineage_tables.ps1`)
- [ ] Source database (laptop PostgreSQL) has test table
- [ ] Debezium connector configured for source

### Test Data Ready
- [ ] Sample transactions table with 50+ rows
- [ ] At least 3 different product categories
- [ ] Date range: last 30 days

---

## 🔧 Environment Setup (10 minutes)

### Step 1: Start All Services

```powershell
# Navigate to project root
cd "C:\Users\Syarifah\OneDrive - M Telecommunication Sdn Bhd\INTERNSHIP\SyniqAi"

# Start Kafka ecosystem
.\restart_kafka.ps1

# Start Backend
cd gui\api
uvicorn backend:app --reload

# Start Frontend (new terminal)
cd gui
npm run dev

# Verify all services
docker ps
```

**Expected Result:**
- ✓ All containers running
- ✓ Backend shows "Application startup complete"
- ✓ Frontend accessible at http://localhost:3000

---

### Step 2: Prepare Source Data (Laptop PostgreSQL)

**On the other laptop**, connect to PostgreSQL and create test table:

```sql
-- Create test table
CREATE TABLE public.user_transactions (
    transaction_id SERIAL PRIMARY KEY,
    user_id INT NOT NULL,
    product_id INT NOT NULL,
    product_name VARCHAR(100),
    category VARCHAR(50),
    amount DECIMAL(10,2),
    transaction_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    status VARCHAR(20) DEFAULT 'completed'
);

-- Insert initial data (50 rows)
INSERT INTO public.user_transactions (user_id, product_id, product_name, category, amount, transaction_date, status)
SELECT 
    (random() * 100 + 1)::INT,
    (random() * 20 + 1)::INT,
    'Product ' || (random() * 20 + 1)::INT,
    CASE (random() * 3)::INT
        WHEN 0 THEN 'Electronics'
        WHEN 1 THEN 'Clothing'
        WHEN 2 THEN 'Food'
        ELSE 'Books'
    END,
    (random() * 500 + 10)::DECIMAL(10,2),
    CURRENT_TIMESTAMP - (random() * 30 || ' days')::INTERVAL,
    CASE (random() * 2)::INT
        WHEN 0 THEN 'completed'
        ELSE 'pending'
    END
FROM generate_series(1, 50);

-- Verify data
SELECT COUNT(*), MIN(transaction_date), MAX(transaction_date) FROM public.user_transactions;
```

**Checkpoint:**
- [ ] Table created successfully
- [ ] 50 rows inserted
- [ ] Date range covers last 30 days

---

## 📡 Phase 1: CDC Real-Time Streaming (15 minutes)

### Test 1.1: Configure Debezium Connector

Navigate to: **Change Data Capture → Real-time (Debezium)**

1. [ ] Click "Add Database Connection"
2. [ ] Fill in source database details:
   - Host: `<laptop-ip-address>`
   - Port: `5432`
   - Database: `postgres`
   - Username: `postgres`
   - Password: `<password>`
   - Table: `public.user_transactions`
3. [ ] Click "Start CDC Streaming"

**Expected Result:**
- ✓ Connection successful message
- ✓ Debezium connector created (check Kafka UI at http://localhost:8080)
- ✓ Topic `dbserver1.public.user_transactions` appears in Kafka UI

---

### Test 1.2: Make Changes on Source Database

**On laptop PostgreSQL**, execute these changes one by one:

```sql
-- Change 1: INSERT new transaction
INSERT INTO public.user_transactions (user_id, product_id, product_name, category, amount, status)
VALUES (101, 5, 'Laptop Pro', 'Electronics', 1299.99, 'completed');

-- Wait 5 seconds, then check SyniqAI frontend

-- Change 2: UPDATE existing transaction
UPDATE public.user_transactions 
SET status = 'refunded', amount = 0 
WHERE transaction_id = 1;

-- Wait 5 seconds, then check SyniqAI frontend

-- Change 3: DELETE transaction
DELETE FROM public.user_transactions WHERE transaction_id = 2;

-- Wait 5 seconds, then check SyniqAI frontend

-- Change 4: Bulk INSERT
INSERT INTO public.user_transactions (user_id, product_id, product_name, category, amount)
SELECT 
    (random() * 100 + 1)::INT,
    (random() * 20 + 1)::INT,
    'Product ' || (random() * 20 + 1)::INT,
    'Electronics',
    (random() * 200 + 50)::DECIMAL(10,2)
FROM generate_series(1, 10);
```

**For Each Change, Verify:**
- [ ] Message appears in Kafka UI topic (within 5 seconds)
- [ ] CDC logs show "Record published" in SyniqAI frontend
- [ ] Event type shows: INSERT, UPDATE, DELETE

**Checkpoint:**
- [ ] All 4 change types captured
- [ ] Latency < 10 seconds from source change to Kafka message
- [ ] CDC consumer logs show successful processing

---

## 🥉 Phase 2: Bronze Layer Ingestion (10 minutes)

### Test 2.1: Verify Bronze Table Creation

Navigate to: **Bronze Layer**

1. [ ] Bronze table `bronze_user_transactions` appears in table list
2. [ ] Table shows row count (should be ~60 rows)
3. [ ] Last updated timestamp is recent
4. [ ] Click "Preview Data" → shows raw CDC events with metadata

**Expected Columns:**
- All source columns (transaction_id, user_id, product_id, etc.)
- CDC metadata: `__op` (operation), `__ts_ms` (timestamp), `__deleted`

**Checkpoint:**
- [ ] Bronze table contains all 60+ records
- [ ] INSERT, UPDATE, DELETE operations visible in `__op` column
- [ ] No duplicate records

---

### Test 2.2: Query Bronze Data

Click **"SQL Query"** button:

```sql
-- Test Query 1: Count by operation type
SELECT __op as operation, COUNT(*) as count
FROM bronze_user_transactions
GROUP BY __op;

-- Expected: INSERT: ~60, UPDATE: 1, DELETE: 1

-- Test Query 2: Recent changes (last 10 minutes)
SELECT transaction_id, product_name, __op, __ts_ms
FROM bronze_user_transactions
WHERE __ts_ms > EXTRACT(EPOCH FROM NOW() - INTERVAL '10 minutes') * 1000
ORDER BY __ts_ms DESC;

-- Expected: Shows the 4 test changes made in Phase 1
```

**Checkpoint:**
- [ ] SQL editor executes successfully
- [ ] Results match expected counts
- [ ] Recent changes visible

---

## 🥈 Phase 3: Silver Layer Transformations (30 minutes)

### Test 3.1: Silver Transformation #1 - Data Cleaning

Navigate to: **Silver Processing → Structured**

**Transformation: Clean & Deduplicate**

1. [ ] Click "New Transformation"
2. [ ] Source: `bronze_user_transactions`
3. [ ] Target: `silver_transactions_clean`
4. [ ] Enable rules:
   - [ ] Remove deleted records (`__op != 'd'`)
   - [ ] Remove duplicates by `transaction_id`
   - [ ] Convert timestamp to datetime
   - [ ] Validate amount > 0
   - [ ] Trim whitespace from product_name

5. [ ] Click "Preview Transformation" → shows before/after samples
6. [ ] Click "Execute Transformation"

**Expected Result:**
- ✓ Transformation succeeds within 30 seconds
- ✓ Success message shows row counts (Input: ~60, Output: ~58)
- ✓ Job appears in Job Monitoring with "Completed" status

**Verify Silver Table:**
```sql
-- Check row count (should be ~58, excluding deleted records)
SELECT COUNT(*) FROM silver_transactions_clean;

-- Verify no duplicates
SELECT transaction_id, COUNT(*) as count
FROM silver_transactions_clean
GROUP BY transaction_id
HAVING COUNT(*) > 1;
-- Expected: 0 rows

-- Check data quality
SELECT 
    COUNT(*) as total,
    COUNT(CASE WHEN amount > 0 THEN 1 END) as valid_amount,
    COUNT(CASE WHEN product_name IS NOT NULL THEN 1 END) as has_product_name
FROM silver_transactions_clean;
-- Expected: All counts equal
```

**Checkpoint:**
- [ ] Silver table created with ~58 clean records
- [ ] No duplicates
- [ ] All validation rules passed
- [ ] Lineage recorded (check Data Lineage tab)

---

### Test 3.2: Silver Transformation #2 - Aggregation

**Transformation: Daily Sales Summary**

1. [ ] Source: `silver_transactions_clean`
2. [ ] Target: `silver_daily_sales`
3. [ ] Transformation SQL:
```sql
SELECT 
    DATE(transaction_date) as sale_date,
    category,
    COUNT(*) as transaction_count,
    SUM(amount) as total_revenue,
    AVG(amount) as avg_transaction_value,
    COUNT(DISTINCT user_id) as unique_customers
FROM silver_transactions_clean
WHERE status = 'completed'
GROUP BY DATE(transaction_date), category
ORDER BY sale_date DESC, category;
```

4. [ ] Execute Transformation

**Expected Result:**
- ✓ Creates `silver_daily_sales` table
- ✓ ~15-20 rows (last 30 days × 3-4 categories)
- ✓ All aggregations calculated correctly

**Checkpoint:**
- [ ] Aggregation table created
- [ ] Metrics make sense (total_revenue = sum of amounts)
- [ ] No null values in aggregate columns

---

### Test 3.3: Silver Transformation #3 - Enrichment

**Transformation: Add Category Flags**

1. [ ] Source: `silver_transactions_clean`
2. [ ] Target: `silver_transactions_enriched`
3. [ ] Transformation SQL:
```sql
SELECT 
    *,
    CASE 
        WHEN amount > 500 THEN 'High Value'
        WHEN amount > 100 THEN 'Medium Value'
        ELSE 'Low Value'
    END as value_tier,
    CASE 
        WHEN category = 'Electronics' THEN TRUE
        ELSE FALSE
    END as is_electronics,
    EXTRACT(HOUR FROM transaction_date) as transaction_hour,
    EXTRACT(DOW FROM transaction_date) as transaction_day_of_week
FROM silver_transactions_clean;
```

4. [ ] Execute Transformation

**Checkpoint:**
- [ ] New columns added: value_tier, is_electronics, transaction_hour, transaction_day_of_week
- [ ] Row count matches source (~58)
- [ ] Enrichment logic correct (check value_tier for amount=1299.99 → 'High Value')

---

### Test 3.4: Silver Transformation #4 - Quality Validation

**Transformation: Failed Transactions Analysis**

1. [ ] Source: `silver_transactions_clean`
2. [ ] Target: `silver_failed_transactions`
3. [ ] Transformation SQL:
```sql
SELECT 
    transaction_id,
    user_id,
    product_name,
    amount,
    status,
    transaction_date,
    CASE 
        WHEN status = 'refunded' THEN 'Refund Requested'
        WHEN status = 'pending' THEN 'Payment Pending'
        WHEN status = 'failed' THEN 'Payment Failed'
        ELSE 'Other'
    END as failure_reason
FROM silver_transactions_clean
WHERE status != 'completed'
ORDER BY transaction_date DESC;
```

4. [ ] Execute Transformation

**Checkpoint:**
- [ ] Table contains only non-completed transactions
- [ ] failure_reason column populated
- [ ] Can be used for monitoring/alerting

---

### Test 3.5: SQL Editor in Silver Layer

Navigate to: **Silver Processing → SQL Query**

Execute these queries:

```sql
-- Query 1: Revenue by category
SELECT category, SUM(amount) as revenue, COUNT(*) as transactions
FROM silver_transactions_clean
WHERE status = 'completed'
GROUP BY category
ORDER BY revenue DESC;

-- Query 2: Top spending users
SELECT user_id, COUNT(*) as purchase_count, SUM(amount) as total_spent
FROM silver_transactions_clean
WHERE status = 'completed'
GROUP BY user_id
ORDER BY total_spent DESC
LIMIT 10;

-- Query 3: Daily transaction trend
SELECT DATE(transaction_date) as date, COUNT(*) as transactions
FROM silver_transactions_clean
GROUP BY DATE(transaction_date)
ORDER BY date DESC
LIMIT 7;
```

**Checkpoint:**
- [ ] All 3 queries execute successfully
- [ ] Results display in table format
- [ ] Export to CSV works
- [ ] Query history saved

---

### Test 3.6: Job Monitoring - Silver Layer

Navigate to: **Silver Processing → Job Monitoring**

**Verify:**
- [ ] All 4 transformations appear in job list
- [ ] Status: "Completed" (green checkmark)
- [ ] Duration shown for each job (5-30 seconds typical)
- [ ] Click job → details panel shows:
  - Source table
  - Target table
  - Row counts (input/output)
  - Execution logs
  - Transformation SQL

**Test Actions:**
- [ ] Filter by status: "Completed"
- [ ] Sort by date: Most recent first
- [ ] Search for job: "silver_transactions_clean"
- [ ] Click "Refresh" → updates job list

**Checkpoint:**
- [ ] Job history persists (survives page refresh)
- [ ] No failed jobs
- [ ] Job metrics accurate

---

### Test 3.7: Data Lineage - Silver Layer

Navigate to: **Silver Processing → Data Lineage**

**Graph View:**
- [ ] Bronze tables shown on left (bronze color)
- [ ] Silver tables shown on right (silver color)
- [ ] Arrows connect Bronze → Silver
- [ ] Hover over node → shows table metadata:
  - Row count
  - Last updated
  - Transformation type
- [ ] Click node → highlights connected nodes

**Table View:**
- [ ] List of all Bronze → Silver transformations
- [ ] Columns: Source Table, Target Table, Transformation Type, Created Date
- [ ] 4 transformations visible (from Tests 3.1-3.4)
- [ ] Click transformation → shows details

**Test Lineage Recording:**

Check PostgreSQL directly:

```sql
-- Verify lineage stored
SELECT source_table, target_table, transformation_type, domain
FROM data_lineage
WHERE source_layer = 'bronze' AND target_layer = 'silver'
ORDER BY created_at DESC;

-- Expected: 4 rows showing our transformations

-- Check audit log
SELECT event_type, resource_name, status, event_timestamp
FROM audit_log
WHERE event_category = 'data_processing'
ORDER BY event_timestamp DESC
LIMIT 10;

-- Expected: Shows transformation events
```

**Checkpoint:**
- [ ] Lineage graph renders correctly
- [ ] All 4 transformations tracked
- [ ] Data persists in PostgreSQL
- [ ] Audit events logged

---

## 🥇 Phase 4: Gold Layer Processing (45 minutes)

### Test 4.1: Gold Dashboard (Overview)

Navigate to: **Gold Layer Processing → Overview**

**KPI Cards:**
- [ ] "Total Tables" card shows count (should be at least 4 Silver tables)
- [ ] "Active Transformations" shows 0 (no Gold transforms yet)
- [ ] "Quality Score" shows percentage (based on Silver quality)
- [ ] "Gold-Ready Tables" shows count of Silver tables ready for Gold

**Tables List:**
- [ ] Silver tables appear in list:
  - `silver_transactions_clean`
  - `silver_daily_sales`
  - `silver_transactions_enriched`
  - `silver_failed_transactions`
- [ ] Each table shows:
  - Schema preview
  - Row count
  - Gold-ready status (green shield icon)

**Actions:**
- [ ] Click "View Schema" on a table → modal shows columns and types
- [ ] Click "Use in Gold Transformation" → navigates to Transformation tab

**Checkpoint:**
- [ ] All Silver tables visible
- [ ] Counts accurate
- [ ] No errors in console

---

### Test 4.2: Gold Transformation - Aggregation

Navigate to: **Gold Layer Processing → Gold Transformation**

**Test 4.2.1: Product Performance Analysis**

1. [ ] Click "Aggregation" tab
2. [ ] Source Table: `silver_transactions_enriched`
3. [ ] Target Table: `gold_product_performance`
4. [ ] Configure aggregation:
   - **GROUP BY:** `product_id`, `product_name`, `category`
   - **Aggregations:**
     - `SUM(amount)` AS `total_revenue`
     - `COUNT(*)` AS `transaction_count`
     - `AVG(amount)` AS `avg_price`
     - `COUNT(DISTINCT user_id)` AS `unique_buyers`

5. [ ] Click "Preview SQL" → verify syntax
6. [ ] Click "Execute Transformation"

**Expected Result:**
```
✅ Transformation completed successfully
Input: 58 rows
Output: ~20 rows (unique products)
Duration: < 15 seconds
```

**Verify Gold Table:**
```sql
SELECT * FROM gold_product_performance
ORDER BY total_revenue DESC
LIMIT 5;

-- Expected: Top 5 products by revenue with all metrics
```

**Checkpoint:**
- [ ] Gold table created
- [ ] Aggregations calculated correctly
- [ ] Sum of total_revenue matches Silver layer total
- [ ] Lineage: Silver → Gold recorded

---

### Test 4.2.2: Customer Segmentation

**Transformation: RFM Analysis (Recency, Frequency, Monetary)**

1. [ ] Source: `silver_transactions_clean`
2. [ ] Target: `gold_customer_segments`
3. [ ] Transformation SQL:
```sql
SELECT 
    user_id,
    MAX(transaction_date) as last_purchase_date,
    COUNT(*) as purchase_frequency,
    SUM(amount) as total_spent,
    AVG(amount) as avg_order_value,
    EXTRACT(DAYS FROM (CURRENT_TIMESTAMP - MAX(transaction_date))) as days_since_last_purchase,
    CASE 
        WHEN SUM(amount) > 1000 AND COUNT(*) > 10 THEN 'VIP'
        WHEN SUM(amount) > 500 AND COUNT(*) > 5 THEN 'Premium'
        WHEN SUM(amount) > 100 AND COUNT(*) > 2 THEN 'Regular'
        ELSE 'New'
    END as customer_segment
FROM silver_transactions_clean
WHERE status = 'completed'
GROUP BY user_id
ORDER BY total_spent DESC;
```

4. [ ] Execute Transformation

**Checkpoint:**
- [ ] Customer segmentation created
- [ ] ~50-80 unique customers
- [ ] Segments distributed: VIP, Premium, Regular, New
- [ ] RFM metrics calculated

---

### Test 4.3: Gold Transformation - Join

**Test 4.3.1: Transaction Insights (Multi-Table Join)**

1. [ ] Click "Join" tab
2. [ ] Select tables to join:
   - `silver_transactions_clean` (alias: txn)
   - `gold_product_performance` (alias: prod)
   - `gold_customer_segments` (alias: cust)

3. [ ] Join Configuration:
   - **Join 1:** `txn.product_id = prod.product_id` (INNER JOIN)
   - **Join 2:** `txn.user_id = cust.user_id` (LEFT JOIN)

4. [ ] Select columns:
   ```
   txn.transaction_id,
   txn.transaction_date,
   prod.product_name,
   prod.category,
   txn.amount,
   prod.total_revenue as product_total_revenue,
   cust.customer_segment,
   cust.total_spent as customer_total_spent
   ```

5. [ ] Target: `gold_transaction_insights`
6. [ ] Execute Transformation

**Expected Result:**
- ✓ Enriched transaction table with product and customer context
- ✓ ~58 rows (one per transaction)
- ✓ All joins successful (no nulls from INNER JOIN on products)

**Checkpoint:**
- [ ] Join executed successfully
- [ ] All columns present
- [ ] Customer segment appears from joined table
- [ ] Product total revenue matches across rows for same product

---

### Test 4.4: Gold Quality Monitoring

Navigate to: **Gold Layer Processing → Quality Monitoring**

**Cross-Layer Quality Dashboard:**

**Quality Scores:**
- [ ] Bronze layer score displayed (%), typically 60-70%
- [ ] Silver layer score displayed (%), should be 85-95%
- [ ] Gold layer score displayed (%), should be 90-100%
- [ ] Color coding: Green (>90%), Yellow (70-90%), Red (<70%)

**Star Schema Diagram:**
- [ ] Fact tables identified (`gold_transaction_insights`)
- [ ] Dimension tables identified (`gold_customer_segments`, `gold_product_performance`)
- [ ] Relationships displayed with arrows
- [ ] Hover on node → shows table stats

**Quality Trends (12 weeks):**
- [ ] Line chart displays quality scores over time
- [ ] Separate lines for Bronze, Silver, Gold
- [ ] Shows improvement from Bronze → Silver → Gold
- [ ] Tooltip shows exact values on hover

**Active Alerts:**
- [ ] Alert list displays (if any quality issues)
- [ ] Alert severity badges (INFO, WARNING, HIGH, CRITICAL)
- [ ] Example alert: "Gold layer table missing expected records"
- [ ] Click "Resolve" to acknowledge

**Score Distribution:**
- [ ] Pie chart shows distribution:
  - Excellent (90-100%): X tables
  - Good (70-90%): Y tables
  - Poor (<70%): Z tables

**Checkpoint:**
- [ ] Quality metrics calculated correctly
- [ ] Gold layer has highest quality score
- [ ] Star schema detected correctly
- [ ] No critical alerts (or all resolved)

---

### Test 4.5: Gold Exploratory Analysis (EDA)

Navigate to: **Gold Layer Processing → Exploratory Analysis**

**Test 4.5.1: Statistical Profiling**

1. [ ] Select table: `gold_product_performance`
2. [ ] Click "Run Analysis"

**Profile Summary:**
- [ ] Row count: ~20
- [ ] Column count: 7
- [ ] Memory usage displayed
- [ ] Null percentage per column shown

**Column Statistics:**
- [ ] **total_revenue:**
  - Mean, Median, Std Dev displayed
  - Min/Max values shown
  - Percentiles (25th, 50th, 75th)
- [ ] **transaction_count:**
  - Distribution histogram
  - Outliers highlighted
- [ ] **unique_buyers:**
  - Value distribution chart

**Checkpoint:**
- [ ] Statistics calculated correctly
- [ ] Histograms render properly
- [ ] No NaN or infinity values in stats

---

**Test 4.5.2: Schema Detection**

1. [ ] Click "Detect Schema" button
2. [ ] Wait for analysis (10-15 seconds)

**Expected Detection:**
- [ ] **Star Schema detected:**
  - Fact table: `gold_transaction_insights`
  - Dimensions: `gold_customer_segments`, `gold_product_performance`
- [ ] Cardinality analysis shows:
  - One-to-many relationships
  - Foreign key candidates identified

**Checkpoint:**
- [ ] Schema type correctly identified (Star/Snowflake)
- [ ] Fact vs Dimension classification accurate
- [ ] Relationships visualized

---

**Test 4.5.3: Correlation Analysis**

1. [ ] Select table: `gold_customer_segments`
2. [ ] Click "Correlation Matrix"

**Correlation Heatmap:**
- [ ] Shows correlation between numeric columns:
  - `purchase_frequency` vs `total_spent`: Strong positive (>0.7)
  - `days_since_last_purchase` vs `purchase_frequency`: Negative
  - `avg_order_value` vs `total_spent`: Moderate positive

**Checkpoint:**
- [ ] Heatmap renders with color gradient
- [ ] Hover shows exact correlation values
- [ ] Strong correlations highlighted in red

---

### Test 4.6: Gold Job Monitoring

Navigate to: **Gold Layer Processing → Job Monitoring**

**Job List Verification:**
- [ ] All 3 Gold transformations appear:
  1. `gold_product_performance` (Aggregation)
  2. `gold_customer_segments` (Aggregation)
  3. `gold_transaction_insights` (Join)

**Job Details:**
- [ ] Status: "Completed" for all
- [ ] Duration: 5-30 seconds each
- [ ] Input/Output row counts displayed
- [ ] Execution timestamp shown

**Filter & Sort:**
- [ ] Filter by transformation type: "Aggregation" → shows 2 jobs
- [ ] Filter by status: "Completed" → shows all
- [ ] Sort by date: Most recent first
- [ ] Search: "customer" → finds customer_segments job

**Job Detail Panel:**

Click on `gold_product_performance` job:

- [ ] Shows source table: `silver_transactions_enriched`
- [ ] Shows target table: `gold_product_performance`
- [ ] Shows transformation SQL
- [ ] Shows execution logs:
  ```
  [INFO] Starting aggregation transformation
  [INFO] Reading from silver_transactions_enriched (58 rows)
  [INFO] Grouping by: product_id, product_name, category
  [INFO] Aggregations: SUM, COUNT, AVG
  [INFO] Writing to gold_product_performance (20 rows)
  [INFO] Transformation completed in 8.3s
  ```
- [ ] "Rerun" button available
- [ ] "View Lineage" button navigates to lineage graph

**Checkpoint:**
- [ ] All Gold jobs tracked
- [ ] Job history persists
- [ ] Logs detailed and helpful
- [ ] No errors or warnings

---

### Test 4.7: Gold Data Lineage

Navigate to: **Gold Layer Processing → Data Lineage**

**Full Lineage Graph (Bronze → Silver → Gold):**

**Graph Visualization:**
- [ ] 3 layers displayed horizontally:
  - **Bronze** (left, bronze color): `bronze_user_transactions`
  - **Silver** (middle, silver color): 4 tables
  - **Gold** (right, gold color): 3 tables

- [ ] Arrows connect transformations:
  - Bronze → Silver (4 arrows)
  - Silver → Gold (3 arrows)

- [ ] Node details on hover:
  - Table name
  - Row count
  - Last updated timestamp
  - Transformation count

- [ ] Click node → highlights all connected nodes
  - Example: Click `silver_transactions_clean` → highlights:
    - Upstream: `bronze_user_transactions`
    - Downstream: `gold_product_performance`, `gold_transaction_insights`

**Lineage Table View:**
- [ ] Switch to "Table View" tab
- [ ] Shows all 7 transformations (4 Bronze→Silver + 3 Silver→Gold)
- [ ] Columns displayed:
  - Source Layer
  - Source Table
  - Target Layer
  - Target Table
  - Transformation Type
  - Domain
  - Created Date
  - Status

**Filter Options:**
- [ ] Filter by layer: "Gold" → shows 3 transformations
- [ ] Filter by transformation type: "Aggregation" → shows 2
- [ ] Search: "customer" → finds customer_segments lineage

**Test Impact Analysis:**

Click on `silver_transactions_clean` node:

- [ ] "Upstream Impact" panel shows:
  - Source: `bronze_user_transactions`
  - Change impact: If Bronze updated, Silver will be affected

- [ ] "Downstream Impact" panel shows:
  - Targets: `gold_product_performance`, `gold_transaction_insights`
  - Dependencies: 2 Gold tables depend on this Silver table

**Verify PostgreSQL Storage:**

```sql
-- Check all lineage records
SELECT 
    source_layer || '.' || source_table as source,
    target_layer || '.' || target_table as target,
    transformation_type,
    created_at
FROM data_lineage
WHERE domain = 'finance'
ORDER BY created_at ASC;

-- Expected: 7 rows showing complete lineage chain
```

**Checkpoint:**
- [ ] Complete lineage graph visualized
- [ ] All 7 transformations tracked
- [ ] Impact analysis works
- [ ] Data persists in PostgreSQL
- [ ] No orphaned tables (all tables have lineage)

---

## 📊 Phase 5: Reports Dashboard (20 minutes)

Navigate to: **Reports**

### Test 5.1: Summary Tab

**KPI Cards:**
- [ ] "Total Transformations": 7 (4 Silver + 3 Gold)
- [ ] "Total Tables": 8 (1 Bronze + 4 Silver + 3 Gold)
- [ ] "Audit Events": > 10 (all transformation events)
- [ ] "Quality Checks": 0 (or shows count if quality validation ran)

**Transformation Flow Chart (Bar Chart):**
- [ ] X-axis: Transformation flows ("bronze → silver", "silver → gold")
- [ ] Y-axis: Count
- [ ] Bars displayed:
  - Bronze → Silver: 4 transformations (2 sources, 4 targets)
  - Silver → Gold: 3 transformations (4 sources, 3 targets)
- [ ] Hover shows exact counts
- [ ] Legend: Transformations, Source Tables, Target Tables

**Audit Events by Category (Pie Chart):**
- [ ] Segments:
  - Data Processing: 80% (most events)
  - Rule Management: 10%
  - Security: 5%
  - Other: 5%
- [ ] Hover shows count and percentage
- [ ] Colors distinct for each category

**Success vs Failure Rate (Bar Chart):**
- [ ] Shows by category:
  - `data_processing`: 7 success, 0 failures (green bar)
  - Other categories: minimal events
- [ ] Green bars = success, Red bars = failures
- [ ] All transformations successful (no red bars)

**Date Range Filter:**
- [ ] Select "Last 24 hours" → shows only today's transformations (7)
- [ ] Select "Last 7 days" → shows all 7
- [ ] Select "Last 30 days" → shows all 7
- [ ] Select "Last 90 days" → shows all 7

**Checkpoint:**
- [ ] All KPIs match actual counts
- [ ] Charts render without errors
- [ ] Date filter works correctly
- [ ] No "No data" messages

---

### Test 5.2: Data Lineage Tab

**Lineage Transformations Table:**
- [ ] Table displays all 7 transformations
- [ ] Columns:
  - Created Date (sorted desc)
  - Source Table (with color coding by layer)
  - Transformation Type
  - Target Table (with color coding)

**Row Details:**
- [ ] Bronze → Silver rows:
  - Source: `bronze_user_transactions` (bronze badge)
  - Targets: 4 Silver tables (silver badge)
  - Types: cleaning, aggregation, enrichment, filtering

- [ ] Silver → Gold rows:
  - Sources: Silver tables (silver badge)
  - Targets: `gold_product_performance`, etc. (gold badge)
  - Types: aggregation, join

**Actions:**
- [ ] Click transformation row → expands to show:
  - Full transformation SQL
  - Input/Output row counts
  - Execution duration
  - Domain: finance
  - Batch ID

- [ ] "View in Lineage Graph" button → navigates to lineage visualization

**Checkpoint:**
- [ ] All 7 transformations listed
- [ ] Color coding correct (bronze/silver/gold)
- [ ] Expandable rows show details
- [ ] Links to lineage graph work

---

### Test 5.3: Audit Trail Tab

**Audit Events Table:**
- [ ] Shows > 10 events (transformation events + system events)
- [ ] Columns:
  - Event Timestamp
  - Event Type
  - Event Category (badge color-coded)
  - Status (✓ success / ✗ failure icon)
  - Resource Name

**Event Types:**
- [ ] `transformation_executed`: 7 events (one per transformation)
- [ ] `table_created`: 7 events (one per target table)
- [ ] `lineage_recorded`: 7 events
- [ ] `quality_check_run`: 0-3 events

**Event Details:**

Click on a `transformation_executed` event:

- [ ] Shows full event details:
  - Event ID (UUID)
  - User ID: system
  - Action: execute_transformation
  - Action Details (JSON):
    ```json
    {
      "source_table": "silver_transactions_clean",
      "target_table": "gold_product_performance",
      "transformation_type": "aggregation",
      "row_count_input": 58,
      "row_count_output": 20,
      "duration_seconds": 8.3
    }
    ```
  - Status: success
  - Error Message: null
  - Domain: finance

**Filter by Category:**
- [ ] Select "Data Processing" → shows 7 transformation events
- [ ] Select "Rule Management" → shows 0-1 events
- [ ] Select "All" → shows all events

**Date Range Filter:**
- [ ] Last 24 hours → shows all recent events
- [ ] Last 7 days → shows all

**Checkpoint:**
- [ ] Audit trail complete and accurate
- [ ] Event details JSON well-formatted
- [ ] No failed events (all status = success)
- [ ] Filtering works correctly

---

### Test 5.4: Quality Trends Tab

**Validation History Table:**
- [ ] Shows quality validation runs (if any executed)
- [ ] Columns:
  - Event Timestamp
  - Event Type: `quality_validation`, `data_quality_check`
  - Resource Name: table name
  - Status: success/failure
  - Quality Score (from action_details)

**Quality Score Chart (Line Chart):**
- [ ] X-axis: Date
- [ ] Y-axis: Quality Score (0-100%)
- [ ] Lines:
  - Bronze layer: 65-75% (blue line)
  - Silver layer: 85-95% (green line)
  - Gold layer: 90-100% (gold line)
- [ ] Trend: Improving from Bronze → Silver → Gold

**If No Data (Expected for Fresh Setup):**
- [ ] Shows message: "No quality validation data yet. Run quality checks from Silver/Gold layers."
- [ ] Shows placeholder chart with 0 data points

**Manual Quality Check (Optional):**

If you want to populate quality data:

1. Navigate to **Silver Processing → Quality Rules**
2. Create rule: "Check for null product_name"
3. Execute on `silver_transactions_clean`
4. Return to **Reports → Quality Trends**
5. Verify quality event appears

**Checkpoint:**
- [ ] Quality trends section loads without errors
- [ ] Shows empty state gracefully if no data
- [ ] If quality checks run, displays validation history

---

### Test 5.5: Export Functionality

**Test Export Report:**

1. [ ] Click "Export Report" button (top right)
2. [ ] Wait 2-5 seconds for download
3. [ ] File downloads as `syniqai_report_comprehensive_2026-03-19.json`

**Verify Export Contents:**

Open the JSON file and verify it contains:

```json
{
  "generated_at": "2026-03-19T...",
  "domain": "finance",
  "lineage_summary": {
    "total_transformations": 7,
    "total_tables": 8,
    "by_layer": [...]
  },
  "audit_summary": {
    "total_events": 10+,
    "by_category": {...}
  },
  "recent_transformations": [...],
  "tables_by_layer": {
    "bronze": [...],
    "silver": [...],
    "gold": [...]
  }
}
```

**Checkpoint:**
- [ ] Export downloads successfully
- [ ] JSON valid and well-formatted
- [ ] Contains all summary data
- [ ] File size reasonable (< 1MB for this test)

---

## 🎯 Phase 6: System Validation (15 minutes)

### Test 6.1: Data Consistency Checks

**Cross-Layer Verification:**

Execute these queries to verify data consistency:

```sql
-- 1. Total revenue should match across layers
-- Bronze (raw CDC data)
SELECT SUM(CASE WHEN __op != 'd' THEN amount ELSE 0 END) as bronze_total
FROM bronze_user_transactions
WHERE status = 'completed';

-- Silver (cleaned data)
SELECT SUM(amount) as silver_total
FROM silver_transactions_clean
WHERE status = 'completed';

-- Gold (product aggregation)
SELECT SUM(total_revenue) as gold_total
FROM gold_product_performance;

-- Expected: bronze_total ≈ silver_total = gold_total (within $1 difference)

-- 2. Record counts should be consistent
SELECT 
    'Bronze' as layer, COUNT(*) as count FROM bronze_user_transactions WHERE __op != 'd'
UNION ALL
SELECT 
    'Silver' as layer, COUNT(*) FROM silver_transactions_clean
UNION ALL
SELECT 
    'Gold (Insights)' as layer, COUNT(*) FROM gold_transaction_insights;

-- Expected: Bronze ≈ Silver = Gold Insights (both should be ~58)

-- 3. Check for data loss in transformations
SELECT 
    source_table,
    target_table,
    transformation_logic->>'input_rows' as input_rows,
    transformation_logic->>'output_rows' as output_rows,
    (transformation_logic->>'output_rows')::int - (transformation_logic->>'input_rows')::int as row_diff
FROM data_lineage
WHERE domain = 'finance'
ORDER BY created_at;

-- Expected: Row differences explainable (e.g., -1 for deleted record, +20 for aggregation)
```

**Checkpoint:**
- [ ] Revenue totals match across layers
- [ ] Record counts consistent
- [ ] No unexplained data loss

---

### Test 6.2: Performance Verification

**Measure Transformation Performance:**

```sql
-- Average transformation duration by type
SELECT 
    transformation_type,
    COUNT(*) as count,
    AVG(EXTRACT(EPOCH FROM (validated_at - created_at))) as avg_duration_seconds,
    MAX(EXTRACT(EPOCH FROM (validated_at - created_at))) as max_duration_seconds
FROM data_lineage
WHERE domain = 'finance' AND validated_at IS NOT NULL
GROUP BY transformation_type;

-- Expected: 
-- Aggregation: 5-15 seconds average
-- Join: 10-30 seconds average
-- Cleaning: 3-10 seconds average
```

**Checkpoint:**
- [ ] Transformations complete in reasonable time
- [ ] No timeouts or long-running queries (>60s)
- [ ] Performance acceptable for dataset size

---

### Test 6.3: End-to-End Latency Test

**Measure CDC → Bronze → Silver → Gold Latency:**

1. [ ] Note current time: `__:__:__`
2. [ ] On laptop PostgreSQL, execute:
```sql
INSERT INTO public.user_transactions (user_id, product_id, product_name, category, amount)
VALUES (999, 999, 'E2E Test Product', 'Electronics', 888.88);
```

3. [ ] Monitor SyniqAI:
   - [ ] CDC event appears in Kafka UI (within 5-10 seconds)
   - [ ] Bronze table updated (within 10-20 seconds)
   - [ ] Navigate to Silver Processing → see new record in `silver_transactions_clean`

4. [ ] Manually run Silver transformation (Refresh button)
   - [ ] Silver table updated (within 30 seconds)

5. [ ] Navigate to Gold Processing → Run aggregation transformation
   - [ ] Gold table updated (within 30 seconds)

6. [ ] Total latency: CDC event → Gold table = __:__:__ (target: < 2 minutes)

**Checkpoint:**
- [ ] End-to-end latency < 2 minutes for manual transforms
- [ ] All layers updated successfully
- [ ] Test record visible in all layers

---

## ✅ Final Validation Checklist

### Functional Requirements
- [ ] ✅ CDC captures INSERT, UPDATE, DELETE in real-time
- [ ] ✅ Bronze layer ingests raw CDC events
- [ ] ✅ Silver transformations clean and enrich data
- [ ] ✅ Gold transformations create analytics-ready tables
- [ ] ✅ Lineage tracked across all layers in PostgreSQL
- [ ] ✅ Audit trail records all transformation events
- [ ] ✅ Reports dashboard displays comprehensive metrics
- [ ] ✅ Job monitoring tracks transformation history
- [ ] ✅ Quality monitoring scores data across layers

### Non-Functional Requirements
- [ ] ✅ Performance: Transformations complete in < 30 seconds
- [ ] ✅ Reliability: No failed transformations
- [ ] ✅ Data Quality: Silver/Gold layers have >85% quality score
- [ ] ✅ Usability: UI responsive, no errors in console
- [ ] ✅ Persistence: All data survives page refresh

### Integration Points
- [ ] ✅ PostgreSQL: Lineage and audit data persisted
- [ ] ✅ Kafka: CDC events flowing through topics
- [ ] ✅ MinIO: (if used) Data stored in object storage
- [ ] ✅ Debezium: Source database changes captured
- [ ] ✅ Frontend ↔ Backend: All API endpoints working

---

## 🐛 Issues Found

**Issue Log:**

| # | Severity | Component | Description | Status | Resolution |
|---|----------|-----------|-------------|--------|------------|
| 1 | High | CDC | Connection timeout to laptop PostgreSQL | ❌ Open | Check firewall |
| 2 | Medium | Silver | Duplicate records after transformation | ✅ Fixed | Added DISTINCT |
| 3 | Low | UI | Loading spinner not showing | ✅ Fixed | Added state |
| ... | | | | | |

**Add issues as you find them during testing.**

---

## 📈 Test Results Summary

**Test Date:** _______________  
**Tester:** _______________  
**Environment:** Development  
**Test Duration:** _____ hours  

### Overall Results

| Phase | Test Category | Total Tests | Passed | Failed | Success Rate |
|-------|---------------|-------------|--------|--------|--------------|
| 1 | CDC Real-Time Streaming | 8 | __ | __ | __% |
| 2 | Bronze Layer Ingestion | 6 | __ | __ | __% |
| 3 | Silver Transformations | 28 | __ | __ | __% |
| 4 | Gold Layer Processing | 35 | __ | __ | __% |
| 5 | Reports Dashboard | 20 | __ | __ | __% |
| 6 | System Validation | 9 | __ | __ | __% |
| **Total** | **All Phases** | **106** | **__** | **__** | **__%** |

**Target Success Rate:** ≥ 95% (≥100/106 tests passed)

### Key Metrics Achieved

- **CDC Latency:** _____ seconds (target: < 10s)
- **Bronze Ingestion:** _____ seconds (target: < 30s)
- **Silver Transform Avg:** _____ seconds (target: < 20s)
- **Gold Transform Avg:** _____ seconds (target: < 30s)
- **End-to-End Latency:** _____ minutes (target: < 2 mins)
- **Data Quality Score:** ____% (target: > 85%)
- **Zero Data Loss:** ✅ Yes / ❌ No

### Sign-Off

**Test Engineer:** _____________________  Date: __________

**Notes:**
_________________________________________________________________
_________________________________________________________________
_________________________________________________________________

---

## 🎉 Conclusion

**System Status:** ✅ Production Ready / ⚠️ Needs Minor Fixes / ❌ Requires Major Work

**Next Steps:**
1. [ ] Fix any critical/high severity issues
2. [ ] Document any workarounds
3. [ ] Schedule regression testing
4. [ ] Prepare deployment plan

---

**END OF TEST GUIDE**
