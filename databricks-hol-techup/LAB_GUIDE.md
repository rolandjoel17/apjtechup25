# Databricks & Snowflake Data Lakehouse Hands-On Lab

## Lab Overview
**Duration:** 3-4 hours  
**Audience:** Snowflake Solution Engineers  
**Objective:** Gain hands-on experience with Databricks medallion architecture and compare approaches with Snowflake

## Business Scenario
You are a data engineer at a rapidly growing e-commerce company. The business needs a modern data lakehouse architecture to support:
- Real-time analytics and reporting
- Customer behavior analysis
- Product performance optimization
- Inventory management
- Executive dashboards

## Lab Learning Objectives

By the end of this lab, you will:
1. ✅ Understand medallion architecture (Bronze-Silver-Gold)
2. ✅ Work with Delta Lake and Apache Iceberg storage formats
3. ✅ Build ELT pipelines using SQL
4. ✅ Create analytical queries for business insights
5. ✅ Compare Databricks and Snowflake approaches
6. ✅ Optimize queries across different storage formats

## Lab Architecture

```
Raw Data (CSV) → Bronze (Delta) → Silver (Delta) → Gold (Iceberg)
                    ↓               ↓               ↓
                Raw Tables      Clean Tables   Analytics Tables
                ~10.1M rows     ~9.5M rows      ~50K rows
```

## Part 1: Environment Setup (30 minutes)

### 1.1 Data Generation
```bash
# Navigate to data generation directory
cd data_generation

# Install dependencies
pip install -r requirements.txt

# Generate ~10M records of e-commerce data
python generate_data.py
```

**Expected Output:**
- 10 CSV files in `bronze/sample_data/`
- Total records: ~10,100,000
- File size: ~2-3 GB

### 1.2 Verify Data Quality
Examine the generated files:
- `customers_raw.csv` (500K records)
- `products_raw.csv` (100K records)
- `order_items_raw.csv` (6M records) - Largest table
- Plus 7 additional supporting tables

**💡 Key Learning:** Real-world data volumes require careful consideration of storage formats and query optimization.

## Part 2: Bronze Layer - Raw Data Ingestion (45 minutes)

### 2.1 Upload Data (Databricks)
1. Navigate to Databricks workspace → Data → DBFS
2. Create folder: `/FileStore/shared_uploads/ecommerce_data/`
3. Upload all 10 CSV files

### 2.2 Create Delta Tables
Execute: `bronze/ddl/01_create_bronze_tables.sql`

**Key Features to Note:**
```sql
-- Delta table with partitioning
CREATE TABLE orders_raw (
    order_id BIGINT,
    customer_id BIGINT,
    order_date TIMESTAMP,
    -- ... other columns
) USING DELTA
PARTITIONED BY (DATE(order_date))
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);
```

### 2.3 Load Data
Execute: `bronze/ddl/02_load_bronze_data.sql`

**💡 Key Learning:** Delta Lake provides ACID transactions, time travel, and automatic optimization.

### 2.4 Verification
```sql
-- Check record counts
SELECT * FROM bronze_table_summary ORDER BY record_count DESC;

-- Sample data exploration
SELECT * FROM customers_raw LIMIT 10;
SELECT * FROM products_raw WHERE category_id = 1 LIMIT 5;
```

**Expected Results:**
- Total records: ~10.1M
- All tables loaded successfully
- Data quality looks reasonable

## Part 3: Silver Layer - Data Cleaning & Enrichment (60 minutes)

### 3.1 Create Clean Tables
Execute: `silver/ddl/01_create_silver_tables.sql`

**Key Improvements in Silver:**
- Data type standardization
- Calculated fields (age, profit margins, etc.)
- Business rules applied
- Reference data joined

### 3.2 Run ELT Transformations
Execute: `silver/elt/00_run_all_bronze_to_silver.sql`

**Transformation Examples:**
```sql
-- Customer age calculation and segmentation
CASE 
    WHEN YEAR(CURRENT_DATE()) - YEAR(c.birth_date) < 25 THEN '18-24'
    WHEN YEAR(CURRENT_DATE()) - YEAR(c.birth_date) < 35 THEN '25-34'
    -- ... other age groups
END as age_group

-- Product profit margin calculation
ROUND(((p.price - p.cost) / NULLIF(p.price, 0)) * 100, 2) as profit_margin
```

### 3.3 Data Quality Validation
```sql
-- Check data quality metrics
SELECT 
    COUNT(*) as total_customers,
    COUNT(CASE WHEN email_domain IS NOT NULL THEN 1 END) as valid_emails,
    COUNT(CASE WHEN age IS NOT NULL THEN 1 END) as customers_with_age,
    AVG(days_since_registration) as avg_days_since_registration
FROM customers_clean;
```

**💡 Key Learning:** Silver layer focuses on data quality, standardization, and business rule application.

## Part 4: Gold Layer - Analytics Ready Data (60 minutes)

### 4.1 Create Iceberg Tables
Execute: `gold/ddl/01_create_gold_tables.sql`

**Iceberg Features:**
```sql
-- Iceberg table with advanced features
CREATE TABLE customer_metrics (
    customer_id BIGINT,
    customer_lifetime_value DECIMAL(15,2),
    churn_risk_score DECIMAL(5,2),
    -- ... analytics columns
) USING ICEBERG
PARTITIONED BY (customer_tier, ltv_tier)
TBLPROPERTIES (
    'format-version' = '2',
    'write.distribution-mode' = 'hash'
);
```

### 4.2 Build Analytics Tables
Execute: `gold/elt/00_run_all_silver_to_gold.sql`

**Analytics Examples:**
- **Customer Metrics:** LTV, churn risk, segmentation
- **Product Performance:** Sales, ratings, inventory turnover
- **Sales Summary:** Time-based aggregations, trends
- **Inventory Insights:** Stock optimization, forecasting

### 4.3 Verify Gold Layer
```sql
-- Check analytics tables
SELECT * FROM gold_table_summary;

-- Sample customer insights
SELECT 
    customer_segment,
    COUNT(*) as customers,
    AVG(customer_lifetime_value) as avg_ltv,
    AVG(churn_risk_score) as avg_churn_risk
FROM customer_metrics
GROUP BY customer_segment;
```

**💡 Key Learning:** Gold layer provides business-ready, pre-aggregated data optimized for analytics.

## Part 5: Business Analytics (45 minutes)

### 5.1 Executive Dashboard
Execute queries from: `queries/analytical_queries.sql`

**Sample Business Questions:**
1. Who are our top 20 customers by lifetime value?
2. Which product categories drive the most revenue?
3. What's our customer churn risk by segment?
4. Which products need inventory reordering?

### 5.2 Customer Analytics
```sql
-- Customer Lifetime Value Analysis
SELECT 
    customer_name,
    customer_segment,
    CONCAT('$', FORMAT_NUMBER(customer_lifetime_value, 2)) as ltv,
    total_orders,
    favorite_category
FROM customer_metrics 
WHERE total_orders > 5
ORDER BY customer_lifetime_value DESC 
LIMIT 20;
```

### 5.3 Product Performance
```sql
-- Category Performance Analysis
SELECT 
    category_name,
    COUNT(*) as product_count,
    SUM(total_revenue) as category_revenue,
    AVG(average_rating) as avg_rating
FROM product_performance
GROUP BY category_name
ORDER BY category_revenue DESC;
```

### 5.4 Operational Insights
```sql
-- Inventory Alerts
SELECT 
    product_name,
    category_name,
    current_stock_level,
    days_supply_current,
    'Reorder Needed' as alert
FROM inventory_insights
WHERE needs_reorder = TRUE 
  AND classification = 'A'
ORDER BY inventory_value DESC;
```

**💡 Key Learning:** Well-designed gold layer enables self-service analytics and faster time-to-insight.

## Part 6: Performance Comparison (30 minutes)

### 6.1 Query Performance Testing
Test the same queries across layers:

```sql
-- Bronze layer (raw data scan)
SELECT COUNT(*) FROM orders_raw 
WHERE order_date >= '2024-01-01';

-- Silver layer (optimized with partitioning)
SELECT COUNT(*) FROM orders_clean 
WHERE order_date_only >= '2024-01-01';

-- Gold layer (pre-aggregated)
SELECT SUM(total_orders) FROM sales_summary 
WHERE date_key >= '2024-01-01';
```

### 6.2 Storage Format Comparison
Compare Delta vs Iceberg:
- **Delta Lake:** Better for streaming, frequent updates
- **Iceberg:** Better for analytics, schema evolution

### 6.3 Optimization Techniques
```sql
-- Delta Lake optimization
OPTIMIZE customers_clean;
OPTIMIZE orders_clean ZORDER BY (customer_id);

-- Check table history
DESCRIBE HISTORY orders_clean;
```

**💡 Key Learning:** Different storage formats and layers serve different use cases and performance requirements.

## Part 7: Snowflake Comparison (Optional - 30 minutes)

### 7.1 Architecture Differences
- **Databricks:** File-based lakehouse with Delta/Iceberg
- **Snowflake:** Cloud data warehouse with micro-partitions

### 7.2 SQL Differences
Key syntax changes needed for Snowflake:
```sql
-- Databricks (Delta)
CREATE TABLE orders USING DELTA PARTITIONED BY (date);

-- Snowflake equivalent
CREATE TABLE orders CLUSTER BY (date);
```

### 7.3 Feature Comparison
| Feature | Databricks | Snowflake |
|---------|------------|-----------|
| Storage Format | Delta Lake/Iceberg | Micro-partitions |
| ACID Transactions | ✅ | ✅ |
| Time Travel | ✅ | ✅ |
| Auto-scaling | ✅ | ✅ |
| ML Integration | Strong | Growing |
| Data Sharing | Good | Excellent |

## Lab Wrap-Up & Discussion (15 minutes)

### Key Takeaways
1. **Medallion architecture** provides clear data quality progression
2. **Storage formats** (Delta/Iceberg) offer significant advantages over traditional formats
3. **ELT approach** enables flexible, SQL-based transformations
4. **Layered approach** balances performance and flexibility

### Real-World Considerations
- **Data governance:** Implement proper access controls and lineage
- **Scalability:** Consider streaming and incremental processing
- **Monitoring:** Set up data quality and pipeline monitoring
- **Cost optimization:** Use appropriate cluster sizing and auto-scaling

### Discussion Questions
1. How would you implement this architecture for real-time requirements?
2. What data governance controls would you add for production?
3. How does this compare to your current Snowflake implementations?
4. What use cases favor Databricks vs Snowflake?

## Additional Resources

### Databricks Documentation
- [Delta Lake Guide](https://docs.databricks.com/delta/)
- [Medallion Architecture](https://docs.databricks.com/lakehouse/medallion.html)
- [Performance Tuning](https://docs.databricks.com/optimizations/)

### Hands-On Extensions
- Implement streaming data ingestion
- Add data quality monitoring
- Create automated ML pipelines
- Set up cross-cloud data sharing

### Sample Code Repository
All lab code is available in this repository with detailed comments and additional examples for further exploration.

---

**Lab Complete!** 🎉

You've successfully built a complete data lakehouse with medallion architecture, processed 10M+ records, and created business-ready analytics. This foundation can be extended for real-world production use cases.
