-- Bronze Layer DDL Scripts for E-Commerce Data Lakehouse
-- Creates raw data tables in Delta format for Databricks/Snowflake HOL

-- Set database context
CREATE DATABASE IF NOT EXISTS ecommerce_bronze;
USE ecommerce_bronze;

-- 1. Categories Raw Table
CREATE OR REPLACE TABLE categories_raw (
    category_id BIGINT,
    category_name STRING,
    parent_category_id BIGINT,
    description STRING,
    created_at TIMESTAMP,
    status STRING
) USING DELTA;

-- 2. Suppliers Raw Table  
CREATE OR REPLACE TABLE suppliers_raw (
    supplier_id BIGINT,
    supplier_name STRING,
    contact_person STRING,
    email STRING,
    phone STRING,
    address_line1 STRING,
    address_line2 STRING,
    city STRING,
    state STRING,
    country STRING,
    postal_code STRING,
    created_at TIMESTAMP,
    status STRING
) USING DELTA;

-- 3. Customers Raw Table
CREATE OR REPLACE TABLE customers_raw (
    customer_id BIGINT,
    first_name STRING,
    last_name STRING,
    email STRING,
    phone STRING,
    registration_date DATE,
    birth_date DATE,
    gender STRING,
    address_line1 STRING,
    address_line2 STRING,
    city STRING,
    state STRING,
    country STRING,
    postal_code STRING,
    created_at TIMESTAMP,
    source_system STRING
) USING DELTA;

-- 4. Products Raw Table
CREATE OR REPLACE TABLE products_raw (
    product_id BIGINT,
    product_name STRING,
    description STRING,
    category_id BIGINT,
    supplier_id BIGINT,
    sku STRING,
    price DECIMAL(10,2),
    cost DECIMAL(10,2),
    weight DECIMAL(8,2),
    dimensions STRING,
    color STRING,
    size STRING,
    brand STRING,
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    status STRING
) USING DELTA
;

-- 5. Orders Raw Table
CREATE OR REPLACE TABLE orders_raw (
    order_id BIGINT,
    customer_id BIGINT,
    order_date TIMESTAMP,
    order_status STRING,
    payment_method STRING,
    payment_status STRING,
    shipping_address STRING,
    billing_address STRING,
    total_amount DECIMAL(12,2),
    tax_amount DECIMAL(10,2),
    shipping_cost DECIMAL(8,2),
    discount_amount DECIMAL(10,2),
    created_at TIMESTAMP,
    updated_at TIMESTAMP
) USING DELTA;

-- 6. Order Items Raw Table (Largest table - ~6M records)
CREATE OR REPLACE TABLE order_items_raw (
    order_item_id BIGINT,
    order_id BIGINT,
    product_id BIGINT,
    quantity INT,
    unit_price DECIMAL(10,2),
    discount_amount DECIMAL(10,2),
    total_amount DECIMAL(12,2),
    created_at TIMESTAMP
) USING DELTA;

-- 7. Inventory Raw Table
CREATE OR REPLACE TABLE inventory_raw (
    inventory_id BIGINT,
    product_id BIGINT,
    warehouse_id INT,
    quantity_on_hand INT,
    quantity_reserved INT,
    quantity_available INT,
    reorder_level INT,
    last_updated TIMESTAMP,
    created_at TIMESTAMP
) USING DELTA;

-- 8. Reviews Raw Table
CREATE OR REPLACE TABLE reviews_raw (
    review_id BIGINT,
    product_id BIGINT,
    customer_id BIGINT,
    rating INT,
    review_text STRING,
    review_date DATE,
    verified_purchase BOOLEAN,
    helpful_votes INT,
    created_at TIMESTAMP
) USING DELTA;

-- 9. Web Events Raw Table
CREATE OR REPLACE TABLE web_events_raw (
    event_id BIGINT,
    session_id STRING,
    customer_id BIGINT,
    event_type STRING,
    page_url STRING,
    product_id BIGINT,
    timestamp TIMESTAMP,
    ip_address STRING,
    user_agent STRING,
    referrer STRING,
    created_at TIMESTAMP
) USING DELTA;

-- 10. Shipping Raw Table
CREATE OR REPLACE TABLE shipping_raw (
    shipping_id BIGINT,
    order_id BIGINT,
    carrier STRING,
    tracking_number STRING,
    shipping_method STRING,
    shipped_date DATE,
    estimated_delivery DATE,
    actual_delivery DATE,
    shipping_status STRING,
    created_at TIMESTAMP,
    updated_at TIMESTAMP
) USING DELTA;

-- Create views for easier access
CREATE OR REPLACE VIEW bronze_table_summary AS
SELECT 
    'categories_raw' as table_name,
    COUNT(*) as record_count,
    MAX(created_at) as last_updated
FROM categories_raw
UNION ALL
SELECT 
    'suppliers_raw' as table_name,
    COUNT(*) as record_count,
    MAX(created_at) as last_updated
FROM suppliers_raw
UNION ALL
SELECT 
    'customers_raw' as table_name,
    COUNT(*) as record_count,
    MAX(created_at) as last_updated
FROM customers_raw
UNION ALL
SELECT 
    'products_raw' as table_name,
    COUNT(*) as record_count,
    MAX(created_at) as last_updated
FROM products_raw
UNION ALL
SELECT 
    'orders_raw' as table_name,
    COUNT(*) as record_count,
    MAX(created_at) as last_updated
FROM orders_raw
UNION ALL
SELECT 
    'order_items_raw' as table_name,
    COUNT(*) as record_count,
    MAX(created_at) as last_updated
FROM order_items_raw
UNION ALL
SELECT 
    'inventory_raw' as table_name,
    COUNT(*) as record_count,
    MAX(last_updated) as last_updated
FROM inventory_raw
UNION ALL
SELECT 
    'reviews_raw' as table_name,
    COUNT(*) as record_count,
    MAX(created_at) as last_updated
FROM reviews_raw
UNION ALL
SELECT 
    'web_events_raw' as table_name,
    COUNT(*) as record_count,
    MAX(created_at) as last_updated
FROM web_events_raw
UNION ALL
SELECT 
    'shipping_raw' as table_name,
    COUNT(*) as record_count,
    MAX(created_at) as last_updated
FROM shipping_raw;

-- Display summary
SELECT * FROM bronze_table_summary ORDER BY record_count DESC;
