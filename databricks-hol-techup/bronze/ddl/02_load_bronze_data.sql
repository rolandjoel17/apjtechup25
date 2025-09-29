-- Load data into Bronze layer tables from CSV files
-- Run this after generating sample data with the Python script

use catalog apjtechup;
USE schema bronze;

-- Load Categories
create or replace table categories_raw
select * from hive_metastore.ecommerce_hol.categories_raw;

-- Load Suppliers
create or replace table suppliers_raw
select * from hive_metastore.ecommerce_hol.suppliers_raw;

-- Load Customers
create or replace table customers_raw
select * from hive_metastore.ecommerce_hol.customers_raw;

-- Load Products
create or replace table products_raw
select * from hive_metastore.ecommerce_hol.products_raw;

-- Load Orders
create or replace table orders_raw
select * from hive_metastore.ecommerce_hol.orders_raw;

-- Load Order Items (largest table)
create or replace table order_items_raw
select * from hive_metastore.ecommerce_hol.order_items_raw;

-- Load Inventory
create or replace table inventory_raw
select * from hive_metastore.ecommerce_hol.inventory_raw;

-- Load Reviews
create or replace table reviews_raw
select * from hive_metastore.ecommerce_hol.reviews_raw;

-- Load Web Events
create or replace table web_events_raw
select * from hive_metastore.ecommerce_hol.web_events_raw;

-- Load Shipping
create or replace table shipping_raw
select * from hive_metastore.ecommerce_hol.shipping_raw;


-- Run OPTIMIZE and ANALYZE on all tables for better performance
OPTIMIZE categories_raw;
OPTIMIZE suppliers_raw;
OPTIMIZE customers_raw;
OPTIMIZE products_raw;
OPTIMIZE orders_raw;
OPTIMIZE order_items_raw;
OPTIMIZE inventory_raw;
OPTIMIZE reviews_raw;
OPTIMIZE web_events_raw;
OPTIMIZE shipping_raw;

-- Analyze tables for query optimization
ANALYZE TABLE categories_raw COMPUTE STATISTICS;
ANALYZE TABLE suppliers_raw COMPUTE STATISTICS;
ANALYZE TABLE customers_raw COMPUTE STATISTICS;
ANALYZE TABLE products_raw COMPUTE STATISTICS;
ANALYZE TABLE orders_raw COMPUTE STATISTICS;
ANALYZE TABLE order_items_raw COMPUTE STATISTICS;
ANALYZE TABLE inventory_raw COMPUTE STATISTICS;
ANALYZE TABLE reviews_raw COMPUTE STATISTICS;
ANALYZE TABLE web_events_raw COMPUTE STATISTICS;
ANALYZE TABLE shipping_raw COMPUTE STATISTICS;