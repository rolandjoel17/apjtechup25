# E-Commerce Data Schema Design

## Bronze Layer (Raw Data) - 10 Tables

### 1. customers_raw (~500k records)
- customer_id, first_name, last_name, email, phone, registration_date, birth_date, gender, address_line1, address_line2, city, state, country, postal_code, created_at, source_system

### 2. products_raw (~100k records)  
- product_id, product_name, description, category_id, supplier_id, sku, price, cost, weight, dimensions, color, size, brand, created_at, updated_at, status

### 3. orders_raw (~2M records)
- order_id, customer_id, order_date, order_status, payment_method, payment_status, shipping_address, billing_address, total_amount, tax_amount, shipping_cost, discount_amount, created_at, updated_at

### 4. order_items_raw (~6M records) 
- order_item_id, order_id, product_id, quantity, unit_price, discount_amount, total_amount, created_at

### 5. inventory_raw (~200k records)
- inventory_id, product_id, warehouse_id, quantity_on_hand, quantity_reserved, quantity_available, reorder_level, last_updated, created_at

### 6. suppliers_raw (~1k records)
- supplier_id, supplier_name, contact_person, email, phone, address_line1, address_line2, city, state, country, postal_code, created_at, status

### 7. categories_raw (~500 records)
- category_id, category_name, parent_category_id, description, created_at, status

### 8. reviews_raw (~800k records)
- review_id, product_id, customer_id, rating, review_text, review_date, verified_purchase, helpful_votes, created_at

### 9. web_events_raw (~300k records)
- event_id, session_id, customer_id, event_type, page_url, product_id, timestamp, ip_address, user_agent, referrer, created_at

### 10. shipping_raw (~2M records)
- shipping_id, order_id, carrier, tracking_number, shipping_method, shipped_date, estimated_delivery, actual_delivery, shipping_status, created_at, updated_at

## Silver Layer (Cleaned & Enriched) - 6 Tables

### 1. customers_clean
- Clean addresses, standardize phone/email formats, derive age from birth_date, customer segmentation flags

### 2. products_clean  
- Clean product data, standardize categories, calculate profit margins, add product lifecycle status

### 3. orders_clean
- Clean order data, add calculated fields (order_value_category, days_to_ship), status standardization

### 4. order_items_clean
- Clean line item data, add product enrichment, calculate item-level metrics

### 5. inventory_clean
- Inventory with turnover calculations, stock level categories, reorder recommendations

### 6. web_events_clean
- Sessionized web events, page categorization, customer journey mapping

## Gold Layer (Analytics Ready) - 4 Tables

### 1. customer_metrics
- Customer lifetime value, purchase frequency, average order value, churn risk, segment classification

### 2. product_performance
- Sales metrics, inventory turnover, review summaries, profitability analysis, trending indicators

### 3. sales_summary  
- Daily/monthly sales aggregations, growth metrics, seasonal patterns, geographic breakdowns

### 4. inventory_insights
- Stock optimization recommendations, demand forecasting, supplier performance, warehouse efficiency

## Data Volume Distribution

| Layer | Total Records | Largest Table | 
|-------|---------------|---------------|
| Bronze | ~10.1M | order_items_raw (6M) |
| Silver | ~9.5M | order_items_clean (6M) |  
| Gold | ~50k | customer_metrics (500k) |

## Key Relationships

- customers ← orders ← order_items → products
- products → categories, suppliers, inventory, reviews
- orders → shipping
- customers → web_events, reviews
