-- Sample Analytical Queries for E-Commerce Data Lakehouse Gold Layer
-- These queries demonstrate the business value of the medallion architecture

USE ecommerce_gold;

-- =============================================================================
-- 1. EXECUTIVE DASHBOARD QUERIES
-- =============================================================================

-- Executive Summary KPIs
SELECT 
    'Executive Summary' as report_name,
    CURRENT_DATE() as report_date;

-- Overall Business Health
WITH business_kpis AS (
    SELECT 
        SUM(gross_revenue) as total_revenue,
        SUM(gross_profit) as total_profit,
        AVG(gross_margin_percentage) as avg_margin,
        SUM(total_orders) as total_orders,
        SUM(total_customers) as total_customers,
        SUM(new_customers) as new_customers
    FROM sales_summary 
    WHERE date_key >= CURRENT_DATE() - INTERVAL 30 DAYS
)
SELECT 
    'Business KPIs (Last 30 Days)' as metric_group,
    CONCAT('$', FORMAT_NUMBER(total_revenue, 2)) as total_revenue,
    CONCAT('$', FORMAT_NUMBER(total_profit, 2)) as total_profit,
    CONCAT(ROUND(avg_margin, 1), '%') as avg_margin,
    FORMAT_NUMBER(total_orders, 0) as total_orders,
    FORMAT_NUMBER(total_customers, 0) as total_customers,
    FORMAT_NUMBER(new_customers, 0) as new_customers,
    CONCAT(ROUND(new_customers * 100.0 / total_customers, 1), '%') as new_customer_rate
FROM business_kpis;

-- =============================================================================
-- 2. CUSTOMER ANALYTICS
-- =============================================================================

-- Customer Lifetime Value Analysis
SELECT 'Top 20 Customers by LTV' as analysis_type;
SELECT 
    customer_name,
    customer_segment,
    ltv_tier,
    CONCAT('$', FORMAT_NUMBER(customer_lifetime_value, 2)) as lifetime_value,
    total_orders,
    CONCAT('$', FORMAT_NUMBER(average_order_value, 2)) as avg_order_value,
    favorite_category,
    CASE 
        WHEN days_since_last_order <= 30 THEN 'Active'
        WHEN days_since_last_order <= 90 THEN 'At Risk'
        ELSE 'Inactive'
    END as status
FROM customer_metrics 
WHERE total_orders > 0
ORDER BY customer_lifetime_value DESC 
LIMIT 20;

-- Customer Segmentation Analysis
SELECT 'Customer Segmentation' as analysis_type;
SELECT 
    customer_segment,
    COUNT(*) as customer_count,
    CONCAT(ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 1), '%') as percentage,
    CONCAT('$', FORMAT_NUMBER(AVG(customer_lifetime_value), 2)) as avg_ltv,
    ROUND(AVG(total_orders), 1) as avg_orders,
    CONCAT('$', FORMAT_NUMBER(AVG(average_order_value), 2)) as avg_order_value,
    CONCAT(ROUND(AVG(churn_risk_score), 2)) as avg_churn_risk
FROM customer_metrics
GROUP BY customer_segment
ORDER BY avg_ltv DESC;

-- Churn Risk Analysis
SELECT 'High Churn Risk Customers' as analysis_type;
SELECT 
    customer_name,
    customer_segment,
    ltv_tier,
    CONCAT('$', FORMAT_NUMBER(customer_lifetime_value, 2)) as lifetime_value,
    days_since_last_order,
    total_orders,
    churn_risk_score,
    favorite_category
FROM customer_metrics 
WHERE churn_risk_category = 'High Risk' 
  AND customer_lifetime_value > 200
ORDER BY customer_lifetime_value DESC 
LIMIT 20;

-- =============================================================================
-- 3. PRODUCT ANALYTICS
-- =============================================================================

-- Top Performing Products
SELECT 'Top 20 Products by Revenue' as analysis_type;
SELECT 
    product_name,
    category_name,
    brand,
    performance_tier,
    CONCAT('$', FORMAT_NUMBER(total_revenue, 2)) as revenue,
    FORMAT_NUMBER(total_quantity_sold, 0) as units_sold,
    CONCAT('$', FORMAT_NUMBER(current_price, 2)) as price,
    ROUND(average_rating, 1) as rating,
    total_reviews,
    velocity_category
FROM product_performance 
ORDER BY total_revenue DESC 
LIMIT 20;

-- Category Performance Analysis
SELECT 'Category Performance' as analysis_type;
SELECT 
    category_name,
    COUNT(*) as product_count,
    CONCAT('$', FORMAT_NUMBER(SUM(total_revenue), 2)) as total_revenue,
    CONCAT('$', FORMAT_NUMBER(AVG(total_revenue), 2)) as avg_revenue_per_product,
    ROUND(AVG(average_rating), 1) as avg_rating,
    SUM(total_quantity_sold) as total_units_sold,
    COUNT(CASE WHEN performance_tier IN ('Star', 'Strong') THEN 1 END) as top_performers,
    ROUND(AVG(inventory_turnover_ratio), 1) as avg_turnover
FROM product_performance
GROUP BY category_name
ORDER BY SUM(total_revenue) DESC;

-- Product Lifecycle Analysis
SELECT 'Product Lifecycle Distribution' as analysis_type;
SELECT 
    lifecycle_stage,
    COUNT(*) as product_count,
    CONCAT('$', FORMAT_NUMBER(SUM(total_revenue), 2)) as total_revenue,
    CONCAT('$', FORMAT_NUMBER(AVG(total_revenue), 2)) as avg_revenue,
    ROUND(AVG(average_rating), 1) as avg_rating,
    COUNT(CASE WHEN performance_tier = 'Star' THEN 1 END) as star_products
FROM product_performance
GROUP BY lifecycle_stage
ORDER BY SUM(total_revenue) DESC;

-- Inventory Optimization Opportunities
SELECT 'Inventory Optimization Alerts' as analysis_type;
SELECT 
    product_name,
    category_name,
    warehouse_region,
    current_stock_level,
    optimal_stock_level,
    days_supply_current,
    classification,
    CASE 
        WHEN needs_reorder THEN 'Reorder Needed'
        WHEN overstocked THEN 'Overstocked'
        WHEN is_slow_moving THEN 'Slow Moving'
        ELSE 'Normal'
    END as alert_type,
    CONCAT('$', FORMAT_NUMBER(inventory_value, 2)) as inventory_value
FROM inventory_insights
WHERE needs_reorder = TRUE 
   OR overstocked = TRUE 
   OR is_slow_moving = TRUE
ORDER BY inventory_value DESC
LIMIT 25;

-- =============================================================================
-- 4. SALES TREND ANALYSIS
-- =============================================================================

-- Monthly Sales Trends (Last 12 Months)
SELECT 'Monthly Sales Trends' as analysis_type;
SELECT 
    CONCAT(year, '-', LPAD(month, 2, '0')) as month_year,
    CONCAT('$', FORMAT_NUMBER(SUM(gross_revenue), 2)) as revenue,
    FORMAT_NUMBER(SUM(total_orders), 0) as orders,
    FORMAT_NUMBER(SUM(total_customers), 0) as customers,
    CONCAT('$', FORMAT_NUMBER(AVG(average_order_value), 2)) as avg_order_value,
    ROUND(AVG(gross_margin_percentage), 1) as margin_percent
FROM sales_summary
WHERE date_key >= CURRENT_DATE() - INTERVAL 365 DAYS
GROUP BY year, month
ORDER BY year DESC, month DESC
LIMIT 12;

-- Seasonal Patterns
SELECT 'Quarterly Sales Comparison' as analysis_type;
SELECT 
    quarter,
    COUNT(DISTINCT date_key) as days_in_quarter,
    CONCAT('$', FORMAT_NUMBER(SUM(gross_revenue), 2)) as total_revenue,
    CONCAT('$', FORMAT_NUMBER(SUM(gross_revenue) / COUNT(DISTINCT date_key), 2)) as avg_daily_revenue,
    FORMAT_NUMBER(SUM(total_orders), 0) as total_orders,
    ROUND(SUM(total_orders) / COUNT(DISTINCT date_key), 0) as avg_daily_orders
FROM sales_summary
WHERE date_key >= CURRENT_DATE() - INTERVAL 365 DAYS
GROUP BY quarter
ORDER BY quarter;

-- Day of Week Performance
SELECT 'Day of Week Performance' as analysis_type;
SELECT 
    day_of_week,
    COUNT(DISTINCT date_key) as total_days,
    CONCAT('$', FORMAT_NUMBER(AVG(gross_revenue), 2)) as avg_daily_revenue,
    ROUND(AVG(total_orders), 0) as avg_daily_orders,
    CONCAT('$', FORMAT_NUMBER(AVG(average_order_value), 2)) as avg_order_value,
    ROUND(AVG(gross_margin_percentage), 1) as avg_margin
FROM sales_summary
WHERE date_key >= CURRENT_DATE() - INTERVAL 90 DAYS
GROUP BY day_of_week
ORDER BY 
    CASE day_of_week
        WHEN 'Monday' THEN 1
        WHEN 'Tuesday' THEN 2
        WHEN 'Wednesday' THEN 3
        WHEN 'Thursday' THEN 4
        WHEN 'Friday' THEN 5
        WHEN 'Saturday' THEN 6
        WHEN 'Sunday' THEN 7
    END;

-- =============================================================================
-- 5. REGIONAL ANALYSIS
-- =============================================================================

-- Regional Performance
SELECT 'Regional Sales Performance' as analysis_type;
SELECT 
    region,
    COUNT(DISTINCT state) as states_count,
    CONCAT('$', FORMAT_NUMBER(SUM(gross_revenue), 2)) as total_revenue,
    FORMAT_NUMBER(SUM(total_orders), 0) as total_orders,
    FORMAT_NUMBER(SUM(total_customers), 0) as total_customers,
    CONCAT('$', FORMAT_NUMBER(AVG(average_order_value), 2)) as avg_order_value,
    ROUND(AVG(gross_margin_percentage), 1) as avg_margin
FROM sales_summary
WHERE date_key >= CURRENT_DATE() - INTERVAL 90 DAYS
GROUP BY region
ORDER BY SUM(gross_revenue) DESC;

-- =============================================================================
-- 6. COHORT ANALYSIS
-- =============================================================================

-- Customer Acquisition Cohorts
SELECT 'Monthly Customer Acquisition Cohorts' as analysis_type;
WITH customer_cohorts AS (
    SELECT 
        cm.customer_id,
        DATE_TRUNC('month', cm.first_order_date) as cohort_month,
        cm.customer_lifetime_value,
        cm.total_orders,
        DATEDIFF(CURRENT_DATE(), cm.first_order_date) as days_since_first_order
    FROM customer_metrics cm
    WHERE cm.first_order_date IS NOT NULL
)
SELECT 
    cohort_month,
    COUNT(*) as customers_acquired,
    CONCAT('$', FORMAT_NUMBER(AVG(customer_lifetime_value), 2)) as avg_ltv,
    ROUND(AVG(total_orders), 1) as avg_orders,
    COUNT(CASE WHEN total_orders > 1 THEN 1 END) as repeat_customers,
    CONCAT(ROUND(COUNT(CASE WHEN total_orders > 1 THEN 1 END) * 100.0 / COUNT(*), 1), '%') as repeat_rate
FROM customer_cohorts
WHERE cohort_month >= DATE_TRUNC('month', CURRENT_DATE() - INTERVAL 12 MONTHS)
GROUP BY cohort_month
ORDER BY cohort_month DESC;

-- =============================================================================
-- 7. ADVANCED ANALYTICS
-- =============================================================================

-- Price Elasticity Analysis
SELECT 'Price Tier Performance' as analysis_type;
SELECT 
    price_tier,
    COUNT(*) as product_count,
    CONCAT('$', FORMAT_NUMBER(AVG(current_price), 2)) as avg_price,
    SUM(total_quantity_sold) as total_units_sold,
    ROUND(AVG(conversion_rate), 2) as avg_conversion_rate,
    ROUND(AVG(average_rating), 1) as avg_rating,
    COUNT(CASE WHEN velocity_category = 'Fast Moving' THEN 1 END) as fast_moving_products
FROM product_performance
GROUP BY price_tier
ORDER BY 
    CASE price_tier
        WHEN 'Budget' THEN 1
        WHEN 'Mid-Range' THEN 2
        WHEN 'Premium' THEN 3
        WHEN 'Luxury' THEN 4
    END;

-- Cross-Category Purchase Analysis
SELECT 'Customer Category Diversity' as analysis_type;
WITH customer_categories AS (
    SELECT 
        cm.customer_id,
        cm.customer_name,
        cm.customer_segment,
        cm.category_diversity_score,
        cm.customer_lifetime_value,
        cm.total_orders
    FROM customer_metrics cm
    WHERE cm.total_orders >= 3  -- Customers with multiple orders
)
SELECT 
    CASE 
        WHEN category_diversity_score >= 2.0 THEN 'High Diversity (2+ categories/order)'
        WHEN category_diversity_score >= 1.5 THEN 'Medium Diversity (1.5+ categories/order)'
        WHEN category_diversity_score >= 1.0 THEN 'Low Diversity (1+ category/order)'
        ELSE 'Single Category'
    END as diversity_level,
    COUNT(*) as customer_count,
    CONCAT('$', FORMAT_NUMBER(AVG(customer_lifetime_value), 2)) as avg_ltv,
    ROUND(AVG(total_orders), 1) as avg_orders,
    ROUND(AVG(category_diversity_score), 2) as avg_diversity_score
FROM customer_categories
GROUP BY 
    CASE 
        WHEN category_diversity_score >= 2.0 THEN 'High Diversity (2+ categories/order)'
        WHEN category_diversity_score >= 1.5 THEN 'Medium Diversity (1.5+ categories/order)'
        WHEN category_diversity_score >= 1.0 THEN 'Low Diversity (1+ category/order)'
        ELSE 'Single Category'
    END
ORDER BY AVG(customer_lifetime_value) DESC;

-- =============================================================================
-- 8. BUSINESS ALERTS & RECOMMENDATIONS
-- =============================================================================

-- Business Health Alerts
SELECT 'Business Health Alerts' as analysis_type;

-- Revenue decline alert
SELECT 'Revenue Decline Alert' as alert_type,
       CONCAT('Recent 7-day revenue is down by $', 
              FORMAT_NUMBER(recent_revenue - previous_revenue, 2)) as message
FROM (
    SELECT 
        SUM(CASE WHEN date_key >= CURRENT_DATE() - INTERVAL 7 DAYS THEN gross_revenue ELSE 0 END) as recent_revenue,
        SUM(CASE WHEN date_key >= CURRENT_DATE() - INTERVAL 14 DAYS 
                  AND date_key < CURRENT_DATE() - INTERVAL 7 DAYS THEN gross_revenue ELSE 0 END) as previous_revenue
    FROM sales_summary
) revenue_comparison
WHERE recent_revenue < previous_revenue * 0.9;  -- 10% decline

-- High value customer churn risk
SELECT 'High Value Customer Churn Risk' as alert_type,
       CONCAT(COUNT(*), ' high-value customers at risk of churning (LTV > $500, no orders in 60+ days)') as message
FROM customer_metrics 
WHERE customer_lifetime_value > 500 
  AND churn_risk_category = 'High Risk'
  AND days_since_last_order > 60;

-- Low inventory alerts
SELECT 'Low Inventory Alert' as alert_type,
       CONCAT(COUNT(*), ' products need immediate reordering (current stock < 7 days supply)') as message
FROM inventory_insights 
WHERE needs_reorder = TRUE 
  AND days_supply_current < 7
  AND classification IN ('A', 'B');

SELECT 'Analysis Complete' as status, CURRENT_TIMESTAMP() as completed_at;
