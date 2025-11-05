-- ============================================
-- AGGREGATE TABLES FOR E-COMMERCE ANALYTICS
-- ============================================
-- Purpose: Pre-calculated summaries for fast business intelligence queries
-- Author: Abhiiram
-- Date: November 5, 2025
-- ============================================
-- ============================================
-- 1. CUSTOMER SUMMARY TABLE
-- ============================================
-- Aggregates customer lifetime value, order frequency, and recency
DROP TABLE IF EXISTS customer_summary CASCADE;
CREATE TABLE customer_summary AS
SELECT c.customer_id,
    c.first_name || ' ' || c.last_name AS customer_name,
    c.email,
    c.city,
    c.state,
    -- Order Metrics
    COUNT(o.order_id) AS total_orders,
    COALESCE(SUM(o.total_amount), 0) AS total_spent,
    COALESCE(AVG(o.total_amount), 0) AS avg_order_value,
    -- Time Metrics
    MIN(o.order_date) AS first_order_date,
    MAX(o.order_date) AS last_order_date,
    CURRENT_DATE - MAX(o.order_date)::DATE AS days_since_last_order,
    -- Product Diversity
    COUNT(DISTINCT o.product_id) AS unique_products_purchased,
    -- Status Classification
    CASE
        WHEN MAX(o.order_date) >= CURRENT_DATE - INTERVAL '30 days' THEN 'Active'
        WHEN MAX(o.order_date) >= CURRENT_DATE - INTERVAL '90 days' THEN 'At Risk'
        ELSE 'Churned'
    END AS customer_status,
    -- Record Timestamp
    CURRENT_TIMESTAMP AS last_updated
FROM customers c
    LEFT JOIN orders o ON c.customer_id = o.customer_id
WHERE o.order_status = 'Delivered' -- Only count successful orders
GROUP BY c.customer_id,
    c.first_name,
    c.last_name,
    c.email,
    c.city,
    c.state
ORDER BY total_spent DESC;
-- Add indexes for fast queries
CREATE INDEX idx_customer_summary_total_spent ON customer_summary(total_spent DESC);
CREATE INDEX idx_customer_summary_status ON customer_summary(customer_status);
CREATE INDEX idx_customer_summary_city ON customer_summary(city);
-- Display summary statistics
SELECT 'Total Customers' AS metric,
    COUNT(*) AS value
FROM customer_summary
UNION ALL
SELECT 'Active Customers',
    COUNT(*)
FROM customer_summary
WHERE customer_status = 'Active'
UNION ALL
SELECT 'Average Customer Lifetime Value',
    ROUND(AVG(total_spent), 2)
FROM customer_summary
UNION ALL
SELECT 'Top Customer Spent',
    MAX(total_spent)
FROM customer_summary;
-- Show top 10 customers
SELECT customer_name,
    email,
    total_orders,
    total_spent,
    avg_order_value,
    customer_status,
    days_since_last_order
FROM customer_summary
ORDER BY total_spent DESC
LIMIT 10;