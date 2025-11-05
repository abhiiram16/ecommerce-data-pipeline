-- ============================================
-- PHASE 4.3: SQL VIEWS FOR ANALYTICS
-- ============================================
-- Purpose: Pre-built queries for instant business intelligence
-- Date: November 5, 2025
-- Author: Abhiiram
-- ============================================
-- VIEW 1: Top Customers (VIP segment)
-- Filters active customers and categorizes by tier
CREATE OR REPLACE VIEW top_customers AS
SELECT customer_id,
    customer_name,
    email,
    city,
    state,
    total_orders,
    total_spent,
    avg_order_value,
    days_since_last_order,
    customer_status,
    CASE
        WHEN total_spent > 500000 THEN 'VIP'
        WHEN total_spent > 250000 THEN 'Premium'
        ELSE 'Standard'
    END AS customer_tier
FROM customer_summary
WHERE customer_status = 'Active'
ORDER BY total_spent DESC;
-- VIEW 2: Top Products (Best sellers)
CREATE OR REPLACE VIEW top_products AS
SELECT product_id,
    product_name,
    category,
    brand,
    times_sold,
    total_units_sold,
    total_revenue,
    profit_margin_pct,
    unique_customers,
    CASE
        WHEN total_revenue > 10000000 THEN 'Bestseller'
        WHEN total_revenue > 5000000 THEN 'Popular'
        ELSE 'Standard'
    END AS product_tier
FROM product_summary
WHERE times_sold > 0
ORDER BY total_revenue DESC;
-- VIEW 3: Recent Orders (Last 7 days)
CREATE OR REPLACE VIEW recent_orders AS
SELECT o.order_id,
    c.first_name || ' ' || c.last_name AS customer_name,
    p.product_name,
    o.quantity,
    o.total_amount,
    o.order_date::DATE AS order_date,
    o.order_status,
    CURRENT_DATE - o.order_date::DATE AS days_ago
FROM orders o
    JOIN customers c ON o.customer_id = c.customer_id
    JOIN products p ON o.product_id = p.product_id
WHERE o.order_date::DATE >= CURRENT_DATE - INTERVAL '7 days'
ORDER BY o.order_date DESC;
-- VIEW 4: Customer RFM Segmentation
CREATE OR REPLACE VIEW customer_rfm_segments AS
SELECT customer_id,
    customer_name,
    email,
    total_orders AS frequency,
    days_since_last_order AS recency,
    total_spent AS monetary,
    CASE
        WHEN days_since_last_order <= 30
        AND total_spent > 500000 THEN 'Champions'
        WHEN days_since_last_order <= 30
        AND total_spent > 250000 THEN 'Loyal VIPs'
        WHEN days_since_last_order <= 30 THEN 'At Risk - High Value'
        WHEN days_since_last_order <= 90 THEN 'Potential Reactivation'
        ELSE 'Dormant'
    END AS rfm_segment
FROM customer_summary
ORDER BY total_spent DESC;
-- VIEW 5: Weekly Sales Trends
CREATE OR REPLACE VIEW sales_trends_weekly AS
SELECT DATE_TRUNC('week', sale_date)::DATE AS week_start,
    TO_CHAR(DATE_TRUNC('week', sale_date), 'YYYY-MM-DD') || ' to ' || TO_CHAR(
        DATE_TRUNC('week', sale_date) + INTERVAL '6 days',
        'YYYY-MM-DD'
    ) AS week_range,
    SUM(total_orders) AS weekly_orders,
    SUM(unique_customers) AS weekly_customers,
    SUM(total_revenue) AS weekly_revenue,
    ROUND(AVG(avg_order_value), 2) AS avg_order_value,
    ROUND(
        SUM(total_revenue)::NUMERIC / NULLIF(SUM(total_orders), 0),
        2
    ) AS revenue_per_order
FROM daily_sales_summary
GROUP BY DATE_TRUNC('week', sale_date)
ORDER BY week_start DESC;