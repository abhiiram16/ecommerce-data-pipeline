-- ========================================
-- AGGREGATE TABLES FOR E-COMMERCE ANALYTICS
-- ========================================
-- Purpose: Pre-calculated summaries for fast business intelligence
-- Author: Abhiiram
-- Date: November 6, 2025
-- ========================================
-- ========================================
-- 1. CUSTOMER SUMMARY TABLE
-- ========================================
DROP TABLE IF EXISTS customer_summary CASCADE;
CREATE TABLE customer_summary AS
SELECT c.customer_id,
    c.first_name || ' ' || c.last_name AS customer_name,
    c.email,
    c.city,
    c.state,
    COUNT(o.order_id) AS total_orders,
    COALESCE(SUM(o.total_amount), 0) AS total_spent,
    COALESCE(AVG(o.total_amount), 0) AS avg_order_value,
    MIN(o.order_date) AS first_purchase_date,
    MAX(o.order_date) AS last_purchase_date,
    CURRENT_DATE - MAX(o.order_date)::DATE AS days_since_last_order,
    COUNT(DISTINCT o.product_id) AS unique_products_purchased,
    CASE
        WHEN MAX(o.order_date) >= CURRENT_DATE - INTERVAL '30 days' THEN 'Active'
        WHEN MAX(o.order_date) >= CURRENT_DATE - INTERVAL '90 days' THEN 'At Risk'
        ELSE 'Churned'
    END AS customer_status,
    CURRENT_TIMESTAMP AS last_updated
FROM customers c
    LEFT JOIN orders o ON c.customer_id = o.customer_id
    AND o.order_status = 'Delivered'
GROUP BY c.customer_id,
    c.first_name,
    c.last_name,
    c.email,
    c.city,
    c.state
ORDER BY total_spent DESC;
CREATE INDEX idx_customer_summary_total_spent ON customer_summary(total_spent DESC);
CREATE INDEX idx_customer_summary_status ON customer_summary(customer_status);
-- ========================================
-- 2. PRODUCT SUMMARY TABLE
-- ========================================
DROP TABLE IF EXISTS product_summary CASCADE;
CREATE TABLE product_summary AS
SELECT p.product_id,
    p.product_name,
    p.category,
    p.brand,
    p.price,
    COUNT(o.order_id) AS times_sold,
    COUNT(DISTINCT o.order_id) AS total_orders,
    COALESCE(SUM(o.quantity), 0) AS total_units_sold,
    COALESCE(SUM(o.total_amount), 0) AS total_revenue,
    COUNT(DISTINCT o.customer_id) AS unique_customers,
    ROUND(
        ((p.price - p.cost) / NULLIF(p.price, 0)) * 100,
        2
    ) AS profit_margin_pct,
    CURRENT_TIMESTAMP AS last_updated
FROM products p
    LEFT JOIN orders o ON p.product_id = o.product_id
    AND o.order_status = 'Delivered'
GROUP BY p.product_id,
    p.product_name,
    p.category,
    p.brand,
    p.price,
    p.cost
ORDER BY total_revenue DESC;
CREATE INDEX idx_product_summary_revenue ON product_summary(total_revenue DESC);
CREATE INDEX idx_product_summary_category ON product_summary(category);
-- ========================================
-- 3. DAILY SALES SUMMARY TABLE
-- ========================================
DROP TABLE IF EXISTS daily_sales_summary CASCADE;
CREATE TABLE daily_sales_summary AS
SELECT DATE(o.order_date) AS sale_date,
    COUNT(o.order_id) AS total_orders,
    COUNT(DISTINCT o.customer_id) AS unique_customers,
    COALESCE(SUM(o.total_amount), 0) AS total_revenue,
    ROUND(COALESCE(AVG(o.total_amount), 0), 2) AS avg_order_value,
    COALESCE(SUM(o.quantity), 0) AS total_units,
    CURRENT_TIMESTAMP AS last_updated
FROM orders o
WHERE o.order_status = 'Delivered'
GROUP BY DATE(o.order_date)
ORDER BY sale_date DESC;
CREATE INDEX idx_daily_sales_date ON daily_sales_summary(sale_date DESC);
CREATE INDEX idx_daily_sales_revenue ON daily_sales_summary(total_revenue DESC);
-- ========================================
-- 4. MONTHLY SALES SUMMARY TABLE
-- ========================================
DROP TABLE IF EXISTS monthly_sales_summary CASCADE;
CREATE TABLE monthly_sales_summary AS
SELECT DATE_TRUNC('month', o.order_date)::DATE AS month,
    COUNT(o.order_id) AS total_orders,
    COUNT(DISTINCT o.customer_id) AS unique_customers,
    COALESCE(SUM(o.total_amount), 0) AS total_revenue,
    ROUND(COALESCE(AVG(o.total_amount), 0), 2) AS avg_order_value,
    COALESCE(SUM(o.quantity), 0) AS total_units,
    LAG(SUM(o.total_amount)) OVER (
        ORDER BY DATE_TRUNC('month', o.order_date)
    ) AS prev_month_revenue,
    ROUND(
        (
            SUM(o.total_amount) - LAG(SUM(o.total_amount)) OVER (
                ORDER BY DATE_TRUNC('month', o.order_date)
            )
        ) / NULLIF(
            LAG(SUM(o.total_amount)) OVER (
                ORDER BY DATE_TRUNC('month', o.order_date)
            ),
            0
        ) * 100,
        2
    ) AS mom_growth_pct,
    CURRENT_TIMESTAMP AS last_updated
FROM orders o
WHERE o.order_status = 'Delivered'
GROUP BY DATE_TRUNC('month', o.order_date)
ORDER BY month DESC;
CREATE INDEX idx_monthly_sales_month ON monthly_sales_summary(month DESC);
CREATE INDEX idx_monthly_sales_revenue ON monthly_sales_summary(total_revenue DESC);
-- ========================================
-- VERIFICATION QUERIES
-- ========================================
SELECT 'Aggregate tables created!' AS status;
SELECT 'Customers' AS table_name,
    COUNT(*) AS row_count
FROM customer_summary
UNION ALL
SELECT 'Products',
    COUNT(*)
FROM product_summary
UNION ALL
SELECT 'Daily Sales',
    COUNT(*)
FROM daily_sales_summary
UNION ALL
SELECT 'Monthly Sales',
    COUNT(*)
FROM monthly_sales_summary;