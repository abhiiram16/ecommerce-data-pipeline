-- ========================================
-- PHASE 4: DATA PROCESSING & TRANSFORMATION
-- ========================================
-- AGGREGATE TABLES FOR BUSINESS INTELLIGENCE
-- ========================================
-- Purpose: Documentation and verification of aggregate tables
-- Author: Abhiiram
-- Date: November 6, 2025
-- Database: ecommerce_db
-- ========================================
-- This file contains verification and documentation for 4 materialized aggregate tables:
-- 1. customer_summary - Customer lifetime value & RFM metrics
-- 2. product_summary - Product performance & profitability
-- 3. daily_sales_summary - Daily sales aggregations
-- 4. monthly_sales_summary - Monthly trends & growth
-- ========================================
-- VERIFICATION QUERIES
-- ========================================
-- Check 1: Verify customer_summary integrity
SELECT 'customer_summary' AS table_name,
    COUNT(*) AS total_records,
    COUNT(DISTINCT customer_id) AS unique_customers,
    ROUND(SUM(total_spent), 2) AS total_customer_revenue,
    ROUND(AVG(avg_order_value), 2) AS avg_order_value,
    MIN(customer_status) AS min_status,
    MAX(customer_status) AS max_status
FROM customer_summary;
-- Check 2: Verify product_summary integrity
SELECT 'product_summary' AS table_name,
    COUNT(*) AS total_products,
    SUM(times_sold) AS total_sales,
    SUM(total_units_sold) AS total_units,
    ROUND(SUM(total_revenue), 2) AS total_product_revenue,
    ROUND(AVG(profit_margin_pct), 2) AS avg_profit_margin,
    COUNT(DISTINCT category) AS categories
FROM product_summary;
-- Check 3: Verify daily_sales_summary integrity
SELECT 'daily_sales_summary' AS table_name,
    COUNT(*) AS total_days,
    MIN(sale_date) AS earliest_date,
    MAX(sale_date) AS latest_date,
    SUM(total_orders) AS cumulative_orders,
    ROUND(SUM(total_revenue), 2) AS total_daily_revenue,
    ROUND(AVG(avg_order_value), 2) AS average_aov
FROM daily_sales_summary;
-- Check 4: Verify monthly_sales_summary integrity
SELECT 'monthly_sales_summary' AS table_name,
    COUNT(*) AS total_months,
    MIN(month) AS earliest_month,
    MAX(month) AS latest_month,
    SUM(total_orders) AS total_monthly_orders,
    ROUND(SUM(total_revenue), 2) AS total_monthly_revenue,
    ROUND(AVG(mom_growth_pct), 2) AS avg_mom_growth
FROM monthly_sales_summary;
-- Check 5: Cross-table consistency verification
SELECT 'Revenue Consistency' AS check_name,
    ROUND(
        (
            SELECT SUM(total_revenue)
            FROM daily_sales_summary
        ),
        2
    ) AS daily_total,
    ROUND(
        (
            SELECT SUM(total_revenue)
            FROM monthly_sales_summary
        ),
        2
    ) AS monthly_total,
    ROUND(
        (
            SELECT SUM(total_spent)
            FROM customer_summary
        ),
        2
    ) AS customer_total
UNION ALL
SELECT 'Order Consistency',
    (
        SELECT SUM(total_orders)
        FROM daily_sales_summary
    ),
    (
        SELECT SUM(total_orders)
        FROM monthly_sales_summary
    ),
    (
        SELECT SUM(total_orders)
        FROM customer_summary
    );
-- ========================================
-- SUMMARY STATISTICS
-- ========================================
SELECT 'AGGREGATE TABLES SUMMARY' AS report_name,
    CURRENT_TIMESTAMP AS generated_at;
-- Top 5 customers by spending
SELECT 'Top 5 Customers' AS report;
SELECT customer_name,
    total_spent,
    total_orders
FROM customer_summary
ORDER BY total_spent DESC
LIMIT 5;
-- Top 5 products by revenue
SELECT 'Top 5 Products' AS report;
SELECT product_name,
    total_revenue,
    times_sold
FROM product_summary
ORDER BY total_revenue DESC
LIMIT 5;
-- Best performing days (by revenue)
SELECT 'Top 5 Sales Days' AS report;
SELECT sale_date,
    total_revenue,
    total_orders
FROM daily_sales_summary
ORDER BY total_revenue DESC
LIMIT 5;
-- Best performing months (by revenue)
SELECT 'Monthly Performance' AS report;
SELECT month,
    total_revenue,
    mom_growth_pct
FROM monthly_sales_summary
ORDER BY month DESC;
-- ========================================
-- TABLE CREATION STATUS
-- ========================================
-- Status: All 4 tables successfully created and verified
-- Customer Summary: Ready ✓
-- Product Summary: Ready ✓
-- Daily Sales Summary: Ready ✓
-- Monthly Sales Summary: Ready ✓