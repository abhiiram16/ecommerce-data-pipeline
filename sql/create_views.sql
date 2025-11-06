-- ========================================
-- PHASE 4.3: SQL VIEWS FOR ANALYTICS
-- ========================================
-- Purpose: Pre-built queries for instant business intelligence
-- Author: Abhiiram
-- Date: November 6, 2025
-- ========================================
-- VIEW 1: Top Customers (VIP segment with RFM)
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
    price,
    times_sold,
    total_units_sold,
    total_revenue,
    profit_margin_pct,
    unique_customers,
    CASE
        WHEN total_revenue > 1000000 THEN 'Bestseller'
        WHEN total_revenue > 500000 THEN 'Popular'
        ELSE 'Growing'
    END AS product_tier
FROM product_summary
WHERE times_sold > 0
ORDER BY total_revenue DESC;
-- VIEW 3: Customer RFM Segmentation (Complex Analysis)
CREATE OR REPLACE VIEW customer_rfm_segments AS
SELECT cs.customer_id,
    cs.customer_name,
    cs.email,
    cs.total_orders AS frequency,
    cs.days_since_last_order AS recency,
    cs.total_spent AS monetary,
    CASE
        WHEN cs.days_since_last_order <= 30
        AND cs.total_spent > 500000 THEN 'Champions'
        WHEN cs.days_since_last_order <= 30
        AND cs.total_spent > 250000 THEN 'Loyal VIPs'
        WHEN cs.days_since_last_order <= 30 THEN 'Potential Loyalists'
        WHEN cs.days_since_last_order <= 60 THEN 'At Risk - High Value'
        WHEN cs.days_since_last_order <= 90 THEN 'Need Attention'
        WHEN cs.days_since_last_order <= 180 THEN 'At Risk - Low Value'
        ELSE 'Lost'
    END AS rfm_segment,
    CASE
        WHEN cs.total_orders >= 50 THEN 'Very Frequent'
        WHEN cs.total_orders >= 20 THEN 'Frequent'
        WHEN cs.total_orders >= 10 THEN 'Regular'
        WHEN cs.total_orders >= 5 THEN 'Occasional'
        ELSE 'One-time'
    END AS purchase_frequency
FROM customer_summary cs
ORDER BY cs.total_spent DESC;
-- VIEW 4: Daily Sales Trends
CREATE OR REPLACE VIEW sales_trends_daily AS
SELECT sale_date,
    total_orders,
    unique_customers,
    total_revenue,
    avg_order_value,
    total_units,
    LAG(total_revenue) OVER (
        ORDER BY sale_date
    ) AS prev_day_revenue,
    ROUND(
        (
            total_revenue - LAG(total_revenue) OVER (
                ORDER BY sale_date
            )
        ) / NULLIF(
            LAG(total_revenue) OVER (
                ORDER BY sale_date
            ),
            0
        ) * 100,
        2
    ) AS daily_growth_pct,
    CASE
        WHEN EXTRACT(
            DOW
            FROM sale_date
        ) IN (0, 6) THEN 'Weekend'
        ELSE 'Weekday'
    END AS day_type
FROM daily_sales_summary
ORDER BY sale_date DESC;
-- VIEW 5: Monthly Performance with Growth Analysis
CREATE OR REPLACE VIEW monthly_performance AS
SELECT month,
    total_orders,
    unique_customers,
    total_revenue,
    avg_order_value,
    total_units,
    ROUND(total_revenue / NULLIF(unique_customers, 0), 2) AS revenue_per_customer,
    mom_growth_pct,
    CASE
        WHEN mom_growth_pct > 20 THEN 'Excellent Growth'
        WHEN mom_growth_pct > 5 THEN 'Good Growth'
        WHEN mom_growth_pct > 0 THEN 'Slight Growth'
        WHEN mom_growth_pct >= -5 THEN 'Minor Decline'
        ELSE 'Significant Decline'
    END AS growth_status
FROM monthly_sales_summary
ORDER BY month DESC;
-- VIEW 6: Category Performance Analysis
CREATE OR REPLACE VIEW category_performance AS
SELECT p.category,
    COUNT(DISTINCT o.order_id) AS total_orders,
    COUNT(DISTINCT o.customer_id) AS unique_customers,
    SUM(o.total_amount) AS category_revenue,
    ROUND(AVG(o.total_amount), 2) AS avg_order_value,
    SUM(o.quantity) AS total_units,
    COUNT(DISTINCT p.product_id) AS product_count,
    ROUND(
        SUM(o.total_amount) / COUNT(DISTINCT p.product_id),
        2
    ) AS revenue_per_product
FROM products p
    LEFT JOIN orders o ON p.product_id = o.product_id
    AND o.order_status = 'Delivered'
GROUP BY p.category
ORDER BY category_revenue DESC;
-- VIEW 7: Payment Method Analysis
CREATE OR REPLACE VIEW payment_method_analysis AS
SELECT payment_method,
    COUNT(order_id) AS total_transactions,
    SUM(total_amount) AS total_revenue,
    ROUND(AVG(total_amount), 2) AS avg_transaction_value,
    ROUND(
        COUNT(order_id) * 100.0 / (
            SELECT COUNT(*)
            FROM orders
        ),
        2
    ) AS percentage_of_orders,
    COUNT(DISTINCT customer_id) AS unique_customers
FROM orders
WHERE order_status = 'Delivered'
GROUP BY payment_method
ORDER BY total_revenue DESC;
-- VIEW 8: Order Status Distribution
CREATE OR REPLACE VIEW order_status_distribution AS
SELECT order_status,
    COUNT(*) AS order_count,
    ROUND(
        COUNT(*) * 100.0 / (
            SELECT COUNT(*)
            FROM orders
        ),
        2
    ) AS percentage,
    SUM(total_amount) AS revenue_impact,
    COUNT(DISTINCT customer_id) AS affected_customers
FROM orders
GROUP BY order_status
ORDER BY order_count DESC;
-- ========================================
-- GRANT PERMISSIONS
-- ========================================
GRANT SELECT ON top_customers TO dataeng;
GRANT SELECT ON top_products TO dataeng;
GRANT SELECT ON customer_rfm_segments TO dataeng;
GRANT SELECT ON sales_trends_daily TO dataeng;
GRANT SELECT ON monthly_performance TO dataeng;
GRANT SELECT ON category_performance TO dataeng;
GRANT SELECT ON payment_method_analysis TO dataeng;
GRANT SELECT ON order_status_distribution TO dataeng;