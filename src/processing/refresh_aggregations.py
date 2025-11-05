"""
Aggregate Table Refresh Script
================================
Refreshes all materialized aggregate tables with latest transactional data.

This script should be run periodically (daily, hourly, etc.) to keep analytics
tables up-to-date with the latest orders, customers, and product data.

Usage:
    python refresh_aggregations.py

Author: Abhiiram
Date: November 5, 2025
"""

import psycopg2
from psycopg2 import sql
from datetime import datetime
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Database connection parameters
DB_CONFIG = {
    'host': 'localhost',
    'port': 5432,
    'database': 'ecommerce_db',
    'user': 'dataeng',
    'password': 'pipeline123'
}

# SQL for refreshing aggregate tables
SQL_QUERIES = {
    'customer_summary': """
        DROP TABLE IF EXISTS customer_summary CASCADE;
        
        CREATE TABLE customer_summary AS
        SELECT 
            c.customer_id,
            c.first_name || ' ' || c.last_name AS customer_name,
            c.email,
            c.city,
            c.state,
            COUNT(o.order_id) AS total_orders,
            COALESCE(SUM(o.total_amount), 0) AS total_spent,
            COALESCE(AVG(o.total_amount), 0) AS avg_order_value,
            MIN(o.order_date) AS first_order_date,
            MAX(o.order_date) AS last_order_date,
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
        WHERE o.order_status = 'Delivered'
        GROUP BY c.customer_id, c.first_name, c.last_name, c.email, c.city, c.state
        ORDER BY total_spent DESC;
        
        CREATE INDEX idx_customer_summary_total_spent ON customer_summary(total_spent DESC);
        CREATE INDEX idx_customer_summary_status ON customer_summary(customer_status);
        CREATE INDEX idx_customer_summary_city ON customer_summary(city);
    """,

    'product_summary': """
        DROP TABLE IF EXISTS product_summary CASCADE;
        
        CREATE TABLE product_summary AS
        SELECT 
            p.product_id,
            p.product_name,
            p.category,
            p.subcategory,
            p.brand,
            p.price,
            p.cost,
            COUNT(DISTINCT o.order_id) AS times_sold,
            SUM(o.quantity) AS total_units_sold,
            ROUND(SUM(o.total_amount), 2) AS total_revenue,
            ROUND(SUM(o.quantity * (p.price - p.cost)), 2) AS total_profit,
            ROUND(AVG(o.quantity), 2) AS avg_quantity_per_order,
            ROUND(AVG(o.total_amount), 2) AS avg_order_value,
            MAX(o.order_date) AS last_sold_date,
            MIN(o.order_date) AS first_sold_date,
            COUNT(DISTINCT o.customer_id) AS unique_customers,
            ROUND(((SUM(o.total_amount) - SUM(o.quantity * p.cost)) / NULLIF(SUM(o.total_amount), 0) * 100), 2) AS profit_margin_pct,
            CURRENT_TIMESTAMP AS last_updated
        FROM products p
        LEFT JOIN orders o ON p.product_id = o.product_id
        WHERE o.order_status = 'Delivered' OR o.order_id IS NULL
        GROUP BY p.product_id, p.product_name, p.category, p.subcategory, p.brand, p.price, p.cost
        ORDER BY total_revenue DESC;
        
        CREATE INDEX idx_product_summary_revenue ON product_summary(total_revenue DESC);
        CREATE INDEX idx_product_summary_category ON product_summary(category);
    """,

    'daily_sales_summary': """
        DROP TABLE IF EXISTS daily_sales_summary CASCADE;
        
        CREATE TABLE daily_sales_summary AS
        SELECT 
            DATE(o.order_date) AS sale_date,
            COUNT(DISTINCT o.order_id) AS total_orders,
            COUNT(DISTINCT o.customer_id) AS unique_customers,
            SUM(o.quantity) AS total_units_sold,
            SUM(o.total_amount) AS total_revenue,
            ROUND(AVG(o.total_amount), 2) AS avg_order_value,
            MIN(o.total_amount) AS min_order_value,
            MAX(o.total_amount) AS max_order_value,
            COUNT(DISTINCT o.product_id) AS products_sold,
            CURRENT_TIMESTAMP AS last_updated
        FROM orders o
        WHERE o.order_status = 'Delivered'
        GROUP BY DATE(o.order_date)
        ORDER BY sale_date DESC;
        
        CREATE INDEX idx_daily_sales_date ON daily_sales_summary(sale_date DESC);
    """,

    'monthly_sales_summary': """
        DROP TABLE IF EXISTS monthly_sales_summary CASCADE;
        
        CREATE TABLE monthly_sales_summary AS
        SELECT 
            DATE_TRUNC('month', o.order_date)::DATE AS month_start,
            TO_CHAR(DATE_TRUNC('month', o.order_date), 'YYYY-MM') AS month,
            COUNT(DISTINCT o.order_id) AS total_orders,
            COUNT(DISTINCT o.customer_id) AS unique_customers,
            SUM(o.quantity) AS total_units_sold,
            ROUND(SUM(o.total_amount), 2) AS total_revenue,
            ROUND(AVG(o.total_amount), 2) AS avg_order_value,
            COUNT(DISTINCT o.product_id) AS products_sold,
            ROUND(
                (SUM(o.total_amount) - LAG(SUM(o.total_amount)) 
                 OVER (ORDER BY DATE_TRUNC('month', o.order_date))) 
                / NULLIF(LAG(SUM(o.total_amount)) OVER (ORDER BY DATE_TRUNC('month', o.order_date)), 0) * 100, 2
            ) AS revenue_growth_pct,
            CURRENT_TIMESTAMP AS last_updated
        FROM orders o
        WHERE o.order_status = 'Delivered'
        GROUP BY DATE_TRUNC('month', o.order_date)
        ORDER BY month_start DESC;
        
        CREATE INDEX idx_monthly_sales_month ON monthly_sales_summary(month_start DESC);
    """
}


def connect_to_database():
    """Establish connection to PostgreSQL database."""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        logger.info("Successfully connected to database")
        return conn
    except Exception as e:
        logger.error(f"Failed to connect to database: {e}")
        raise


def refresh_table(conn, table_name, query):
    """
    Refresh a single aggregate table.

    Args:
        conn: Database connection object
        table_name: Name of the table to refresh
        query: SQL query to execute

    Returns:
        bool: True if successful, False otherwise
    """
    try:
        start_time = datetime.now()
        logger.info(f"Starting refresh of {table_name}...")

        cursor = conn.cursor()
        cursor.execute(query)
        conn.commit()

        # Get row count
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        row_count = cursor.fetchone()[0]

        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()

        logger.info(
            f"✓ {table_name} refreshed successfully: {row_count:,} rows in {duration:.2f}s")
        cursor.close()
        return True

    except Exception as e:
        logger.error(f"✗ Failed to refresh {table_name}: {e}")
        conn.rollback()
        return False


def refresh_all_aggregates():
    """Main function to refresh all aggregate tables."""
    logger.info("=" * 60)
    logger.info("STARTING AGGREGATE TABLE REFRESH")
    logger.info("=" * 60)

    start_time = datetime.now()
    conn = None
    success_count = 0
    fail_count = 0

    try:
        conn = connect_to_database()

        for table_name, query in SQL_QUERIES.items():
            if refresh_table(conn, table_name, query):
                success_count += 1
            else:
                fail_count += 1

        end_time = datetime.now()
        total_duration = (end_time - start_time).total_seconds()

        logger.info("=" * 60)
        logger.info("REFRESH SUMMARY")
        logger.info(f"Total tables: {len(SQL_QUERIES)}")
        logger.info(f"✓ Successful: {success_count}")
        logger.info(f"✗ Failed: {fail_count}")
        logger.info(f"Total duration: {total_duration:.2f}s")
        logger.info("=" * 60)

        return fail_count == 0

    except Exception as e:
        logger.error(f"Critical error during refresh: {e}")
        return False

    finally:
        if conn:
            conn.close()
            logger.info("Database connection closed")


if __name__ == "__main__":
    success = refresh_all_aggregates()
    exit(0 if success else 1)
