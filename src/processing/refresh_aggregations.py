"""
Refresh Aggregation Tables (Simple Truncate Mode)
==================================================

Refresh all aggregate tables with cumulative data.
Aggregates are recalculated fresh from base tables.

Author: Abhiiram
Date: November 7, 2025
"""

from src.utils.db_connector import get_connection
from src.utils.config import Config
import psycopg2
import sys
import os
from datetime import datetime
from loguru import logger

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../../'))


logger.add(
    f"{Config.LOGS_DIR}/aggregations_{{time:YYYY-MM-DD}}.log",
    level=Config.LOG_LEVEL
)


def refresh_customer_summary():
    """Refresh customer summary - recalculate from base tables."""

    logger.info("⏳ Refreshing customer_summary...")
    print("⏳ Refreshing customer_summary...")

    try:
        conn = get_connection()
        cursor = conn.cursor()

        # TRUNCATE - clear old aggregates
        cursor.execute("DELETE FROM customer_summary")

        # INSERT fresh calculations from ALL base data
        query = """
        INSERT INTO customer_summary 
        SELECT 
            c.customer_id,
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
        GROUP BY c.customer_id, c.first_name, c.last_name, c.email, c.city, c.state
        ORDER BY total_spent DESC
        """

        cursor.execute(query)
        conn.commit()

        count = cursor.rowcount
        logger.info(f"✓ Refreshed customer_summary: {count} records")
        print(f"✓ Refreshed customer_summary: {count} records")

        cursor.close()
        conn.close()

    except Exception as e:
        logger.error(f"✗ customer_summary failed: {e}")
        raise


def refresh_product_summary():
    """Refresh product summary - recalculate from base tables."""

    logger.info("⏳ Refreshing product_summary...")
    print("⏳ Refreshing product_summary...")

    try:
        conn = get_connection()
        cursor = conn.cursor()

        cursor.execute("DELETE FROM product_summary")

        query = """
        INSERT INTO product_summary 
        SELECT 
            p.product_id,
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
                ((p.price - p.cost) / NULLIF(p.price, 0)) * 100, 2
            ) AS profit_margin_pct,
            CURRENT_TIMESTAMP AS last_updated
        FROM products p
        LEFT JOIN orders o ON p.product_id = o.product_id 
            AND o.order_status = 'Delivered'
        GROUP BY p.product_id, p.product_name, p.category, p.brand, p.price, p.cost
        ORDER BY total_revenue DESC
        """

        cursor.execute(query)
        conn.commit()

        count = cursor.rowcount
        logger.info(f"✓ Refreshed product_summary: {count} records")
        print(f"✓ Refreshed product_summary: {count} records")

        cursor.close()
        conn.close()

    except Exception as e:
        logger.error(f"✗ product_summary failed: {e}")
        raise


def refresh_daily_sales_summary():
    """Refresh daily sales summary - recalculate from base tables."""

    logger.info("⏳ Refreshing daily_sales_summary...")
    print("⏳ Refreshing daily_sales_summary...")

    try:
        conn = get_connection()
        cursor = conn.cursor()

        cursor.execute("DELETE FROM daily_sales_summary")

        query = """
        INSERT INTO daily_sales_summary 
        SELECT
            DATE(o.order_date) AS sale_date,
            COUNT(o.order_id) AS total_orders,
            COUNT(DISTINCT o.customer_id) AS unique_customers,
            COALESCE(SUM(o.total_amount), 0) AS total_revenue,
            ROUND(COALESCE(AVG(o.total_amount), 0), 2) AS avg_order_value,
            COALESCE(SUM(o.quantity), 0) AS total_units,
            CURRENT_TIMESTAMP AS last_updated
        FROM orders o
        WHERE o.order_status = 'Delivered'
        GROUP BY DATE(o.order_date)
        ORDER BY sale_date DESC
        """

        cursor.execute(query)
        conn.commit()

        count = cursor.rowcount
        logger.info(f"✓ Refreshed daily_sales_summary: {count} records")
        print(f"✓ Refreshed daily_sales_summary: {count} records")

        cursor.close()
        conn.close()

    except Exception as e:
        logger.error(f"✗ daily_sales_summary failed: {e}")
        raise


def refresh_monthly_sales_summary():
    """Refresh monthly sales summary - recalculate from base tables."""

    logger.info("⏳ Refreshing monthly_sales_summary...")
    print("⏳ Refreshing monthly_sales_summary...")

    try:
        conn = get_connection()
        cursor = conn.cursor()

        cursor.execute("DELETE FROM monthly_sales_summary")

        query = """
        INSERT INTO monthly_sales_summary 
        SELECT
            DATE_TRUNC('month', o.order_date)::DATE AS month,
            COUNT(o.order_id) AS total_orders,
            COUNT(DISTINCT o.customer_id) AS unique_customers,
            COALESCE(SUM(o.total_amount), 0) AS total_revenue,
            ROUND(COALESCE(AVG(o.total_amount), 0), 2) AS avg_order_value,
            COALESCE(SUM(o.quantity), 0) AS total_units,
            LAG(SUM(o.total_amount)) OVER (
                ORDER BY DATE_TRUNC('month', o.order_date)
            ) AS prev_month_revenue,
            ROUND(
                (SUM(o.total_amount) - LAG(SUM(o.total_amount)) OVER (
                    ORDER BY DATE_TRUNC('month', o.order_date)
                )) / NULLIF(LAG(SUM(o.total_amount)) OVER (
                    ORDER BY DATE_TRUNC('month', o.order_date)
                ), 0) * 100, 2
            ) AS mom_growth_pct,
            CURRENT_TIMESTAMP AS last_updated
        FROM orders o
        WHERE o.order_status = 'Delivered'
        GROUP BY DATE_TRUNC('month', o.order_date)
        ORDER BY month DESC
        """

        cursor.execute(query)
        conn.commit()

        count = cursor.rowcount
        logger.info(f"✓ Refreshed monthly_sales_summary: {count} records")
        print(f"✓ Refreshed monthly_sales_summary: {count} records")

        cursor.close()
        conn.close()

    except Exception as e:
        logger.error(f"✗ monthly_sales_summary failed: {e}")
        raise


def main():
    """Refresh all aggregation tables."""

    logger.info("=" * 60)
    logger.info("REFRESH AGGREGATIONS")
    logger.info(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("=" * 60)

    print("=" * 60)
    print("REFRESH AGGREGATIONS")
    print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)

    try:
        refresh_customer_summary()
        refresh_product_summary()
        refresh_daily_sales_summary()
        refresh_monthly_sales_summary()

        logger.info("=" * 60)
        logger.info("✓ ALL AGGREGATIONS REFRESHED SUCCESSFULLY")
        logger.info(f"Recalculated from ALL cumulative base data")
        logger.info("=" * 60)

        print("=" * 60)
        print("✓ ALL AGGREGATIONS REFRESHED SUCCESSFULLY")
        print(f"Recalculated from ALL cumulative base data")
        print("=" * 60)

    except Exception as e:
        logger.error(f"✗ Refresh failed: {e}")
        print(f"✗ Refresh failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    Config.create_directories()
    main()
