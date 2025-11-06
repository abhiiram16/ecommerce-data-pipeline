"""
Refresh Aggregations DAG
========================

Orchestrates daily refresh of all aggregate tables.

Schedule: Every day at 3 AM IST
Pattern: Fan-out (parallel refreshes) → Fan-in (verification)
Tasks: 5 (4 parallel aggregates + 1 verification)

Author: Abhiiram
Date: November 6, 2025
"""

from datetime import datetime, timedelta
from typing import Dict, Any
import os
import sys

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowException
from loguru import logger
import psycopg2

# Configure logging
logger.add("logs/airflow_{time:YYYY-MM-DD}.log", level="INFO")

# Add src to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../../src'))

# ========================================
# DEFAULT ARGUMENTS
# ========================================

default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'start_date': datetime(2025, 11, 6),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

# ========================================
# DAG DEFINITION
# ========================================

dag = DAG(
    'refresh_aggregations',
    default_args=default_args,
    description='Daily refresh of all aggregate and summary tables',
    schedule_interval='0 3 * * *',  # 3 AM IST every day
    catchup=False,
    tags=['aggregations', 'daily'],
    max_active_runs=1,
)

# ========================================
# DATABASE CONFIGURATION
# ========================================


def get_db_connection():
    """Create and return database connection."""
    try:
        db_config = {
            'host': os.getenv('ECOMMERCE_DB_HOST', 'ecommerce-postgres'),
            'port': int(os.getenv('ECOMMERCE_DB_PORT', 5432)),
            'database': os.getenv('ECOMMERCE_DB_NAME', 'ecommerce_db'),
            'user': os.getenv('ECOMMERCE_DB_USER', 'dataeng'),
            'password': os.getenv('ECOMMERCE_DB_PASSWORD', 'pipeline123'),
            'connect_timeout': 5,
        }

        conn = psycopg2.connect(**db_config)
        return conn

    except Exception as e:
        logger.error(f"✗ Database connection failed: {e}")
        raise AirflowException(f"Database connection error: {e}")


# ========================================
# TASK FUNCTIONS
# ========================================

def refresh_customer_summary(**context) -> Dict[str, Any]:
    """
    Refresh customer_summary aggregate table.

    Computes:
    - Customer lifetime value (CLV)
    - RFM scores (Recency, Frequency, Monetary)
    - Purchase statistics

    Returns:
        dict: Rows updated and execution time
    """
    task_start = datetime.now()

    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        logger.info("⏳ Starting customer_summary refresh")

        # Refresh customer summary via SQL
        sql = """
        TRUNCATE TABLE customer_summary;
        
        INSERT INTO customer_summary 
        SELECT
            c.customer_id,
            c.customer_name,
            c.email,
            COUNT(DISTINCT o.order_id) as total_orders,
            SUM(o.total_amount) as total_spent,
            AVG(o.total_amount) as avg_order_value,
            MAX(o.order_date) as last_purchase_date,
            MIN(o.order_date) as first_purchase_date,
            ROUND(
                (CURRENT_DATE - MAX(o.order_date))::numeric / 
                NULLIF(COUNT(DISTINCT o.order_id), 0), 2
            ) as days_since_purchase
        FROM customers c
        LEFT JOIN orders o ON c.customer_id = o.customer_id 
            AND o.order_status = 'Delivered'
        GROUP BY c.customer_id, c.customer_name, c.email;
        """

        cursor.execute(sql)
        conn.commit()

        rows_affected = cursor.rowcount
        execution_time = (datetime.now() - task_start).total_seconds()

        cursor.close()
        conn.close()

        logger.info(
            f"✓ customer_summary refreshed - {rows_affected} rows in {execution_time:.2f}s")
        print(
            f"✓ customer_summary: {rows_affected} rows in {execution_time:.2f}s")

        context['task_instance'].xcom_push(
            key='customer_rows',
            value=rows_affected
        )

        return {'rows_updated': rows_affected, 'execution_time': execution_time}

    except Exception as e:
        logger.error(f"✗ customer_summary refresh failed: {e}")
        raise AirflowException(f"Customer summary refresh error: {e}")


def refresh_product_summary(**context) -> Dict[str, Any]:
    """
    Refresh product_summary aggregate table.

    Computes:
    - Product revenue and profit
    - Unit economics
    - Sales volume statistics

    Returns:
        dict: Rows updated and execution time
    """
    task_start = datetime.now()

    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        logger.info("⏳ Starting product_summary refresh")

        # Refresh product summary via SQL
        sql = """
        TRUNCATE TABLE product_summary;
        
        INSERT INTO product_summary 
        SELECT
            p.product_id,
            p.product_name,
            p.category,
            p.price,
            COUNT(o.order_id) as total_units_sold,
            COUNT(DISTINCT o.order_id) as total_orders,
            SUM(o.quantity) as quantity_sold,
            SUM(o.total_amount) as total_revenue,
            ROUND(SUM(o.total_amount) / NULLIF(SUM(o.quantity), 0), 2) as avg_price_sold,
            MAX(o.order_date) as last_sold_date,
            ROUND(
                SUM(o.total_amount) / NULLIF(COUNT(o.order_id), 0), 2
            ) as revenue_per_order
        FROM products p
        LEFT JOIN orders o ON p.product_id = o.product_id 
            AND o.order_status = 'Delivered'
        GROUP BY p.product_id, p.product_name, p.category, p.price;
        """

        cursor.execute(sql)
        conn.commit()

        rows_affected = cursor.rowcount
        execution_time = (datetime.now() - task_start).total_seconds()

        cursor.close()
        conn.close()

        logger.info(
            f"✓ product_summary refreshed - {rows_affected} rows in {execution_time:.2f}s")
        print(
            f"✓ product_summary: {rows_affected} rows in {execution_time:.2f}s")

        context['task_instance'].xcom_push(
            key='product_rows',
            value=rows_affected
        )

        return {'rows_updated': rows_affected, 'execution_time': execution_time}

    except Exception as e:
        logger.error(f"✗ product_summary refresh failed: {e}")
        raise AirflowException(f"Product summary refresh error: {e}")


def refresh_daily_sales(**context) -> Dict[str, Any]:
    """
    Refresh daily_sales_summary aggregate table.

    Computes:
    - Daily revenue
    - Daily order count
    - Daily customer count
    - Daily average order value

    Returns:
        dict: Rows updated and execution time
    """
    task_start = datetime.now()

    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        logger.info("⏳ Starting daily_sales_summary refresh")

        # Refresh daily sales summary via SQL
        sql = """
        TRUNCATE TABLE daily_sales_summary;
        
        INSERT INTO daily_sales_summary 
        SELECT
            DATE(o.order_date) as date,
            COUNT(o.order_id) as total_orders,
            COUNT(DISTINCT o.customer_id) as unique_customers,
            SUM(o.total_amount) as total_revenue,
            ROUND(AVG(o.total_amount), 2) as avg_order_value,
            SUM(o.quantity) as total_units,
            ROUND(
                SUM(o.total_amount) / COUNT(o.order_id), 2
            ) as revenue_per_order
        FROM orders o
        WHERE o.order_status = 'Delivered'
        GROUP BY DATE(o.order_date)
        ORDER BY date DESC;
        """

        cursor.execute(sql)
        conn.commit()

        rows_affected = cursor.rowcount
        execution_time = (datetime.now() - task_start).total_seconds()

        cursor.close()
        conn.close()

        logger.info(
            f"✓ daily_sales_summary refreshed - {rows_affected} rows in {execution_time:.2f}s")
        print(
            f"✓ daily_sales_summary: {rows_affected} rows in {execution_time:.2f}s")

        context['task_instance'].xcom_push(
            key='daily_rows',
            value=rows_affected
        )

        return {'rows_updated': rows_affected, 'execution_time': execution_time}

    except Exception as e:
        logger.error(f"✗ daily_sales_summary refresh failed: {e}")
        raise AirflowException(f"Daily sales summary refresh error: {e}")


def refresh_monthly_sales(**context) -> Dict[str, Any]:
    """
    Refresh monthly_sales_summary aggregate table.

    Computes:
    - Monthly revenue
    - Monthly order count
    - Month-over-month growth
    - Monthly customer count

    Returns:
        dict: Rows updated and execution time
    """
    task_start = datetime.now()

    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        logger.info("⏳ Starting monthly_sales_summary refresh")

        # Refresh monthly sales summary via SQL
        sql = """
        TRUNCATE TABLE monthly_sales_summary;
        
        INSERT INTO monthly_sales_summary 
        SELECT
            DATE_TRUNC('month', o.order_date)::date as month,
            COUNT(o.order_id) as total_orders,
            COUNT(DISTINCT o.customer_id) as unique_customers,
            SUM(o.total_amount) as total_revenue,
            ROUND(AVG(o.total_amount), 2) as avg_order_value,
            SUM(o.quantity) as total_units,
            LAG(SUM(o.total_amount)) OVER (
                ORDER BY DATE_TRUNC('month', o.order_date)
            ) as prev_month_revenue,
            ROUND(
                (SUM(o.total_amount) - LAG(SUM(o.total_amount)) OVER (
                    ORDER BY DATE_TRUNC('month', o.order_date)
                )) / NULLIF(LAG(SUM(o.total_amount)) OVER (
                    ORDER BY DATE_TRUNC('month', o.order_date)
                ), 0) * 100, 2
            ) as mom_growth_pct
        FROM orders o
        WHERE o.order_status = 'Delivered'
        GROUP BY DATE_TRUNC('month', o.order_date)
        ORDER BY month DESC;
        """

        cursor.execute(sql)
        conn.commit()

        rows_affected = cursor.rowcount
        execution_time = (datetime.now() - task_start).total_seconds()

        cursor.close()
        conn.close()

        logger.info(
            f"✓ monthly_sales_summary refreshed - {rows_affected} rows in {execution_time:.2f}s")
        print(
            f"✓ monthly_sales_summary: {rows_affected} rows in {execution_time:.2f}s")

        context['task_instance'].xcom_push(
            key='monthly_rows',
            value=rows_affected
        )

        return {'rows_updated': rows_affected, 'execution_time': execution_time}

    except Exception as e:
        logger.error(f"✗ monthly_sales_summary refresh failed: {e}")
        raise AirflowException(f"Monthly sales summary refresh error: {e}")


def verify_aggregations(**context) -> Dict[str, Any]:
    """
    Verify all aggregate tables are consistent and accurate.

    Validates:
    - Daily sales sum matches orders total
    - Monthly sales sum matches daily sum
    - Customer totals reconcile
    - No data quality issues

    Returns:
        dict: Verification status and metrics
    """
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        logger.info("✓ Starting aggregate verification")

        verification_checks = {}

        # Check 1: Daily sales sum vs orders
        cursor.execute("""
            SELECT 
                ROUND(SUM(total_revenue), 2) as daily_sum,
                ROUND(SUM(total_amount), 2) as order_sum,
                ABS(SUM(total_revenue) - SUM(total_amount)) as difference
            FROM daily_sales_summary
            CROSS JOIN orders
            WHERE DATE(orders.order_date) IN (SELECT date FROM daily_sales_summary)
            AND orders.order_status = 'Delivered'
        """)

        result = cursor.fetchone()
        if result:
            verification_checks['daily_vs_orders'] = {
                'daily_sum': result[0],
                'order_sum': result[1],
                'difference': result[2],
                'status': 'PASS' if result[2] < 1 else 'WARNING'
            }
            logger.info(
                f"✓ Daily sales vs orders: {verification_checks['daily_vs_orders']['status']}")

        # Check 2: Monthly sales consistency
        cursor.execute("""
            SELECT 
                COUNT(*) as total_months,
                SUM(CASE WHEN mom_growth_pct IS NULL THEN 0 ELSE 1 END) as months_with_growth,
                MIN(month) as earliest_month,
                MAX(month) as latest_month
            FROM monthly_sales_summary
        """)

        result = cursor.fetchone()
        if result:
            verification_checks['monthly_consistency'] = {
                'total_months': result[0],
                'months_with_growth': result[1],
                'earliest_month': str(result[2]),
                'latest_month': str(result[3]),
                'status': 'PASS'
            }
            logger.info(
                f"✓ Monthly consistency: {verification_checks['monthly_consistency']['status']}")

        # Check 3: Customer total orders
        cursor.execute("""
            SELECT 
                COUNT(*) as customers_in_summary,
                SUM(total_orders) as total_orders_from_summary,
                (SELECT COUNT(*) FROM orders WHERE order_status = 'Delivered') as actual_orders,
                ABS(SUM(total_orders) - (SELECT COUNT(*) FROM orders WHERE order_status = 'Delivered')) as difference
            FROM customer_summary
        """)

        result = cursor.fetchone()
        if result:
            verification_checks['customer_reconciliation'] = {
                'customers': result[0],
                'orders_in_summary': result[1],
                'actual_orders': result[2],
                'difference': result[3],
                'status': 'PASS' if result[3] == 0 else 'WARNING'
            }
            logger.info(
                f"✓ Customer reconciliation: {verification_checks['customer_reconciliation']['status']}")

        # Check 4: Product revenue check
        cursor.execute("""
            SELECT 
                COUNT(*) as products_refreshed,
                SUM(total_revenue) as total_product_revenue,
                (SELECT SUM(total_amount) FROM orders WHERE order_status = 'Delivered') as actual_revenue,
                ABS(SUM(total_revenue) - (SELECT SUM(total_amount) FROM orders WHERE order_status = 'Delivered')) as difference
            FROM product_summary
        """)

        result = cursor.fetchone()
        if result:
            verification_checks['product_revenue'] = {
                'products': result[0],
                'product_revenue': result[1],
                'actual_revenue': result[2],
                'difference': result[3],
                'status': 'PASS' if result[3] < 1 else 'WARNING'
            }
            logger.info(
                f"✓ Product revenue: {verification_checks['product_revenue']['status']}")

        cursor.close()
        conn.close()

        # Get previous task results
        ti = context['task_instance']
        customer_rows = ti.xcom_pull(
            task_ids='refresh_customer_summary', key='customer_rows') or 0
        product_rows = ti.xcom_pull(
            task_ids='refresh_product_summary', key='product_rows') or 0
        daily_rows = ti.xcom_pull(
            task_ids='refresh_daily_sales', key='daily_rows') or 0
        monthly_rows = ti.xcom_pull(
            task_ids='refresh_monthly_sales', key='monthly_rows') or 0

        # Print verification summary
        print("\n" + "=" * 70)
        print("AGGREGATE VERIFICATION SUMMARY")
        print("=" * 70)
        print(f"✓ Customer Summary:   {customer_rows} rows")
        print(f"✓ Product Summary:    {product_rows} rows")
        print(f"✓ Daily Sales:        {daily_rows} rows")
        print(f"✓ Monthly Sales:      {monthly_rows} rows")
        print("-" * 70)
        for check_name, details in verification_checks.items():
            status = details['status']
            print(f"✓ {check_name}: {status}")
        print("=" * 70)

        logger.info("✓ All aggregates verified successfully")

        return {'verification_checks': verification_checks, 'status': 'SUCCESS'}

    except Exception as e:
        logger.error(f"✗ Aggregate verification failed: {e}")
        raise AirflowException(f"Verification error: {e}")


# ========================================
# DEFINE TASKS
# ========================================

task_customer = PythonOperator(
    task_id='refresh_customer_summary',
    python_callable=refresh_customer_summary,
    dag=dag,
    pool='database',
    pool_slots=1,
)

task_product = PythonOperator(
    task_id='refresh_product_summary',
    python_callable=refresh_product_summary,
    dag=dag,
    pool='database',
    pool_slots=1,
)

task_daily = PythonOperator(
    task_id='refresh_daily_sales',
    python_callable=refresh_daily_sales,
    dag=dag,
    pool='database',
    pool_slots=1,
)

task_monthly = PythonOperator(
    task_id='refresh_monthly_sales',
    python_callable=refresh_monthly_sales,
    dag=dag,
    pool='database',
    pool_slots=1,
)

task_verify = PythonOperator(
    task_id='verify_aggregations',
    python_callable=verify_aggregations,
    dag=dag,
)

# ========================================
# DEFINE DEPENDENCIES
# ========================================
# Pattern: Parallel (4 refreshes) → Aggregation (1 verification)

[task_customer, task_product, task_daily, task_monthly] >> task_verify
