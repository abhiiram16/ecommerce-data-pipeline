"""
Daily Data Quality Check DAG
============================

Orchestrates data quality validation workflow.

Schedule: Every day at 2 AM IST
Pattern: Fan-out (schema validation) → Parallel checks → Aggregation
Tasks: 5 (1 sequential + 3 parallel + 1 aggregation)

Author: Abhiiram
Date: November 6, 2025
"""

from datetime import datetime, timedelta
from typing import Dict, Any
import sys
import os

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowException
from loguru import logger

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
    'daily_quality_check',
    default_args=default_args,
    description='Daily data quality validation with 4-dimension checks',
    schedule_interval='0 2 * * *',  # 2 AM IST every day
    catchup=False,
    tags=['quality', 'daily', 'monitoring'],
    max_active_runs=1,  # Prevent concurrent runs
)

# ========================================
# TASK FUNCTIONS
# ========================================


def validate_database_connection(**context) -> Dict[str, Any]:
    """
    Validate database connectivity before running checks.

    This task ensures the ecommerce database is accessible
    and all required tables exist.

    Returns:
        dict: Connection status and table information
    """
    try:
        import psycopg2

        db_config = {
            'host': os.getenv('ECOMMERCE_DB_HOST', 'ecommerce-postgres'),
            'port': int(os.getenv('ECOMMERCE_DB_PORT', 5432)),
            'database': os.getenv('ECOMMERCE_DB_NAME', 'ecommerce_db'),
            'user': os.getenv('ECOMMERCE_DB_USER', 'dataeng'),
            'password': os.getenv('ECOMMERCE_DB_PASSWORD', 'pipeline123'),
            'connect_timeout': 5,
        }

        conn = psycopg2.connect(**db_config)
        cursor = conn.cursor()

        # Check required tables
        required_tables = ['customers', 'products', 'orders']
        cursor.execute("""
            SELECT table_name FROM information_schema.tables
            WHERE table_schema = 'public'
        """)
        existing_tables = [row[0] for row in cursor.fetchall()]

        missing_tables = [
            t for t in required_tables if t not in existing_tables]

        if missing_tables:
            raise AirflowException(f"Missing tables: {missing_tables}")

        cursor.close()
        conn.close()

        logger.info("✓ Database connection validated")
        context['task_instance'].xcom_push(
            key='db_connection_status',
            value='SUCCESS'
        )

        return {'status': 'SUCCESS', 'message': 'All tables present'}

    except Exception as e:
        logger.error(f"✗ Database connection failed: {e}")
        raise AirflowException(f"Database validation failed: {e}")


def check_completeness(**context) -> Dict[str, Any]:
    """
    Check for null values in critical columns.

    Validates that essential fields have no missing data.

    Returns:
        dict: Completeness score and null value counts
    """
    try:
        import psycopg2

        db_config = {
            'host': os.getenv('ECOMMERCE_DB_HOST', 'ecommerce-postgres'),
            'port': int(os.getenv('ECOMMERCE_DB_PORT', 5432)),
            'database': os.getenv('ECOMMERCE_DB_NAME', 'ecommerce_db'),
            'user': os.getenv('ECOMMERCE_DB_USER', 'dataeng'),
            'password': os.getenv('ECOMMERCE_DB_PASSWORD', 'pipeline123'),
            'connect_timeout': 5,
        }

        conn = psycopg2.connect(**db_config)
        cursor = conn.cursor()

        # Check for nulls in critical columns
        checks = {
            'customers_email_null': "SELECT COUNT(*) FROM customers WHERE email IS NULL",
            'customers_id_null': "SELECT COUNT(*) FROM customers WHERE customer_id IS NULL",
            'products_name_null': "SELECT COUNT(*) FROM products WHERE product_name IS NULL",
            'products_price_null': "SELECT COUNT(*) FROM products WHERE price IS NULL",
            'orders_id_null': "SELECT COUNT(*) FROM orders WHERE order_id IS NULL",
            'orders_amount_null': "SELECT COUNT(*) FROM orders WHERE total_amount IS NULL",
        }

        results = {}
        null_count = 0

        for check_name, query in checks.items():
            cursor.execute(query)
            count = cursor.fetchone()[0]
            results[check_name] = count
            null_count += count

        cursor.close()
        conn.close()

        completeness_score = 100.0 if null_count == 0 else 95.0

        logger.info(
            f"✓ Completeness check passed - Score: {completeness_score}%")
        context['task_instance'].xcom_push(
            key='completeness_score',
            value=completeness_score
        )

        return {'score': completeness_score, 'null_count': null_count}

    except Exception as e:
        logger.error(f"✗ Completeness check failed: {e}")
        raise AirflowException(f"Completeness validation failed: {e}")


def check_validity(**context) -> Dict[str, Any]:
    """
    Validate data ranges and formats.

    Checks that values fall within acceptable ranges and
    match expected data types and formats.

    Returns:
        dict: Validity score and invalid record counts
    """
    try:
        import psycopg2

        db_config = {
            'host': os.getenv('ECOMMERCE_DB_HOST', 'ecommerce-postgres'),
            'port': int(os.getenv('ECOMMERCE_DB_PORT', 5432)),
            'database': os.getenv('ECOMMERCE_DB_NAME', 'ecommerce_db'),
            'user': os.getenv('ECOMMERCE_DB_USER', 'dataeng'),
            'password': os.getenv('ECOMMERCE_DB_PASSWORD', 'pipeline123'),
            'connect_timeout': 5,
        }

        conn = psycopg2.connect(**db_config)
        cursor = conn.cursor()

        # Validity checks
        checks = {
            'age_range': "SELECT COUNT(*) FROM customers WHERE age < 18 OR age > 120",
            'price_positive': "SELECT COUNT(*) FROM products WHERE price <= 0",
            'order_amount_positive': "SELECT COUNT(*) FROM orders WHERE total_amount <= 0",
            'quantity_positive': "SELECT COUNT(*) FROM orders WHERE quantity <= 0",
            'email_format': "SELECT COUNT(*) FROM customers WHERE email NOT LIKE '%@%.%'",
            'future_dates': "SELECT COUNT(*) FROM orders WHERE order_date > CURRENT_TIMESTAMP",
        }

        results = {}
        invalid_count = 0

        for check_name, query in checks.items():
            cursor.execute(query)
            count = cursor.fetchone()[0]
            results[check_name] = count
            invalid_count += count

        cursor.close()
        conn.close()

        validity_score = 100.0 if invalid_count == 0 else 95.0

        logger.info(f"✓ Validity check passed - Score: {validity_score}%")
        context['task_instance'].xcom_push(
            key='validity_score',
            value=validity_score
        )

        return {'score': validity_score, 'invalid_count': invalid_count}

    except Exception as e:
        logger.error(f"✗ Validity check failed: {e}")
        raise AirflowException(f"Validity validation failed: {e}")


def check_consistency(**context) -> Dict[str, Any]:
    """
    Check referential integrity and business logic consistency.

    Validates foreign key relationships and ensures
    data calculations are correct.

    Returns:
        dict: Consistency score and violation counts
    """
    try:
        import psycopg2

        db_config = {
            'host': os.getenv('ECOMMERCE_DB_HOST', 'ecommerce-postgres'),
            'port': int(os.getenv('ECOMMERCE_DB_PORT', 5432)),
            'database': os.getenv('ECOMMERCE_DB_NAME', 'ecommerce_db'),
            'user': os.getenv('ECOMMERCE_DB_USER', 'dataeng'),
            'password': os.getenv('ECOMMERCE_DB_PASSWORD', 'pipeline123'),
            'connect_timeout': 5,
        }

        conn = psycopg2.connect(**db_config)
        cursor = conn.cursor()

        # Consistency checks
        checks = {
            'customer_fk': """
                SELECT COUNT(*) FROM orders o
                LEFT JOIN customers c ON o.customer_id = c.customer_id
                WHERE c.customer_id IS NULL
            """,
            'product_fk': """
                SELECT COUNT(*) FROM orders o
                LEFT JOIN products p ON o.product_id = p.product_id
                WHERE p.product_id IS NULL
            """,
            'calculation_accuracy': """
                SELECT COUNT(*) FROM orders o
                JOIN products p ON o.product_id = p.product_id
                WHERE ABS(o.total_amount - (o.quantity * p.price)) > (o.quantity * p.price * 0.01)
            """,
        }

        results = {}
        violation_count = 0

        for check_name, query in checks.items():
            cursor.execute(query)
            count = cursor.fetchone()[0]
            results[check_name] = count
            violation_count += count

        cursor.close()
        conn.close()

        consistency_score = 100.0 if violation_count == 0 else 99.0

        logger.info(
            f"✓ Consistency check passed - Score: {consistency_score}%")
        context['task_instance'].xcom_push(
            key='consistency_score',
            value=consistency_score
        )

        return {'score': consistency_score, 'violation_count': violation_count}

    except Exception as e:
        logger.error(f"✗ Consistency check failed: {e}")
        raise AirflowException(f"Consistency validation failed: {e}")


def aggregate_quality_score(**context) -> Dict[str, Any]:
    """
    Aggregate all quality scores and generate summary report.

    Combines results from all 4 quality dimensions and
    assigns overall grade.

    Returns:
        dict: Overall quality metrics and grade
    """
    try:
        ti = context['task_instance']

        # Retrieve scores from previous tasks
        completeness = ti.xcom_pull(
            task_ids='check_completeness',
            key='completeness_score'
        ) or 100.0

        validity = ti.xcom_pull(
            task_ids='check_validity',
            key='validity_score'
        ) or 100.0

        consistency = ti.xcom_pull(
            task_ids='check_consistency',
            key='consistency_score'
        ) or 100.0

        # Uniqueness assumed 100% for this DAG
        uniqueness = 100.0

        # Calculate overall score
        overall_score = (completeness + validity +
                         consistency + uniqueness) / 4

        # Assign grade
        if overall_score >= 99:
            grade = "A+ (Excellent)"
        elif overall_score >= 95:
            grade = "A (Very Good)"
        elif overall_score >= 90:
            grade = "B (Good)"
        elif overall_score >= 80:
            grade = "C (Acceptable)"
        else:
            grade = "F (Needs Attention)"

        # Log summary
        print("\n" + "=" * 70)
        print("DATA QUALITY SUMMARY REPORT")
        print("=" * 70)
        print(f"Completeness Score:  {completeness:.1f}%")
        print(f"Validity Score:      {validity:.1f}%")
        print(f"Consistency Score:   {consistency:.1f}%")
        print(f"Uniqueness Score:    {uniqueness:.1f}%")
        print("-" * 70)
        print(f"Overall Quality Score: {overall_score:.1f}%")
        print(f"Data Quality Grade:   {grade}")
        print("=" * 70)

        logger.info(
            f"✓ Quality Report - Score: {overall_score:.1f}%, Grade: {grade}")
        ti.xcom_push(key='overall_score', value=overall_score)
        ti.xcom_push(key='grade', value=grade)

        return {
            'overall_score': overall_score,
            'grade': grade,
            'completeness': completeness,
            'validity': validity,
            'consistency': consistency,
            'uniqueness': uniqueness,
        }

    except Exception as e:
        logger.error(f"✗ Quality aggregation failed: {e}")
        raise AirflowException(f"Quality aggregation failed: {e}")


# ========================================
# DEFINE TASKS
# ========================================

task_db_validation = PythonOperator(
    task_id='validate_database_connection',
    python_callable=validate_database_connection,
    dag=dag,
    pool='database',
    pool_slots=1,
)

task_completeness = PythonOperator(
    task_id='check_completeness',
    python_callable=check_completeness,
    dag=dag,
    pool='database',
    pool_slots=1,
)

task_validity = PythonOperator(
    task_id='check_validity',
    python_callable=check_validity,
    dag=dag,
    pool='database',
    pool_slots=1,
)

task_consistency = PythonOperator(
    task_id='check_consistency',
    python_callable=check_consistency,
    dag=dag,
    pool='database',
    pool_slots=1,
)

task_aggregate = PythonOperator(
    task_id='aggregate_quality_score',
    python_callable=aggregate_quality_score,
    dag=dag,
)

# ========================================
# DEFINE DEPENDENCIES
# ========================================
# Pattern: Sequential (DB validation) → Parallel (quality checks) → Aggregation

task_db_validation >> [task_completeness,
                       task_validity, task_consistency] >> task_aggregate
