"""
Data Validation
===============

Quick validation checks for data integrity.

Validates:
- Required fields
- Data types
- Ranges
- Foreign keys

Author: Abhiiram
Date: November 6, 2025
"""

from src.utils.db_connector import get_connection
from src.utils.config import Config
import sys
import os
from datetime import datetime
from loguru import logger

# Add parent directory to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../../'))


# Configure logging
logger.add(
    f"{Config.LOGS_DIR}/validation_{{time:YYYY-MM-DD}}.log",
    level=Config.LOG_LEVEL
)

# ========================================
# VALIDATION FUNCTIONS
# ========================================


def validate_required_fields() -> bool:
    """Validate that all required fields exist and are populated."""

    logger.info("🔍 Validating required fields...")
    print("🔍 Validating required fields...")

    try:
        conn = get_connection()
        cursor = conn.cursor()

        all_valid = True

        # Check customers
        cursor.execute(
            "SELECT COUNT(*) FROM customers WHERE customer_id IS NULL")
        if cursor.fetchone()[0] > 0:
            logger.warning("⚠️ Found NULL customer_id values")
            all_valid = False

        cursor.execute("SELECT COUNT(*) FROM customers WHERE email IS NULL")
        if cursor.fetchone()[0] > 0:
            logger.warning("⚠️ Found NULL email values")
            all_valid = False

        # Check products
        cursor.execute(
            "SELECT COUNT(*) FROM products WHERE product_id IS NULL")
        if cursor.fetchone()[0] > 0:
            logger.warning("⚠️ Found NULL product_id values")
            all_valid = False

        cursor.execute("SELECT COUNT(*) FROM products WHERE price IS NULL")
        if cursor.fetchone()[0] > 0:
            logger.warning("⚠️ Found NULL price values")
            all_valid = False

        # Check orders
        cursor.execute("SELECT COUNT(*) FROM orders WHERE order_id IS NULL")
        if cursor.fetchone()[0] > 0:
            logger.warning("⚠️ Found NULL order_id values")
            all_valid = False

        cursor.execute(
            "SELECT COUNT(*) FROM orders WHERE total_amount IS NULL")
        if cursor.fetchone()[0] > 0:
            logger.warning("⚠️ Found NULL total_amount values")
            all_valid = False

        cursor.close()
        conn.close()

        if all_valid:
            logger.info("✓ All required fields are populated")
            print("✓ All required fields are populated")

        return all_valid

    except Exception as e:
        logger.error(f"✗ Required field validation failed: {e}")
        raise


def validate_data_types() -> bool:
    """Validate data types in tables."""

    logger.info("📝 Validating data types...")
    print("📝 Validating data types...")

    try:
        conn = get_connection()
        cursor = conn.cursor()

        all_valid = True

        # Check customer ages (should be numeric and within range)
        cursor.execute(
            "SELECT COUNT(*) FROM customers WHERE age < 18 OR age > 120")
        invalid_ages = cursor.fetchone()[0]

        if invalid_ages > 0:
            logger.warning(f"⚠️ Found {invalid_ages} invalid age values")
            all_valid = False

        # Check prices (should be numeric and positive)
        cursor.execute("SELECT COUNT(*) FROM products WHERE price <= 0")
        invalid_prices = cursor.fetchone()[0]

        if invalid_prices > 0:
            logger.warning(
                f"⚠️ Found {invalid_prices} invalid price values (≤0)")
            all_valid = False

        # Check order amounts (should be numeric and positive)
        cursor.execute("SELECT COUNT(*) FROM orders WHERE total_amount <= 0")
        invalid_amounts = cursor.fetchone()[0]

        if invalid_amounts > 0:
            logger.warning(
                f"⚠️ Found {invalid_amounts} invalid order amounts (≤0)")
            all_valid = False

        cursor.close()
        conn.close()

        if all_valid:
            logger.info("✓ All data types are valid")
            print("✓ All data types are valid")

        return all_valid

    except Exception as e:
        logger.error(f"✗ Data type validation failed: {e}")
        raise


def validate_foreign_keys() -> bool:
    """Validate foreign key relationships."""

    logger.info("🔗 Validating foreign keys...")
    print("🔗 Validating foreign keys...")

    try:
        conn = get_connection()
        cursor = conn.cursor()

        all_valid = True

        # Check customer foreign keys in orders
        cursor.execute("""
            SELECT COUNT(*) FROM orders o
            LEFT JOIN customers c ON o.customer_id = c.customer_id
            WHERE c.customer_id IS NULL
        """)
        orphaned_customers = cursor.fetchone()[0]

        if orphaned_customers > 0:
            logger.warning(
                f"⚠️ Found {orphaned_customers} orders with invalid customer_id")
            all_valid = False

        # Check product foreign keys in orders
        cursor.execute("""
            SELECT COUNT(*) FROM orders o
            LEFT JOIN products p ON o.product_id = p.product_id
            WHERE p.product_id IS NULL
        """)
        orphaned_products = cursor.fetchone()[0]

        if orphaned_products > 0:
            logger.warning(
                f"⚠️ Found {orphaned_products} orders with invalid product_id")
            all_valid = False

        cursor.close()
        conn.close()

        if all_valid:
            logger.info("✓ All foreign keys are valid")
            print("✓ All foreign keys are valid")

        return all_valid

    except Exception as e:
        logger.error(f"✗ Foreign key validation failed: {e}")
        raise


def validate_uniqueness() -> bool:
    """Validate uniqueness constraints."""

    logger.info("🔑 Validating uniqueness...")
    print("🔑 Validating uniqueness...")

    try:
        conn = get_connection()
        cursor = conn.cursor()

        all_valid = True

        # Check customer ID uniqueness
        cursor.execute("""
            SELECT COUNT(*) - COUNT(DISTINCT customer_id) FROM customers
        """)
        dup_customers = cursor.fetchone()[0]

        if dup_customers > 0:
            logger.warning(f"⚠️ Found {dup_customers} duplicate customer IDs")
            all_valid = False

        # Check product ID uniqueness
        cursor.execute("""
            SELECT COUNT(*) - COUNT(DISTINCT product_id) FROM products
        """)
        dup_products = cursor.fetchone()[0]

        if dup_products > 0:
            logger.warning(f"⚠️ Found {dup_products} duplicate product IDs")
            all_valid = False

        # Check order ID uniqueness
        cursor.execute("""
            SELECT COUNT(*) - COUNT(DISTINCT order_id) FROM orders
        """)
        dup_orders = cursor.fetchone()[0]

        if dup_orders > 0:
            logger.warning(f"⚠️ Found {dup_orders} duplicate order IDs")
            all_valid = False

        # Check email uniqueness
        cursor.execute("""
            SELECT COUNT(*) - COUNT(DISTINCT email) FROM customers
        """)
        dup_emails = cursor.fetchone()[0]

        if dup_emails > 0:
            logger.warning(f"⚠️ Found {dup_emails} duplicate emails")
            all_valid = False

        cursor.close()
        conn.close()

        if all_valid:
            logger.info("✓ All uniqueness constraints satisfied")
            print("✓ All uniqueness constraints satisfied")

        return all_valid

    except Exception as e:
        logger.error(f"✗ Uniqueness validation failed: {e}")
        raise


def main():
    """Main entry point."""

    logger.info("=" * 60)
    logger.info("DATA VALIDATION")
    logger.info("=" * 60)

    print("=" * 60)
    print("DATA VALIDATION")
    print("=" * 60)

    try:
        results = []

        results.append(validate_required_fields())
        results.append(validate_data_types())
        results.append(validate_foreign_keys())
        results.append(validate_uniqueness())

        all_passed = all(results)

        logger.info("=" * 60)
        if all_passed:
            logger.info("✓ ALL VALIDATIONS PASSED")
            print("\n" + "=" * 60)
            print("✓ ALL VALIDATIONS PASSED")
        else:
            logger.warning("⚠️ SOME VALIDATIONS FAILED")
            print("\n" + "=" * 60)
            print("⚠️ SOME VALIDATIONS FAILED")

        logger.info("=" * 60)
        print("=" * 60)

        if not all_passed:
            sys.exit(1)

    except Exception as e:
        logger.error(f"✗ Critical error: {e}")
        print(f"\n✗ Critical error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    Config.create_directories()
    main()
