"""
Data Quality Checks
===================

Comprehensive data quality validation framework.

Implements 4 quality dimensions:
1. Completeness - Check for null values in critical columns
2. Validity - Check data ranges, types, formats
3. Consistency - Check referential integrity
4. Uniqueness - Check for duplicates

Author: Abhiiram
Date: November 6, 2025
"""

import os
import psycopg2
from datetime import datetime
from typing import Dict, List, Any, Optional
from loguru import logger
import json

# Configure logging
logger.add("logs/data_quality_{time:YYYY-MM-DD}.log", level="INFO")


class DatabaseConnection:
    """Manage PostgreSQL database connections with error handling."""

    def __init__(self):
        """Initialize database connection from environment variables."""
        self.conn = None
        self._connect()

    def _connect(self) -> None:
        """Create database connection with error handling."""
        try:
            db_config = {
                'host': os.getenv('ECOMMERCE_DB_HOST', 'localhost'),
                'port': int(os.getenv('ECOMMERCE_DB_PORT', 5432)),
                'database': os.getenv('ECOMMERCE_DB_NAME', 'ecommerce_db'),
                'user': os.getenv('ECOMMERCE_DB_USER', 'dataeng'),
                'password': os.getenv('ECOMMERCE_DB_PASSWORD', 'pipeline123'),
                'connect_timeout': 5
            }

            self.conn = psycopg2.connect(**db_config)
            logger.info("✓ Database connection established")

        except psycopg2.OperationalError as e:
            logger.error(f"✗ Database connection failed: {e}")
            raise
        except Exception as e:
            logger.error(f"✗ Unexpected error during connection: {e}")
            raise

    def execute_query(self, query: str) -> Any:
        """Execute query and return result with error handling."""
        try:
            if not self.conn:
                raise RuntimeError("Database connection not established")

            cursor = self.conn.cursor()
            cursor.execute(query)
            result = cursor.fetchone()[0]
            cursor.close()

            return result

        except psycopg2.ProgrammingError as e:
            logger.error(f"✗ SQL syntax error: {e}")
            raise
        except Exception as e:
            logger.error(f"✗ Query execution error: {e}")
            raise

    def close(self) -> None:
        """Close database connection safely."""
        if self.conn:
            self.conn.close()
            logger.info("✓ Database connection closed")


class DataQualityChecker:
    """Main class for running data quality checks."""

    def __init__(self):
        """Initialize quality checker with database connection."""
        self.db = DatabaseConnection()
        self.results: List[Dict[str, Any]] = []
        self.passed: int = 0
        self.failed: int = 0
        self.timestamp = datetime.now()

    def run_check(
        self,
        check_name: str,
        query: str,
        expected: Any,
        severity: str = "ERROR"
    ) -> bool:
        """
        Run a single quality check.

        Args:
            check_name: Name of the check
            query: SQL query to execute
            expected: Expected value for the check to pass
            severity: ERROR or WARNING

        Returns:
            bool: True if check passed
        """
        try:
            result = self.db.execute_query(query)
            passed = (result == expected)

            self.results.append({
                'check_name': check_name,
                'severity': severity,
                'status': 'PASS' if passed else 'FAIL',
                'expected': expected,
                'actual': result,
                'timestamp': datetime.now().isoformat()
            })

            if passed:
                self.passed += 1
                logger.info(f"✓ {check_name}: PASS")
                print(f" ✓ {check_name}: PASS")
            else:
                self.failed += 1
                logger.warning(
                    f"✗ {check_name}: FAIL (Expected: {expected}, Got: {result})")
                print(
                    f" ✗ {check_name}: FAIL (Expected: {expected}, Got: {result})")

            return passed

        except Exception as e:
            self.failed += 1
            self.results.append({
                'check_name': check_name,
                'severity': severity,
                'status': 'ERROR',
                'error': str(e),
                'timestamp': datetime.now().isoformat()
            })
            logger.error(f"✗ {check_name}: ERROR - {e}")
            print(f" ✗ {check_name}: ERROR - {e}")
            return False

    # ========================================
    # COMPLETENESS CHECKS
    # ========================================

    def check_completeness(self) -> None:
        """Check for null values in critical columns."""
        logger.info("Starting COMPLETENESS checks")
        print("\n📋 COMPLETENESS CHECKS")
        print("-" * 60)

        # Customer table
        self.run_check(
            "Customers: No NULL emails",
            "SELECT COUNT(*) FROM customers WHERE email IS NULL",
            0
        )

        self.run_check(
            "Customers: No NULL customer_ids",
            "SELECT COUNT(*) FROM customers WHERE customer_id IS NULL",
            0
        )

        # Products table
        self.run_check(
            "Products: No NULL product names",
            "SELECT COUNT(*) FROM products WHERE product_name IS NULL",
            0
        )

        self.run_check(
            "Products: No NULL prices",
            "SELECT COUNT(*) FROM products WHERE price IS NULL",
            0
        )

        # Orders table
        self.run_check(
            "Orders: No NULL order_ids",
            "SELECT COUNT(*) FROM orders WHERE order_id IS NULL",
            0
        )

        self.run_check(
            "Orders: No NULL total_amounts",
            "SELECT COUNT(*) FROM orders WHERE total_amount IS NULL",
            0
        )

    # ========================================
    # VALIDITY CHECKS
    # ========================================

    def check_validity(self) -> None:
        """Check data ranges and formats."""
        logger.info("Starting VALIDITY checks")
        print("\n✅ VALIDITY CHECKS")
        print("-" * 60)

        # Age ranges
        self.run_check(
            "Customers: Age between 18-120",
            "SELECT COUNT(*) FROM customers WHERE age < 18 OR age > 120",
            0
        )

        # Price ranges
        self.run_check(
            "Products: Price > 0",
            "SELECT COUNT(*) FROM products WHERE price <= 0",
            0
        )

        self.run_check(
            "Orders: Total amount > 0",
            "SELECT COUNT(*) FROM orders WHERE total_amount <= 0",
            0
        )

        # Quantity ranges
        self.run_check(
            "Orders: Quantity > 0",
            "SELECT COUNT(*) FROM orders WHERE quantity <= 0",
            0
        )

        # Email format (basic check)
        self.run_check(
            "Customers: Valid email format",
            "SELECT COUNT(*) FROM customers WHERE email NOT LIKE '%@%.%'",
            0
        )

        # Date ranges
        self.run_check(
            "Orders: No future order dates",
            "SELECT COUNT(*) FROM orders WHERE order_date > CURRENT_TIMESTAMP",
            0
        )

    # ========================================
    # CONSISTENCY CHECKS
    # ========================================

    def check_consistency(self) -> None:
        """Check referential integrity."""
        logger.info("Starting CONSISTENCY checks")
        print("\n🔗 CONSISTENCY CHECKS")
        print("-" * 60)

        # Foreign key integrity
        self.run_check(
            "Orders: All customer_ids exist in customers",
            """SELECT COUNT(*) FROM orders o
               LEFT JOIN customers c ON o.customer_id = c.customer_id
               WHERE c.customer_id IS NULL""",
            0
        )

        self.run_check(
            "Orders: All product_ids exist in products",
            """SELECT COUNT(*) FROM orders o
               LEFT JOIN products p ON o.product_id = p.product_id
               WHERE p.product_id IS NULL""",
            0
        )

        # Business logic consistency
        self.run_check(
            "Orders: Quantity * Price = Total (within 1%)",
            """SELECT COUNT(*) FROM orders o
               JOIN products p ON o.product_id = p.product_id
               WHERE ABS(o.total_amount - (o.quantity * p.price)) > (o.quantity * p.price * 0.01)""",
            0,
            severity="WARNING"
        )

        # Aggregate table consistency
        self.run_check(
            "Customer Summary: Row count matches active customers",
            """SELECT ABS(
                   (SELECT COUNT(DISTINCT customer_id) FROM orders WHERE order_status = 'Delivered') -
                   (SELECT COUNT(*) FROM customer_summary)
               )""",
            0,
            severity="WARNING"
        )

    # ========================================
    # UNIQUENESS CHECKS
    # ========================================

    def check_uniqueness(self) -> None:
        """Check for duplicate records."""
        logger.info("Starting UNIQUENESS checks")
        print("\n🔑 UNIQUENESS CHECKS")
        print("-" * 60)

        # Primary key uniqueness
        self.run_check(
            "Customers: No duplicate customer_ids",
            "SELECT COUNT(*) - COUNT(DISTINCT customer_id) FROM customers",
            0
        )

        self.run_check(
            "Products: No duplicate product_ids",
            "SELECT COUNT(*) - COUNT(DISTINCT product_id) FROM products",
            0
        )

        self.run_check(
            "Orders: No duplicate order_ids",
            "SELECT COUNT(*) - COUNT(DISTINCT order_id) FROM orders",
            0
        )

        # Business uniqueness
        self.run_check(
            "Customers: No duplicate emails",
            "SELECT COUNT(*) - COUNT(DISTINCT email) FROM customers",
            0
        )

    # ========================================
    # RUN ALL CHECKS & GENERATE REPORT
    # ========================================

    def run_all_checks(self) -> List[Dict[str, Any]]:
        """Execute all quality checks."""
        logger.info("=" * 60)
        logger.info("DATA QUALITY VALIDATION STARTED")
        logger.info(
            f"Timestamp: {self.timestamp.strftime('%Y-%m-%d %H:%M:%S')}")

        print("=" * 60)
        print("DATA QUALITY VALIDATION")
        print(f"Started: {self.timestamp.strftime('%Y-%m-%d %H:%M:%S')}")
        print("=" * 60)

        try:
            self.check_completeness()
            self.check_validity()
            self.check_consistency()
            self.check_uniqueness()

            self._print_summary()
            self._save_results()

            return self.results

        except Exception as e:
            logger.error(f"✗ Quality check failed: {e}")
            raise

    def _print_summary(self) -> None:
        """Print quality summary report."""
        total = self.passed + self.failed
        pass_rate = (self.passed / total * 100) if total > 0 else 0

        # Determine grade
        if pass_rate == 100:
            grade = "A+ (Excellent)"
        elif pass_rate >= 95:
            grade = "A (Very Good)"
        elif pass_rate >= 90:
            grade = "B (Good)"
        elif pass_rate >= 80:
            grade = "C (Acceptable)"
        else:
            grade = "F (Needs Attention)"

        print("\n" + "=" * 60)
        print("QUALITY SUMMARY")
        print("-" * 60)
        print(f" Total Checks: {total}")
        print(f" ✓ Passed: {self.passed} ({pass_rate:.1f}%)")
        print(f" ✗ Failed: {self.failed}")
        print(f" Quality Score: {pass_rate:.1f}%")
        print(f" Data Quality Grade: {grade}")
        print("=" * 60)

        logger.info(
            f"Quality Summary - Passed: {self.passed}, Failed: {self.failed}, Grade: {grade}")

    def _save_results(self) -> None:
        """Save quality check results to JSON file."""
        try:
            os.makedirs('data/processed', exist_ok=True)

            results_file = f"data/processed/quality_check_{self.timestamp.strftime('%Y%m%d_%H%M%S')}.json"

            with open(results_file, 'w') as f:
                json.dump(self.results, f, indent=2)

            logger.info(f"✓ Results saved to {results_file}")
            print(f"\n✓ Results saved to {results_file}")

        except Exception as e:
            logger.error(f"✗ Failed to save results: {e}")

    def close(self) -> None:
        """Close database connection."""
        self.db.close()


def main():
    """Main entry point for data quality checks."""
    checker = None

    try:
        checker = DataQualityChecker()
        results = checker.run_all_checks()

    except Exception as e:
        logger.error(f"✗ Critical error: {e}")
        print(f"\n✗ Critical error: {e}")
        raise

    finally:
        if checker:
            checker.close()


if __name__ == "__main__":
    main()
