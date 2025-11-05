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
Date: November 5, 2025
"""

import psycopg2
import pandas as pd
from datetime import datetime
from typing import Dict, List, Tuple

# Database connection
DB_CONFIG = {
    'host': 'localhost',
    'port': 5432,
    'database': 'ecommerce_db',
    'user': 'dataeng',
    'password': 'pipeline123'
}


class DataQualityChecker:
    """Main class for running data quality checks."""

    def __init__(self):
        self.conn = psycopg2.connect(**DB_CONFIG)
        self.results = []
        self.passed = 0
        self.failed = 0

    def run_check(self, check_name: str, query: str, expected: any,
                  severity: str = "ERROR") -> bool:
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
            cursor = self.conn.cursor()
            cursor.execute(query)
            result = cursor.fetchone()[0]
            cursor.close()

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
                print(f"  ✓ {check_name}: PASS")
            else:
                self.failed += 1
                print(
                    f"  ✗ {check_name}: FAIL (Expected: {expected}, Got: {result})")

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
            print(f"  ✗ {check_name}: ERROR - {e}")
            return False

    # ========================================
    # COMPLETENESS CHECKS
    # ========================================

    def check_completeness(self):
        """Check for null values in critical columns."""
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

    def check_validity(self):
        """Check data ranges and formats."""
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

        # Date ranges (orders shouldn't be in future)
        self.run_check(
            "Orders: No future order dates",
            "SELECT COUNT(*) FROM orders WHERE order_date > CURRENT_TIMESTAMP",
            0
        )

    # ========================================
    # CONSISTENCY CHECKS
    # ========================================

    def check_consistency(self):
        """Check referential integrity."""
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

    def check_uniqueness(self):
        """Check for duplicate records."""
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
    # RUN ALL CHECKS
    # ========================================

    def run_all_checks(self):
        """Execute all quality checks."""
        print("=" * 60)
        print("DATA QUALITY VALIDATION")
        print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("=" * 60)

        self.check_completeness()
        self.check_validity()
        self.check_consistency()
        self.check_uniqueness()

        # Summary
        total = self.passed + self.failed
        pass_rate = (self.passed / total * 100) if total > 0 else 0

        print("\n" + "=" * 60)
        print("QUALITY SUMMARY")
        print("-" * 60)
        print(f"  Total Checks:  {total}")
        print(f"  ✓ Passed:      {self.passed} ({pass_rate:.1f}%)")
        print(f"  ✗ Failed:      {self.failed}")
        print(f"  Quality Score: {pass_rate:.1f}%")
        print("=" * 60)

        # Quality grade
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

        print(f"\n  Data Quality Grade: {grade}\n")

        return self.results

    def close(self):
        """Close database connection."""
        if self.conn:
            self.conn.close()


if __name__ == "__main__":
    checker = DataQualityChecker()
    results = checker.run_all_checks()
    checker.close()
