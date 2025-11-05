"""
Anomaly Detection
=================
Statistical anomaly detection for e-commerce data.

Detects:
1. Statistical outliers (Z-score method)
2. Business rule violations
3. Unusual patterns in transactions

Author: Abhiiram
Date: November 5, 2025
"""

import psycopg2
import pandas as pd
import numpy as np
from datetime import datetime
from typing import List, Dict

# Database connection
DB_CONFIG = {
    'host': 'localhost',
    'port': 5432,
    'database': 'ecommerce_db',
    'user': 'dataeng',
    'password': 'pipeline123'
}


class AnomalyDetector:
    """Detect anomalies in e-commerce data."""

    def __init__(self):
        self.conn = psycopg2.connect(**DB_CONFIG)
        self.anomalies = []

    def z_score_outliers(self, df: pd.DataFrame, column: str,
                         threshold: float = 3.0) -> pd.DataFrame:
        """
        Detect outliers using Z-score method.

        Args:
            df: DataFrame to analyze
            column: Column name to check
            threshold: Z-score threshold (default: 3.0)

        Returns:
            DataFrame with outliers
        """
        mean = df[column].mean()
        std = df[column].std()

        if std == 0:
            return pd.DataFrame()

        df['z_score'] = np.abs((df[column] - mean) / std)
        outliers = df[df['z_score'] > threshold].copy()

        return outliers

    def detect_order_amount_anomalies(self):
        """Detect unusually high or low order amounts."""
        print("\n💰 ORDER AMOUNT ANOMALIES")
        print("-" * 60)

        query = """
            SELECT order_id, customer_id, product_id, 
                   quantity, total_amount, order_date
            FROM orders
            WHERE order_status = 'Delivered'
        """

        df = pd.read_sql(query, self.conn)

        # Detect outliers
        outliers = self.z_score_outliers(df, 'total_amount', threshold=3.0)

        if len(outliers) > 0:
            print(
                f"  Found {len(outliers)} unusual order amounts (Z-score > 3.0)")
            print(f"  Top 5 highest amounts:")

            top_5 = outliers.nlargest(5, 'total_amount')
            for idx, row in top_5.iterrows():
                print(f"    Order {row['order_id']}: ₹{row['total_amount']:,.2f} "
                      f"(Z-score: {row['z_score']:.2f})")

            self.anomalies.append({
                'type': 'Order Amount Outliers',
                'count': len(outliers),
                'severity': 'INFO',
                'details': f"Orders with amounts >3 std deviations from mean"
            })
        else:
            print("  ✓ No significant outliers detected")

    def detect_quantity_anomalies(self):
        """Detect unusual order quantities."""
        print("\n📦 ORDER QUANTITY ANOMALIES")
        print("-" * 60)

        query = """
            SELECT o.order_id, o.customer_id, o.product_id, 
                   p.product_name, o.quantity
            FROM orders o
            JOIN products p ON o.product_id = p.product_id
            WHERE o.order_status = 'Delivered'
        """

        df = pd.read_sql(query, self.conn)

        # Very high quantities (potential bulk orders or errors)
        high_qty = df[df['quantity'] > 5]

        if len(high_qty) > 0:
            print(f"  Found {len(high_qty)} orders with quantity > 5")
            print(f"  Top 5 highest quantities:")

            top_5 = high_qty.nlargest(5, 'quantity')
            for idx, row in top_5.iterrows():
                print(
                    f"    Order {row['order_id']}: {row['quantity']} × {row['product_name']}")

            self.anomalies.append({
                'type': 'High Quantity Orders',
                'count': len(high_qty),
                'severity': 'INFO',
                'details': 'Orders with quantities > 5 (potential bulk purchases)'
            })
        else:
            print("  ✓ No unusual quantities detected")

    def detect_customer_spending_anomalies(self):
        """Detect customers with unusual spending patterns."""
        print("\n👤 CUSTOMER SPENDING ANOMALIES")
        print("-" * 60)

        df = pd.read_sql("SELECT * FROM customer_summary", self.conn)

        # High spenders (outliers)
        outliers = self.z_score_outliers(df, 'total_spent', threshold=2.5)

        if len(outliers) > 0:
            print(
                f"  Found {len(outliers)} customers with unusual spending (Z-score > 2.5)")
            print(f"  Top 5 spenders:")

            top_5 = outliers.nlargest(5, 'total_spent')
            for idx, row in top_5.iterrows():
                print(f"    {row['customer_name']}: ₹{row['total_spent']:,.2f} "
                      f"({row['total_orders']} orders, Z-score: {row['z_score']:.2f})")

            self.anomalies.append({
                'type': 'High-Value Customers',
                'count': len(outliers),
                'severity': 'INFO',
                'details': 'VIP customers with exceptional spending patterns'
            })
        else:
            print("  ✓ No unusual spending patterns detected")

    def detect_daily_sales_anomalies(self):
        """Detect unusual daily sales patterns."""
        print("\n📊 DAILY SALES ANOMALIES")
        print("-" * 60)

        df = pd.read_sql("""
            SELECT sale_date, total_orders, total_revenue
            FROM daily_sales_summary
            ORDER BY sale_date DESC
        """, self.conn)

        # Detect revenue outliers
        outliers = self.z_score_outliers(df, 'total_revenue', threshold=2.0)

        if len(outliers) > 0:
            print(
                f"  Found {len(outliers)} days with unusual sales (Z-score > 2.0)")
            print(f"  Top 5 unusual days:")

            top_5 = outliers.nlargest(5, 'total_revenue')
            for idx, row in top_5.iterrows():
                print(f"    {row['sale_date']}: ₹{row['total_revenue']:,.2f} "
                      f"({row['total_orders']} orders, Z-score: {row['z_score']:.2f})")

            self.anomalies.append({
                'type': 'Daily Sales Spikes',
                'count': len(outliers),
                'severity': 'INFO',
                'details': 'Days with exceptional sales performance'
            })
        else:
            print("  ✓ No unusual daily patterns detected")

    def detect_business_rule_violations(self):
        """Detect violations of business rules."""
        print("\n⚠️  BUSINESS RULE VIOLATIONS")
        print("-" * 60)

        violations = []

        # Rule 1: Very high order values (potential fraud)
        cursor = self.conn.cursor()
        cursor.execute("""
            SELECT COUNT(*) FROM orders 
            WHERE total_amount > 200000 AND order_status = 'Delivered'
        """)
        high_value = cursor.fetchone()[0]

        if high_value > 0:
            print(
                f"  ⚠️  {high_value} orders exceed ₹2 Lakh (potential review needed)")
            violations.append(f"{high_value} very high-value orders")

        # Rule 2: Customers with >20 orders in database
        cursor.execute("""
            SELECT COUNT(*) FROM customer_summary WHERE total_orders > 20
        """)
        frequent = cursor.fetchone()[0]

        if frequent > 0:
            print(
                f"  ℹ️  {frequent} customers have >20 orders (loyalty program candidates)")
            violations.append(f"{frequent} highly active customers")

        # Rule 3: Orders with 0 revenue (should not exist)
        cursor.execute("""
            SELECT COUNT(*) FROM orders WHERE total_amount = 0
        """)
        zero_revenue = cursor.fetchone()[0]

        if zero_revenue > 0:
            print(
                f"  ✗ {zero_revenue} orders with ₹0 total (data quality issue)")
            violations.append(f"{zero_revenue} zero-revenue orders")
        else:
            print(f"  ✓ No zero-revenue orders")

        cursor.close()

        if violations:
            self.anomalies.append({
                'type': 'Business Rule Violations',
                'count': len(violations),
                'severity': 'WARNING',
                'details': ', '.join(violations)
            })

    def run_all_detections(self):
        """Run all anomaly detection checks."""
        print("=" * 60)
        print("ANOMALY DETECTION")
        print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("=" * 60)

        self.detect_order_amount_anomalies()
        self.detect_quantity_anomalies()
        self.detect_customer_spending_anomalies()
        self.detect_daily_sales_anomalies()
        self.detect_business_rule_violations()

        # Summary
        print("\n" + "=" * 60)
        print("ANOMALY SUMMARY")
        print("-" * 60)

        if self.anomalies:
            print(f"  Total anomaly types detected: {len(self.anomalies)}")
            for anomaly in self.anomalies:
                print(
                    f"  • {anomaly['type']}: {anomaly['count']} instances ({anomaly['severity']})")
        else:
            print("  ✓ No significant anomalies detected")

        print("=" * 60)

        return self.anomalies

    def close(self):
        """Close database connection."""
        if self.conn:
            self.conn.close()


if __name__ == "__main__":
    detector = AnomalyDetector()
    anomalies = detector.run_all_detections()
    detector.close()
