"""
Anomaly Detection
=================

Statistical anomaly detection for e-commerce data.

Uses Z-score method to identify outliers in:
- Order amounts
- Quantities
- Customer spending patterns
- Daily sales
- Business rule violations

Author: Abhiiram
Date: November 6, 2025
"""

from src.utils.db_connector import get_connection
from src.utils.config import Config
import psycopg2
import pandas as pd
import numpy as np
from datetime import datetime
from typing import List, Dict
import sys
import os
from loguru import logger

# Add parent directory to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../../'))


# Configure logging
logger.add(
    f"{Config.LOGS_DIR}/anomalies_{{time:YYYY-MM-DD}}.log",
    level=Config.LOG_LEVEL
)

# ========================================
# ANOMALY DETECTOR CLASS
# ========================================


class AnomalyDetector:
    """Detect anomalies in e-commerce data."""

    def __init__(self):
        """Initialize detector with database connection."""
        self.conn = None
        self.anomalies: List[Dict] = []
        logger.info("✓ Anomaly detector initialized")

    def z_score_outliers(self, df: pd.DataFrame, column: str, threshold: float = 3.0) -> pd.DataFrame:
        """
        Detect outliers using Z-score method.

        Args:
            df: DataFrame to analyze
            column: Column name to check
            threshold: Z-score threshold (default: 3.0)

        Returns:
            DataFrame with outliers
        """
        try:
            mean = df[column].mean()
            std = df[column].std()

            if std == 0:
                return pd.DataFrame()

            df['z_score'] = np.abs((df[column] - mean) / std)
            outliers = df[df['z_score'] > threshold].copy()

            logger.info(
                f"✓ Z-score analysis: {len(outliers)} outliers found (threshold={threshold})")
            return outliers

        except Exception as e:
            logger.error(f"✗ Z-score calculation failed: {e}")
            raise

    def detect_order_amount_anomalies(self) -> None:
        """Detect unusually high or low order amounts."""

        logger.info("💰 ORDER AMOUNT ANOMALIES")
        print("\n💰 ORDER AMOUNT ANOMALIES")
        print("-" * 60)

        try:
            query = """
            SELECT order_id, customer_id, product_id,
            quantity, total_amount, order_date
            FROM orders
            WHERE order_status = 'Delivered'
            """

            conn = get_connection()
            df = pd.read_sql(query, conn)
            conn.close()

            # Detect outliers
            outliers = self.z_score_outliers(
                df, 'total_amount', threshold=Config.ANOMALY_ZSCORE_THRESHOLD)

            if len(outliers) > 0:
                msg = f"Found {len(outliers)} unusual order amounts (Z-score > {Config.ANOMALY_ZSCORE_THRESHOLD})"
                logger.info(f" {msg}")
                print(f" Found {len(outliers)} unusual order amounts")
                print(f" Top 5 highest amounts:")

                top_5 = outliers.nlargest(5, 'total_amount')
                for idx, row in top_5.iterrows():
                    log_msg = f"  Order {row['order_id']}: ₹{row['total_amount']:,.2f} (Z-score: {row['z_score']:.2f})"
                    logger.info(log_msg)
                    print(log_msg)

                self.anomalies.append({
                    'type': 'Order Amount Outliers',
                    'count': len(outliers),
                    'severity': 'INFO',
                    'details': f"Orders with amounts >{Config.ANOMALY_ZSCORE_THRESHOLD} std deviations from mean"
                })
            else:
                logger.info(" ✓ No significant outliers detected")
                print(" ✓ No significant outliers detected")

        except Exception as e:
            logger.error(f"✗ Order amount detection failed: {e}")
            raise

    def detect_quantity_anomalies(self) -> None:
        """Detect unusual order quantities."""

        logger.info("📦 ORDER QUANTITY ANOMALIES")
        print("\n📦 ORDER QUANTITY ANOMALIES")
        print("-" * 60)

        try:
            query = """
            SELECT o.order_id, o.customer_id, o.product_id,
            p.product_name, o.quantity
            FROM orders o
            JOIN products p ON o.product_id = p.product_id
            WHERE o.order_status = 'Delivered'
            """

            conn = get_connection()
            df = pd.read_sql(query, conn)
            conn.close()

            # High quantities
            high_qty = df[df['quantity'] > 5]

            if len(high_qty) > 0:
                logger.info(f" Found {len(high_qty)} orders with quantity > 5")
                print(f" Found {len(high_qty)} orders with quantity > 5")
                print(f" Top 5 highest quantities:")

                top_5 = high_qty.nlargest(5, 'quantity')
                for idx, row in top_5.iterrows():
                    log_msg = f"  Order {row['order_id']}: {row['quantity']} × {row['product_name']}"
                    logger.info(log_msg)
                    print(log_msg)

                self.anomalies.append({
                    'type': 'High Quantity Orders',
                    'count': len(high_qty),
                    'severity': 'INFO',
                    'details': 'Orders with quantities > 5 (potential bulk purchases)'
                })
            else:
                logger.info(" ✓ No unusual quantities detected")
                print(" ✓ No unusual quantities detected")

        except Exception as e:
            logger.error(f"✗ Quantity detection failed: {e}")
            raise

    def detect_customer_spending_anomalies(self) -> None:
        """Detect customers with unusual spending patterns."""

        logger.info("👤 CUSTOMER SPENDING ANOMALIES")
        print("\n👤 CUSTOMER SPENDING ANOMALIES")
        print("-" * 60)

        try:
            query = "SELECT * FROM customer_summary"

            conn = get_connection()
            df = pd.read_sql(query, conn)
            conn.close()

            # High spenders
            outliers = self.z_score_outliers(df, 'total_spent', threshold=2.5)

            if len(outliers) > 0:
                logger.info(
                    f" Found {len(outliers)} customers with unusual spending (Z-score > 2.5)")
                print(
                    f" Found {len(outliers)} customers with unusual spending")
                print(f" Top 5 spenders:")

                top_5 = outliers.nlargest(5, 'total_spent')
                for idx, row in top_5.iterrows():
                    log_msg = f"  {row['customer_name']}: ₹{row['total_spent']:,.2f} ({row['total_orders']} orders, Z-score: {row['z_score']:.2f})"
                    logger.info(log_msg)
                    print(log_msg)

                self.anomalies.append({
                    'type': 'High-Value Customers',
                    'count': len(outliers),
                    'severity': 'INFO',
                    'details': 'VIP customers with exceptional spending patterns'
                })
            else:
                logger.info(" ✓ No unusual spending patterns detected")
                print(" ✓ No unusual spending patterns detected")

        except Exception as e:
            logger.error(f"✗ Customer spending detection failed: {e}")
            raise

    def detect_daily_sales_anomalies(self) -> None:
        """Detect unusual daily sales patterns."""

        logger.info("📊 DAILY SALES ANOMALIES")
        print("\n📊 DAILY SALES ANOMALIES")
        print("-" * 60)

        try:
            query = """
            SELECT sale_date, total_orders, total_revenue
            FROM daily_sales_summary
            ORDER BY sale_date DESC
            """

            conn = get_connection()
            df = pd.read_sql(query, conn)
            conn.close()

            # Revenue outliers
            outliers = self.z_score_outliers(
                df, 'total_revenue', threshold=2.0)

            if len(outliers) > 0:
                logger.info(
                    f" Found {len(outliers)} days with unusual sales (Z-score > 2.0)")
                print(f" Found {len(outliers)} days with unusual sales")
                print(f" Top 5 unusual days:")

                top_5 = outliers.nlargest(5, 'total_revenue')
                for idx, row in top_5.iterrows():
                    log_msg = f"  {row['sale_date']}: ₹{row['total_revenue']:,.2f} ({row['total_orders']} orders, Z-score: {row['z_score']:.2f})"
                    logger.info(log_msg)
                    print(log_msg)

                self.anomalies.append({
                    'type': 'Daily Sales Spikes',
                    'count': len(outliers),
                    'severity': 'INFO',
                    'details': 'Days with exceptional sales performance'
                })
            else:
                logger.info(" ✓ No unusual daily patterns detected")
                print(" ✓ No unusual daily patterns detected")

        except Exception as e:
            logger.error(f"✗ Daily sales detection failed: {e}")
            raise

    def detect_business_rule_violations(self) -> None:
        """Detect violations of business rules."""

        logger.info("⚠️ BUSINESS RULE VIOLATIONS")
        print("\n⚠️ BUSINESS RULE VIOLATIONS")
        print("-" * 60)

        try:
            conn = get_connection()
            cursor = conn.cursor()

            violations = []

            # Rule 1: Very high order values
            cursor.execute("""
                SELECT COUNT(*) FROM orders
                WHERE total_amount > 200000 AND order_status = 'Delivered'
            """)
            high_value = cursor.fetchone()[0]

            if high_value > 0:
                msg = f"⚠️ {high_value} orders exceed ₹2 Lakh (potential review needed)"
                logger.warning(msg)
                print(f" {msg}")
                violations.append(f"{high_value} very high-value orders")

            # Rule 2: Frequent customers
            cursor.execute("""
                SELECT COUNT(*) FROM customer_summary WHERE total_orders > 20
            """)
            frequent = cursor.fetchone()[0]

            if frequent > 0:
                msg = f"ℹ️ {frequent} customers have >20 orders (loyalty program candidates)"
                logger.info(msg)
                print(f" {msg}")
                violations.append(f"{frequent} highly active customers")

            # Rule 3: Zero revenue orders
            cursor.execute("""
                SELECT COUNT(*) FROM orders WHERE total_amount = 0
            """)
            zero_revenue = cursor.fetchone()[0]

            if zero_revenue > 0:
                msg = f"✗ {zero_revenue} orders with ₹0 total (data quality issue)"
                logger.error(msg)
                print(f" {msg}")
                violations.append(f"{zero_revenue} zero-revenue orders")
            else:
                logger.info(" ✓ No zero-revenue orders")
                print(" ✓ No zero-revenue orders")

            cursor.close()
            conn.close()

            if violations:
                self.anomalies.append({
                    'type': 'Business Rule Violations',
                    'count': len(violations),
                    'severity': 'WARNING',
                    'details': ', '.join(violations)
                })

        except Exception as e:
            logger.error(f"✗ Business rule detection failed: {e}")
            raise

    def run_all_detections(self) -> List[Dict]:
        """Run all anomaly detection checks."""

        logger.info("=" * 60)
        logger.info("ANOMALY DETECTION")
        logger.info(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info("=" * 60)

        print("=" * 60)
        print("ANOMALY DETECTION")
        print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("=" * 60)

        try:
            self.detect_order_amount_anomalies()
            self.detect_quantity_anomalies()
            self.detect_customer_spending_anomalies()
            self.detect_daily_sales_anomalies()
            self.detect_business_rule_violations()

            # Summary
            logger.info("=" * 60)
            logger.info("ANOMALY SUMMARY")
            logger.info("-" * 60)

            print("\n" + "=" * 60)
            print("ANOMALY SUMMARY")
            print("-" * 60)

            if self.anomalies:
                logger.info(
                    f"Total anomaly types detected: {len(self.anomalies)}")
                print(f" Total anomaly types detected: {len(self.anomalies)}")

                for anomaly in self.anomalies:
                    msg = f"• {anomaly['type']}: {anomaly['count']} instances ({anomaly['severity']})"
                    logger.info(msg)
                    print(f" {msg}")
            else:
                logger.info("✓ No significant anomalies detected")
                print(" ✓ No significant anomalies detected")

            logger.info("=" * 60)
            print("=" * 60)

            return self.anomalies

        except Exception as e:
            logger.error(f"✗ Detection process failed: {e}")
            raise


def main():
    """Main entry point."""

    try:
        detector = AnomalyDetector()
        anomalies = detector.run_all_detections()

    except Exception as e:
        logger.error(f"✗ Critical error: {e}")
        print(f"\n✗ Critical error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    Config.create_directories()
    main()
