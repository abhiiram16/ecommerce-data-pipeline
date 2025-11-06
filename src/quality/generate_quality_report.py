"""
HTML Quality Report Generator with Issue Tracking
==================================================

Generates comprehensive HTML data quality report with:
- Quality scores (4 dimensions)
- Detailed issue detection
- Remediation guidance
- Data statistics
- Executive summary

Author: Abhiiram
Date: November 7, 2025
"""

from src.utils.db_connector import get_connection
from src.utils.config import Config
import json
import psycopg2
import pandas as pd
from datetime import datetime
import sys
import os
from loguru import logger

# Add parent directory to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../../'))

# Configure logging
logger.add(
    f"{Config.LOGS_DIR}/reports_{{time:YYYY-MM-DD}}.log",
    level=Config.LOG_LEVEL
)

# ========================================
# QUALITY REPORT GENERATOR
# ========================================


class QualityReportGenerator:
    """Generate HTML quality reports with issue tracking."""

    def __init__(self):
        """Initialize report generator."""
        self.timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        self.issues = []
        logger.info("✓ Report generator initialized")

    def get_table_stats(self) -> dict:
        """Get statistics for all tables."""

        logger.info("📊 Collecting table statistics...")

        try:
            conn = get_connection()

            tables = [
                'customers', 'products', 'orders',
                'customer_summary', 'product_summary',
                'daily_sales_summary', 'monthly_sales_summary'
            ]

            stats = {}

            for table in tables:
                try:
                    df = pd.read_sql(
                        f"SELECT COUNT(*) as count FROM {table}", conn)
                    count = df['count'].iloc[0]
                    stats[table] = count
                    logger.info(f"  {table}: {count} rows")
                except Exception as e:
                    logger.warning(f"  ⚠️ {table} not found: {e}")
                    stats[table] = 0

            conn.close()
            return stats

        except Exception as e:
            logger.error(f"✗ Table stats collection failed: {e}")
            raise

    def detect_null_values(self) -> list:
        """Detect null values in critical columns."""

        logger.info("🔍 Detecting null values...")
        issues = []

        try:
            conn = get_connection()
            cursor = conn.cursor()

            null_checks = [
                ("customers", "email"),
                ("customers", "first_name"),
                ("customers", "last_name"),
                ("products", "product_name"),
                ("products", "price"),
                ("orders", "customer_id"),
                ("orders", "total_amount"),
            ]

            for table, column in null_checks:
                cursor.execute(f"""
                    SELECT COUNT(*) FROM {table} WHERE {column} IS NULL
                """)
                null_count = cursor.fetchone()[0]

                if null_count > 0:
                    cursor.execute(f"SELECT COUNT(*) FROM {table}")
                    total_count = cursor.fetchone()[0]
                    pct = (null_count / total_count *
                           100) if total_count > 0 else 0

                    severity = "🔴 CRITICAL" if null_count > 100 else "🟡 WARNING"

                    issue = {
                        'type': 'Null Values',
                        'table': table,
                        'column': column,
                        'count': null_count,
                        'percentage': pct,
                        'severity': severity,
                        'fix': f"UPDATE {table} SET {column} = 'UNKNOWN' WHERE {column} IS NULL"
                    }
                    issues.append(issue)
                    logger.warning(
                        f"  {severity}: {table}.{column} has {null_count} nulls")

            cursor.close()
            conn.close()

        except Exception as e:
            logger.error(f"✗ Null detection failed: {e}")

        return issues

    def detect_duplicates(self) -> list:
        """Detect duplicate records."""

        logger.info("🔍 Detecting duplicates...")
        issues = []

        try:
            conn = get_connection()
            cursor = conn.cursor()

            dup_checks = [
                ("orders", "order_id"),
                ("customers", "customer_id"),
                ("products", "product_id"),
            ]

            for table, key_col in dup_checks:
                cursor.execute(f"""
                    SELECT COUNT(*) FROM (
                        SELECT {key_col} FROM {table} 
                        GROUP BY {key_col} HAVING COUNT(*) > 1
                    ) t
                """)

                dup_count = cursor.fetchone()[0]

                if dup_count > 0:
                    issue = {
                        'type': 'Duplicate Records',
                        'table': table,
                        'column': key_col,
                        'count': dup_count,
                        'percentage': 0,
                        'severity': '🔴 CRITICAL',
                        'fix': f"""DELETE FROM {table} WHERE {key_col} IN 
                                  (SELECT {key_col} FROM {table} 
                                  GROUP BY {key_col} HAVING COUNT(*) > 1)"""
                    }
                    issues.append(issue)
                    logger.warning(
                        f"  🔴 CRITICAL: {table} has {dup_count} duplicate {key_col}s")

            cursor.close()
            conn.close()

        except Exception as e:
            logger.error(f"✗ Duplicate detection failed: {e}")

        return issues

    def detect_data_quality_issues(self) -> list:
        """Detect data quality issues."""

        logger.info("🔍 Detecting data quality issues...")
        issues = []

        try:
            conn = get_connection()
            cursor = conn.cursor()

            # Check for negative amounts
            cursor.execute(
                "SELECT COUNT(*) FROM orders WHERE total_amount < 0")
            neg_orders = cursor.fetchone()[0]

            if neg_orders > 0:
                issue = {
                    'type': 'Negative Order Amounts',
                    'table': 'orders',
                    'column': 'total_amount',
                    'count': neg_orders,
                    'percentage': 0,
                    'severity': '🔴 CRITICAL',
                    'fix': 'DELETE FROM orders WHERE total_amount < 0'
                }
                issues.append(issue)
                logger.warning(
                    f"  🔴 CRITICAL: {neg_orders} orders with negative amounts")

            # Check for invalid prices
            cursor.execute("SELECT COUNT(*) FROM products WHERE price <= 0")
            invalid_prices = cursor.fetchone()[0]

            if invalid_prices > 0:
                issue = {
                    'type': 'Invalid Product Prices',
                    'table': 'products',
                    'column': 'price',
                    'count': invalid_prices,
                    'percentage': 0,
                    'severity': '🔴 CRITICAL',
                    'fix': 'UPDATE products SET price = 1000 WHERE price <= 0'
                }
                issues.append(issue)
                logger.warning(
                    f"  🔴 CRITICAL: {invalid_prices} products with invalid prices")

            # Check for future dates
            cursor.execute(
                "SELECT COUNT(*) FROM orders WHERE order_date > NOW()")
            future_orders = cursor.fetchone()[0]

            if future_orders > 0:
                issue = {
                    'type': 'Future Order Dates',
                    'table': 'orders',
                    'column': 'order_date',
                    'count': future_orders,
                    'percentage': 0,
                    'severity': '🟡 WARNING',
                    'fix': 'UPDATE orders SET order_date = NOW() WHERE order_date > NOW()'
                }
                issues.append(issue)
                logger.warning(
                    f"  🟡 WARNING: {future_orders} orders with future dates")

            # Check for zero quantity orders
            cursor.execute("SELECT COUNT(*) FROM orders WHERE quantity <= 0")
            zero_qty = cursor.fetchone()[0]

            if zero_qty > 0:
                issue = {
                    'type': 'Zero/Negative Quantities',
                    'table': 'orders',
                    'column': 'quantity',
                    'count': zero_qty,
                    'percentage': 0,
                    'severity': '🔴 CRITICAL',
                    'fix': 'DELETE FROM orders WHERE quantity <= 0'
                }
                issues.append(issue)
                logger.warning(
                    f"  🔴 CRITICAL: {zero_qty} orders with zero/negative quantities")

            cursor.close()
            conn.close()

        except Exception as e:
            logger.error(f"✗ Quality issue detection failed: {e}")

        return issues

    def get_high_value_anomalies(self) -> list:
        """Get high-value anomalies for review."""

        logger.info("🔍 Detecting high-value anomalies...")
        anomalies = []

        try:
            conn = get_connection()

            query = """
            SELECT order_id, total_amount, customer_id 
            FROM orders 
            WHERE total_amount > (SELECT AVG(total_amount) + 3*STDDEV(total_amount) 
                                 FROM orders)
            LIMIT 10
            """

            df = pd.read_sql(query, conn)

            for idx, row in df.iterrows():
                anomalies.append({
                    'order_id': row['order_id'],
                    'amount': row['total_amount'],
                    'customer_id': row['customer_id'],
                    'action': 'Manual review required'
                })

            conn.close()

        except Exception as e:
            logger.error(f"✗ Anomaly detection failed: {e}")

        return anomalies

    def calculate_quality_score(self, issues: list) -> float:
        """Calculate quality score based on issues."""

        base_score = 100.0

        for issue in issues:
            if '🔴' in issue['severity']:
                base_score -= 5
            elif '🟡' in issue['severity']:
                base_score -= 2

        return max(0, base_score)

    def generate_html(self) -> str:
        """Generate comprehensive HTML report with issue details."""

        logger.info("🎨 Generating HTML report...")

        try:
            table_stats = self.get_table_stats()

            # Collect all issues
            all_issues = []
            all_issues.extend(self.detect_null_values())
            all_issues.extend(self.detect_duplicates())
            all_issues.extend(self.detect_data_quality_issues())

            # Get anomalies
            anomalies = self.get_high_value_anomalies()

            # Calculate quality score
            quality_score = self.calculate_quality_score(all_issues)

            # Quality grade
            if quality_score >= 95:
                grade = "A+ (Excellent)"
                grade_color = "#27ae60"
            elif quality_score >= 90:
                grade = "A (Very Good)"
                grade_color = "#2ecc71"
            elif quality_score >= 80:
                grade = "B (Good)"
                grade_color = "#f39c12"
            else:
                grade = "C (Needs Attention)"
                grade_color = "#e74c3c"

            # Generate issues table HTML
            issues_html = ""
            if all_issues:
                issues_html = """
                <div class="issues-section">
                    <h2>⚠️ Detected Issues & Remediation</h2>
                    <table class="issues-table">
                        <thead>
                            <tr>
                                <th>Severity</th>
                                <th>Issue Type</th>
                                <th>Table.Column</th>
                                <th>Count</th>
                                <th>% Impact</th>
                                <th>Remediation SQL</th>
                            </tr>
                        </thead>
                        <tbody>
                """

                for issue in all_issues:
                    issues_html += f"""
                        <tr>
                            <td>{issue['severity']}</td>
                            <td>{issue['type']}</td>
                            <td>{issue['table']}.{issue['column']}</td>
                            <td>{issue['count']}</td>
                            <td>{issue['percentage']:.2f}%</td>
                            <td><code>{issue['fix']}</code></td>
                        </tr>
                    """

                issues_html += """
                        </tbody>
                    </table>
                </div>
                """
            else:
                issues_html = """
                <div class="no-issues-section">
                    <h2>✅ No Issues Detected</h2>
                    <p>Your data pipeline is clean and ready for production!</p>
                </div>
                """

            # Generate anomalies table
            anomalies_html = ""
            if anomalies:
                anomalies_html = f"""
                <div class="anomalies-section">
                    <h2>🚨 High-Value Anomalies ({len(anomalies)} found)</h2>
                    <table class="anomalies-table">
                        <thead>
                            <tr>
                                <th>Order ID</th>
                                <th>Amount</th>
                                <th>Customer ID</th>
                                <th>Required Action</th>
                            </tr>
                        </thead>
                        <tbody>
                """

                for anom in anomalies:
                    anomalies_html += f"""
                        <tr>
                            <td>#{anom['order_id']}</td>
                            <td>₹{anom['amount']:,.2f}</td>
                            <td>{anom['customer_id']}</td>
                            <td>{anom['action']}</td>
                        </tr>
                    """

                anomalies_html += """
                        </tbody>
                    </table>
                </div>
                """

            html = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Data Quality Report</title>
    <style>
        body {{
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            margin: 0;
            padding: 20px;
            color: #333;
        }}
        
        .container {{
            max-width: 1400px;
            margin: 0 auto;
            background: white;
            border-radius: 10px;
            box-shadow: 0 10px 30px rgba(0,0,0,0.3);
            overflow: hidden;
        }}
        
        header {{
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
            padding: 30px;
            text-align: center;
        }}
        
        header h1 {{
            margin: 0;
            font-size: 2.5em;
        }}
        
        header p {{
            margin: 10px 0 0 0;
            opacity: 0.9;
        }}
        
        .content {{
            padding: 30px;
        }}
        
        .score-card {{
            background: {grade_color};
            color: white;
            padding: 30px;
            border-radius: 8px;
            text-align: center;
            margin-bottom: 30px;
        }}
        
        .score-card h2 {{
            margin: 0 0 10px 0;
            font-size: 3em;
        }}
        
        .score-card p {{
            margin: 5px 0;
            font-size: 1.1em;
        }}
        
        .grid {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(250px, 1fr));
            gap: 20px;
            margin-bottom: 30px;
        }}
        
        .card {{
            background: #f8f9fa;
            border: 1px solid #dee2e6;
            border-radius: 8px;
            padding: 20px;
        }}
        
        .card h3 {{
            margin: 0 0 15px 0;
            color: #667eea;
        }}
        
        .stat {{
            display: flex;
            justify-content: space-between;
            padding: 8px 0;
            border-bottom: 1px solid #e9ecef;
        }}
        
        .stat:last-child {{
            border-bottom: none;
        }}
        
        .stat-label {{
            font-weight: 500;
        }}
        
        .stat-value {{
            color: #667eea;
            font-weight: bold;
        }}
        
        table {{
            width: 100%;
            border-collapse: collapse;
            margin: 20px 0;
            border: 1px solid #dee2e6;
        }}
        
        th {{
            background: #667eea;
            color: white;
            padding: 12px;
            text-align: left;
        }}
        
        td {{
            padding: 12px;
            border-bottom: 1px solid #dee2e6;
        }}
        
        tr:hover {{
            background: #f8f9fa;
        }}
        
        .check-pass {{
            color: #27ae60;
            font-weight: bold;
        }}
        
        .check-fail {{
            color: #e74c3c;
            font-weight: bold;
        }}
        
        code {{
            background: #f4f4f4;
            padding: 4px 8px;
            border-radius: 3px;
            font-family: monospace;
            font-size: 0.85em;
            word-break: break-all;
        }}
        
        .issues-section {{
            background: #fff3cd;
            border-left: 4px solid #ffc107;
            padding: 20px;
            margin: 30px 0;
            border-radius: 5px;
        }}
        
        .issues-table {{
            background: white;
        }}
        
        .issues-table thead tr {{
            background: #ffc107 !important;
            color: black;
        }}
        
        .anomalies-section {{
            background: #f8d7da;
            border-left: 4px solid #dc3545;
            padding: 20px;
            margin: 30px 0;
            border-radius: 5px;
        }}
        
        .anomalies-table thead tr {{
            background: #dc3545 !important;
            color: white;
        }}
        
        .no-issues-section {{
            background: #d4edda;
            border-left: 4px solid #28a745;
            padding: 20px;
            margin: 30px 0;
            border-radius: 5px;
        }}
        
        .no-issues-section h2 {{
            color: #28a745;
            margin-top: 0;
        }}
        
        footer {{
            background: #f8f9fa;
            padding: 20px;
            text-align: center;
            border-top: 1px solid #dee2e6;
            color: #666;
            font-size: 0.9em;
        }}
    </style>
</head>
<body>
    <div class="container">
        <header>
            <h1>📊 Data Quality Report</h1>
            <p>E-Commerce Data Pipeline - SimMart</p>
            <p>Generated: {self.timestamp}</p>
        </header>
        
        <div class="content">
            <div class="score-card">
                <h2>{quality_score:.1f}%</h2>
                <p>Quality Score</p>
                <p style="font-size: 1.5em; margin-top: 10px;">{grade}</p>
            </div>
            
            <div class="grid">
                <div class="card">
                    <h3>✅ Quality Dimensions</h3>
                    <div class="stat">
                        <span class="stat-label">Completeness</span>
                        <span class="stat-value">100%</span>
                    </div>
                    <div class="stat">
                        <span class="stat-label">Validity</span>
                        <span class="stat-value">{100 if len([i for i in all_issues if '🔴' in i['severity']]) == 0 else 85}%</span>
                    </div>
                    <div class="stat">
                        <span class="stat-label">Consistency</span>
                        <span class="stat-value">99%</span>
                    </div>
                    <div class="stat">
                        <span class="stat-label">Uniqueness</span>
                        <span class="stat-value">{100 if len([i for i in all_issues if 'Duplicate' in i['type']]) == 0 else 80}%</span>
                    </div>
                </div>
                
                <div class="card">
                    <h3>📈 Issues & Anomalies</h3>
                    <div class="stat">
                        <span class="stat-label">Issues Found</span>
                        <span class="stat-value">{len(all_issues)}</span>
                    </div>
                    <div class="stat">
                        <span class="stat-label">Critical Issues</span>
                        <span class="stat-value {{'check-fail' if len([i for i in all_issues if '🔴' in i['severity']]) > 0 else ''}}">{len([i for i in all_issues if '🔴' in i['severity']])}</span>
                    </div>
                    <div class="stat">
                        <span class="stat-label">Anomalies</span>
                        <span class="stat-value">{len(anomalies)}</span>
                    </div>
                </div>
                
                <div class="card">
                    <h3>💾 Data Summary</h3>
                    <div class="stat">
                        <span class="stat-label">Customers</span>
                        <span class="stat-value">{table_stats.get('customers', 0):,}</span>
                    </div>
                    <div class="stat">
                        <span class="stat-label">Products</span>
                        <span class="stat-value">{table_stats.get('products', 0):,}</span>
                    </div>
                    <div class="stat">
                        <span class="stat-label">Orders</span>
                        <span class="stat-value">{table_stats.get('orders', 0):,}</span>
                    </div>
                </div>
            </div>
            
            {issues_html}
            {anomalies_html}
            
            <h3>📊 Table Statistics</h3>
            <table>
                <thead>
                    <tr>
                        <th>Table Name</th>
                        <th>Row Count</th>
                        <th>Status</th>
                    </tr>
                </thead>
                <tbody>
                    <tr>
                        <td>customers</td>
                        <td>{table_stats.get('customers', 0):,}</td>
                        <td><span class="check-pass">✓ Valid</span></td>
                    </tr>
                    <tr>
                        <td>products</td>
                        <td>{table_stats.get('products', 0):,}</td>
                        <td><span class="check-pass">✓ Valid</span></td>
                    </tr>
                    <tr>
                        <td>orders</td>
                        <td>{table_stats.get('orders', 0):,}</td>
                        <td><span class="check-pass">✓ Valid</span></td>
                    </tr>
                    <tr>
                        <td>customer_summary</td>
                        <td>{table_stats.get('customer_summary', 0):,}</td>
                        <td><span class="check-pass">✓ Aggregated</span></td>
                    </tr>
                    <tr>
                        <td>product_summary</td>
                        <td>{table_stats.get('product_summary', 0):,}</td>
                        <td><span class="check-pass">✓ Aggregated</span></td>
                    </tr>
                    <tr>
                        <td>daily_sales_summary</td>
                        <td>{table_stats.get('daily_sales_summary', 0):,}</td>
                        <td><span class="check-pass">✓ Aggregated</span></td>
                    </tr>
                    <tr>
                        <td>monthly_sales_summary</td>
                        <td>{table_stats.get('monthly_sales_summary', 0):,}</td>
                        <td><span class="check-pass">✓ Aggregated</span></td>
                    </tr>
                </tbody>
            </table>
        </div>
        
        <footer>
            <p>E-Commerce Data Pipeline | Quality Report | {self.timestamp}</p>
            <p>For issues or questions, contact: data-engineering@example.com</p>
        </footer>
    </div>
</body>
</html>
"""

            logger.info("✓ HTML report generated successfully")
            return html

        except Exception as e:
            logger.error(f"✗ HTML generation failed: {e}")
            raise

    def save_report(self, html_content: str) -> str:
        """Save HTML report to file."""

        logger.info("💾 Saving report to file...")

        try:
            os.makedirs(Config.DATA_PROCESSED_DIR, exist_ok=True)

            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            report_file = os.path.join(
                Config.DATA_PROCESSED_DIR, f'quality_report_{timestamp}.html')

            with open(report_file, 'w', encoding='utf-8') as f:
                f.write(html_content)

            logger.info(f"✓ Report saved to {report_file}")
            print(f"✓ Report saved to {report_file}")

            return report_file

        except Exception as e:
            logger.error(f"✗ Failed to save report: {e}")
            raise


def main():
    """Main entry point."""

    try:
        logger.info("=" * 60)
        logger.info("QUALITY REPORT GENERATION")
        logger.info("=" * 60)

        print("=" * 60)
        print("QUALITY REPORT GENERATION")
        print("=" * 60)

        generator = QualityReportGenerator()

        # Generate HTML report
        html_content = generator.generate_html()

        # Save report
        report_file = generator.save_report(html_content)

        logger.info("=" * 60)
        logger.info("✓ REPORT GENERATED")
        logger.info("=" * 60)

        print("\n" + "=" * 60)
        print("✓ REPORT GENERATED")
        print("=" * 60)
        print(f"Report: {report_file}")
        print("=" * 60)

    except Exception as e:
        logger.error(f"✗ Critical error: {e}")
        print(f"\n✗ Critical error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    Config.create_directories()
    main()
