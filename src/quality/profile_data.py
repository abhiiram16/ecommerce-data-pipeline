"""
Data Profiling
==============

Statistical profiling of e-commerce data.

Generates:
- Row counts
- Unique value counts
- Data type information
- Sample values

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
    f"{Config.LOGS_DIR}/profiling_{{time:YYYY-MM-DD}}.log",
    level=Config.LOG_LEVEL
)

# ========================================
# PROFILING FUNCTIONS
# ========================================


def profile_table(table_name: str) -> dict:
    """Profile a single table."""

    logger.info(f"📊 Profiling {table_name}...")

    try:
        conn = get_connection()
        cursor = conn.cursor()

        profile = {
            'table_name': table_name,
            'timestamp': datetime.now().isoformat()
        }

        # Row count
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        profile['total_rows'] = cursor.fetchone()[0]
        logger.info(f"  Total rows: {profile['total_rows']:,}")

        # Column information
        cursor.execute(f"""
            SELECT column_name, data_type, is_nullable
            FROM information_schema.columns
            WHERE table_name = %s
            ORDER BY ordinal_position
        """, (table_name,))

        profile['columns'] = []
        for col_name, data_type, nullable in cursor.fetchall():
            # Count unique values
            cursor.execute(
                f"SELECT COUNT(DISTINCT {col_name}) FROM {table_name}")
            unique_count = cursor.fetchone()[0]

            # Count nulls
            cursor.execute(
                f"SELECT COUNT(*) FROM {table_name} WHERE {col_name} IS NULL")
            null_count = cursor.fetchone()[0]

            column_info = {
                'name': col_name,
                'type': data_type,
                'nullable': nullable == 'YES',
                'unique_values': unique_count,
                'null_count': null_count,
                'null_percentage': round((null_count / profile['total_rows'] * 100), 2) if profile['total_rows'] > 0 else 0
            }

            profile['columns'].append(column_info)
            logger.info(
                f"    {col_name} ({data_type}): {unique_count} unique, {null_count} nulls")

        cursor.close()
        conn.close()

        return profile

    except Exception as e:
        logger.error(f"✗ Table profiling failed for {table_name}: {e}")
        raise


def print_profile(profile: dict) -> None:
    """Print profile information."""

    print(f"\n📊 TABLE: {profile['table_name'].upper()}")
    print("-" * 60)
    print(f"Total Rows: {profile['total_rows']:,}")
    print(f"Timestamp: {profile['timestamp']}")
    print(f"\nColumns:")
    print(f"{'Name':<20} {'Type':<15} {'Unique':<10} {'Nulls':<10}")
    print("-" * 60)

    for col in profile['columns']:
        print(
            f"{col['name']:<20} {col['type']:<15} {col['unique_values']:<10} {col['null_count']:<10}")


def main():
    """Main entry point."""

    logger.info("=" * 60)
    logger.info("DATA PROFILING")
    logger.info("=" * 60)

    print("=" * 60)
    print("DATA PROFILING")
    print("=" * 60)

    try:
        tables = [
            'customers',
            'products',
            'orders',
            'customer_summary',
            'product_summary',
            'daily_sales_summary',
            'monthly_sales_summary'
        ]

        profiles = []

        for table in tables:
            try:
                profile = profile_table(table)
                profiles.append(profile)
                print_profile(profile)
            except Exception as e:
                logger.warning(f"⚠️ Could not profile {table}: {e}")
                print(f"\n⚠️ Could not profile {table}")

        logger.info("=" * 60)
        logger.info("✓ PROFILING COMPLETE")
        logger.info("=" * 60)

        print("\n" + "=" * 60)
        print("✓ PROFILING COMPLETE")
        print("=" * 60)

    except Exception as e:
        logger.error(f"✗ Critical error: {e}")
        print(f"\n✗ Critical error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    Config.create_directories()
    main()
