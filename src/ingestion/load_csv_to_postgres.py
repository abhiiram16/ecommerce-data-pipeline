"""
CSV to PostgreSQL Data Loader
=============================

Loads CSV files (customers, products, orders) into PostgreSQL database.

Author: Abhiiram
Date: November 6, 2025
"""

from src.utils.db_connector import get_connection, truncate_table
from src.utils.config import Config, get_db_config
import pandas as pd
import psycopg2
from psycopg2 import sql
import sys
import os
from datetime import datetime
from loguru import logger

# Add parent directory to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../../'))


# Configure logging
logger.add(
    f"{Config.LOGS_DIR}/ingestion_{{time:YYYY-MM-DD}}.log",
    level=Config.LOG_LEVEL
)

# ========================================
# CONFIGURATION
# ========================================

DATA_DIR = Config.DATA_RAW_DIR

CSV_FILES = {
    'customers': 'customers.csv',
    'products': 'products.csv',
    'orders': 'orders.csv'
}

# ========================================
# FUNCTIONS
# ========================================


def get_table_row_count(table_name: str) -> int:
    """Get row count for a specific table."""
    try:
        query = sql.SQL(
            "SELECT COUNT(*) FROM {}").format(sql.Identifier(table_name))
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute(query)
        count = cursor.fetchone()[0]
        cursor.close()
        conn.close()
        return count
    except Exception as e:
        logger.error(f"✗ Row count error: {e}")
        raise


def load_csv_to_table(csv_file: str, table_name: str, batch_size: int = 1000) -> int:
    """Load CSV file into PostgreSQL table using batch inserts."""

    logger.info(f"📂 Reading CSV file: {csv_file}")

    try:
        # Read CSV
        df = pd.read_csv(csv_file)
        logger.info(f"✓ Loaded {len(df):,} rows from CSV")
        print(f"✓ Loaded {len(df):,} rows from CSV")

        # Get column names
        columns = df.columns.tolist()

        # Create INSERT query
        placeholders = ', '.join(['%s'] * len(columns))
        columns_str = ', '.join([f'"{col}"' for col in columns])
        insert_query = f"INSERT INTO {table_name} ({columns_str}) VALUES ({placeholders})"

        # Connect and insert
        conn = get_connection()
        cursor = conn.cursor()

        rows_inserted = 0

        logger.info(f"→ Inserting data in batches of {batch_size}...")
        print(f"→ Inserting data in batches of {batch_size}...")

        for i in range(0, len(df), batch_size):
            batch = df.iloc[i:i+batch_size]
            data = [tuple(row) for row in batch.values]

            cursor.executemany(insert_query, data)
            conn.commit()
            rows_inserted += len(batch)

            if (i + batch_size) % 5000 == 0 or i + batch_size >= len(df):
                progress_pct = (rows_inserted/len(df)*100)
                logger.info(
                    f"→ Inserted {rows_inserted:,} / {len(df):,} rows ({progress_pct:.1f}%)")
                print(
                    f"→ Inserted {rows_inserted:,} / {len(df):,} rows ({progress_pct:.1f}%)")

        cursor.close()
        conn.close()

        logger.info(
            f"✓ Successfully loaded {rows_inserted:,} rows into {table_name}")
        print(
            f"✓ Successfully loaded {rows_inserted:,} rows into {table_name}")

        return rows_inserted

    except Exception as e:
        logger.error(f"✗ Error loading data into {table_name}: {e}")
        raise


def verify_data_load() -> bool:
    """Verify that data was loaded correctly."""

    logger.info("=" * 60)
    logger.info("DATA VERIFICATION")
    print("\n" + "=" * 60)
    print("DATA VERIFICATION")
    print("=" * 60)

    try:
        for table_name in ['customers', 'products', 'orders']:
            count = get_table_row_count(table_name)
            logger.info(f"{table_name:15} : {count:,} rows")
            print(f" {table_name:15} : {count:,} rows")

        # Verify foreign key relationships
        logger.info("🔗 Checking foreign key relationships...")
        print("\n🔗 Checking foreign key relationships...")

        conn = get_connection()
        cursor = conn.cursor()

        cursor.execute("""
            SELECT COUNT(*) FROM orders o
            LEFT JOIN customers c ON o.customer_id = c.customer_id
            WHERE c.customer_id IS NULL
        """)
        orphaned_customers = cursor.fetchone()[0]

        cursor.execute("""
            SELECT COUNT(*) FROM orders o
            LEFT JOIN products p ON o.product_id = p.product_id
            WHERE p.product_id IS NULL
        """)
        orphaned_products = cursor.fetchone()[0]

        cursor.close()
        conn.close()

        if orphaned_customers == 0 and orphaned_products == 0:
            logger.info("✓ All foreign key relationships are valid")
            print(" ✓ All foreign key relationships are valid")
            return True
        else:
            logger.warning(
                f"⚠️ Found {orphaned_customers} orders with invalid customer_id")
            logger.warning(
                f"⚠️ Found {orphaned_products} orders with invalid product_id")
            print(
                f" ⚠️ Found {orphaned_customers} orders with invalid customer_id")
            print(
                f" ⚠️ Found {orphaned_products} orders with invalid product_id")
            return False

    except Exception as e:
        logger.error(f"✗ Verification failed: {e}")
        raise


def main():
    """Main execution function."""

    start_time = datetime.now()

    logger.info("=" * 60)
    logger.info("CSV TO POSTGRESQL DATA LOADER")
    logger.info("=" * 60)

    print("\n" + "=" * 60)
    print("CSV TO POSTGRESQL DATA LOADER")
    print("=" * 60)
    print(f"Start time: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)

    try:
        # Step 1: Truncate tables
        logger.info("[1/4] TRUNCATING EXISTING DATA")
        print("\n[1/4] TRUNCATING EXISTING DATA")
        print("-" * 60)

        for table in ['orders', 'products', 'customers']:
            truncate_table(table)

        # Step 2: Load customers
        logger.info("[2/4] LOADING CUSTOMERS")
        print("\n[2/4] LOADING CUSTOMERS")
        print("-" * 60)

        customers_file = os.path.join(DATA_DIR, CSV_FILES['customers'])
        load_csv_to_table(customers_file, 'customers')

        # Step 3: Load products
        logger.info("[3/4] LOADING PRODUCTS")
        print("\n[3/4] LOADING PRODUCTS")
        print("-" * 60)

        products_file = os.path.join(DATA_DIR, CSV_FILES['products'])
        load_csv_to_table(products_file, 'products')

        # Step 4: Load orders
        logger.info("[4/4] LOADING ORDERS")
        print("\n[4/4] LOADING ORDERS")
        print("-" * 60)

        orders_file = os.path.join(DATA_DIR, CSV_FILES['orders'])
        load_csv_to_table(orders_file, 'orders')

        # Verify
        verify_data_load()

        # Summary
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        total_rows = (get_table_row_count('customers') +
                      get_table_row_count('products') +
                      get_table_row_count('orders'))

        logger.info("=" * 60)
        logger.info("✓ DATA LOAD COMPLETE!")
        logger.info("=" * 60)
        logger.info(f"End time: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info(f"Duration: {duration:.2f} seconds")
        logger.info(f"Records loaded: {total_rows:,}")

        print("\n" + "=" * 60)
        print("✓ DATA LOAD COMPLETE!")
        print("=" * 60)
        print(f" End time: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f" Duration: {duration:.2f} seconds")
        print(f" Records loaded: {total_rows:,}")
        print("=" * 60)

    except KeyboardInterrupt:
        logger.warning("⚠️ Data load interrupted by user")
        print("\n⚠️ Data load interrupted by user")
    except Exception as e:
        logger.error(f"✗ Data load failed: {e}")
        print(f"\n✗ Data load failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    Config.create_directories()
    main()
