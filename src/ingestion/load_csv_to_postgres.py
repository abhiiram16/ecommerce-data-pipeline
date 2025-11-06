"""
CSV to PostgreSQL Data Loader (Incremental Mode with Chunked Reading)
======================================================================

Loads CSV files to PostgreSQL with:
- Incremental append (no truncate)
- Memory-efficient chunked reading (Option 2)
- Duplicate handling with UPSERT
- Cumulative data support
- Foreign key validation
- Progress tracking
- Auto-incrementing random seed

Author: Abhiiram
Date: November 7, 2025
"""

from src.utils.db_connector import get_connection
from src.utils.config import Config
import psycopg2
from psycopg2.extras import execute_values
import pandas as pd
import sys
import os
from datetime import datetime
from loguru import logger
import time

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../../'))

# Configure logging
logger.add(
    f"{Config.LOGS_DIR}/ingestion_{{time:YYYY-MM-DD}}.log",
    level=Config.LOG_LEVEL
)

# Auto-incrementing seed (changes every run)
RANDOM_SEED = int(os.getenv('RANDOM_SEED', int(time.time()) % 100000))

# ========================================
# CSV TO POSTGRESQL LOADER (CHUNKED - OPTION 2)
# ========================================


def load_csv_to_table(table_name: str, csv_file: str, batch_size: int = 1000) -> int:
    """Load CSV in memory-efficient chunks with UPSERT."""

    logger.info(f"📂 Reading CSV file (chunked): {csv_file}")
    print(f"📂 Reading CSV file (chunked): {csv_file}")

    try:
        # Get existing count before loading
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        existing_count = cursor.fetchone()[0]
        cursor.close()
        conn.close()

        logger.info(f"  Current rows in {table_name}: {existing_count:,}")
        print(f"  Current rows in {table_name}: {existing_count:,}")

        conn = get_connection()
        cursor = conn.cursor()

        total_inserted = 0
        chunk_num = 0

        # Read CSV in chunks - memory efficient (Option 2)
        logger.info(f"→ Processing data in batches of {batch_size}...")
        print(f"→ Processing data in batches of {batch_size}...")

        for chunk in pd.read_csv(csv_file, chunksize=batch_size):
            chunk_num += 1
            values = [tuple(row) for row in chunk.values]

            placeholders = ', '.join(['%s'] * len(chunk.columns))
            col_str = ', '.join([f'"{col}"' for col in chunk.columns])

            # Determine UPSERT logic based on table
            if table_name == 'customers':
                conflict_col = 'customer_id'
                update_cols = ', '.join([
                    f'"{col}" = EXCLUDED."{col}"'
                    for col in chunk.columns if col != 'customer_id'
                ])
                upsert_clause = f"""
                ON CONFLICT ({conflict_col}) 
                DO UPDATE SET {update_cols}
                """

            elif table_name == 'products':
                conflict_col = 'product_id'
                update_cols = ', '.join([
                    f'"{col}" = EXCLUDED."{col}"'
                    for col in chunk.columns if col != 'product_id'
                ])
                upsert_clause = f"""
                ON CONFLICT ({conflict_col}) 
                DO UPDATE SET {update_cols}
                """

            elif table_name == 'orders':
                # Orders: skip duplicates (don't update)
                upsert_clause = """
                ON CONFLICT (order_id) 
                DO NOTHING
                """

            else:
                upsert_clause = ""

            # Build complete INSERT query with UPSERT
            query = f"""
            INSERT INTO {table_name} ({col_str}) 
            VALUES %s
            {upsert_clause}
            """

            try:
                execute_values(cursor, query, values, page_size=len(values))
                total_inserted += len(chunk)

                pct = (total_inserted / len(pd.read_csv(csv_file))) * 100
                logger.info(
                    f"→ Batch {chunk_num}: {total_inserted:,} rows processed")
                print(
                    f"→ Batch {chunk_num}: {total_inserted:,} rows processed")

            except Exception as e:
                logger.error(f"✗ Batch insert failed: {e}")
                conn.rollback()
                raise

        conn.commit()

        # Get final count
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        final_count = cursor.fetchone()[0]
        added = final_count - existing_count

        logger.info(
            f"✓ Successfully loaded {added:,} NEW rows into {table_name}")
        logger.info(f"  Total rows in {table_name}: {final_count:,}")
        print(f"✓ Successfully added {added:,} NEW rows to {table_name}")
        print(f"  Total rows in {table_name}: {final_count:,}")

        cursor.close()
        conn.close()

        return added

    except Exception as e:
        logger.error(f"✗ Load failed for {table_name}: {e}")
        print(f"✗ Load failed for {table_name}: {e}")
        raise


def verify_data_load():
    """Verify data load with cumulative counts and relationships."""

    logger.info("=" * 60)
    logger.info("DATA VERIFICATION")
    logger.info("=" * 60)

    print("\n" + "=" * 60)
    print("DATA VERIFICATION")
    print("=" * 60)

    try:
        conn = get_connection()
        cursor = conn.cursor()

        # Get table counts
        tables = ['customers', 'products', 'orders']
        counts = {}

        for table in tables:
            cursor.execute(f"SELECT COUNT(*) FROM {table}")
            count = cursor.fetchone()[0]
            counts[table] = count
            logger.info(f"{table:15s}: {count:,} rows")
            print(f"{table:15s}: {count:,} rows")

        # Verify foreign keys
        logger.info("🔗 Checking foreign key relationships...")
        print("\n🔗 Checking foreign key relationships...")

        # Check orphaned orders (orders with non-existent customer_id)
        cursor.execute("""
            SELECT COUNT(*) FROM orders o 
            WHERE NOT EXISTS (
                SELECT 1 FROM customers c WHERE c.customer_id = o.customer_id
            )
        """)
        orphaned_customers = cursor.fetchone()[0]

        if orphaned_customers > 0:
            logger.warning(
                f"⚠️ {orphaned_customers} orders with non-existent customers")
            print(f"⚠️ {orphaned_customers} orders with non-existent customers")

        # Check orphaned products
        cursor.execute("""
            SELECT COUNT(*) FROM orders o 
            WHERE NOT EXISTS (
                SELECT 1 FROM products p WHERE p.product_id = o.product_id
            )
        """)
        orphaned_products = cursor.fetchone()[0]

        if orphaned_products > 0:
            logger.warning(
                f"⚠️ {orphaned_products} orders with non-existent products")
            print(f"⚠️ {orphaned_products} orders with non-existent products")

        if orphaned_customers == 0 and orphaned_products == 0:
            logger.info("✓ All foreign key relationships are valid")
            print("✓ All foreign key relationships are valid")

        # Calculate cumulative revenue
        cursor.execute("SELECT SUM(total_amount) FROM orders")
        total_revenue = cursor.fetchone()[0] or 0

        cursor.execute("SELECT COUNT(*) FROM orders")
        total_orders = cursor.fetchone()[0]

        logger.info(f"💰 Total Revenue: ₹{total_revenue:,.2f}")
        logger.info(f"📦 Total Orders: {total_orders:,}")
        print(f"💰 Total Revenue: ₹{total_revenue:,.2f}")
        print(f"📦 Total Orders: {total_orders:,}")

        cursor.close()
        conn.close()

    except Exception as e:
        logger.error(f"✗ Verification failed: {e}")
        print(f"✗ Verification failed: {e}")
        raise


def main():
    """Main entry point for incremental data loading."""

    logger.info("=" * 60)
    logger.info("CSV TO POSTGRESQL DATA LOADER (INCREMENTAL MODE)")
    logger.info("=" * 60)

    print("=" * 60)
    print("CSV TO POSTGRESQL DATA LOADER (INCREMENTAL MODE)")
    print("=" * 60)
    print("Start time:", datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
    print("=" * 60)

    try:
        logger.info("[1/4] LOADING CUSTOMERS (with duplicate handling)")
        print("\n[1/4] LOADING CUSTOMERS (with duplicate handling)")
        print("-" * 60)

        customers_added = load_csv_to_table(
            'customers',
            os.path.join(Config.DATA_RAW_DIR, 'customers.csv')
        )

        logger.info("[2/4] LOADING PRODUCTS (with duplicate handling)")
        print("\n[2/4] LOADING PRODUCTS (with duplicate handling)")
        print("-" * 60)

        products_added = load_csv_to_table(
            'products',
            os.path.join(Config.DATA_RAW_DIR, 'products.csv')
        )

        logger.info("[3/4] LOADING ORDERS (appending new orders)")
        print("\n[3/4] LOADING ORDERS (appending new orders)")
        print("-" * 60)

        orders_added = load_csv_to_table(
            'orders',
            os.path.join(Config.DATA_RAW_DIR, 'orders.csv')
        )

        # Verify
        logger.info("[4/4] VERIFYING DATA LOAD")
        print("\n[4/4] VERIFYING DATA LOAD")
        print("-" * 60)

        verify_data_load()

        logger.info("=" * 60)
        logger.info("✓ DATA LOAD COMPLETE!")
        logger.info("=" * 60)
        logger.info(f"New records added:")
        logger.info(f"  - Customers: {customers_added:,}")
        logger.info(f"  - Products: {products_added:,}")
        logger.info(f"  - Orders: {orders_added:,}")
        logger.info(
            f"End time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

        print("\n" + "=" * 60)
        print("✓ DATA LOAD COMPLETE!")
        print("=" * 60)
        print(f"New records added:")
        print(f"  - Customers: {customers_added:,}")
        print(f"  - Products: {products_added:,}")
        print(f"  - Orders: {orders_added:,}")
        print(f"End time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("=" * 60)

    except Exception as e:
        logger.error(f"✗ Data load failed: {e}")
        print(f"\n✗ Data load failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
