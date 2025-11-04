"""
CSV to PostgreSQL Data Loader (Standalone Version)
===================================================
Loads CSV files (customers, products, orders) into PostgreSQL database.

Author: Abhiiram
Date: November 5, 2025
"""

import pandas as pd
import psycopg2
from psycopg2 import sql
import sys
import os
from datetime import datetime

# ============================================
# DATABASE CONFIGURATION
# ============================================

DB_CONFIG = {
    'host': 'localhost',
    'port': 5432,
    'database': 'ecommerce_db',
    'user': 'dataeng',
    'password': 'pipeline123'
}

DATA_DIR = 'data/raw'
CSV_FILES = {
    'customers': 'customers.csv',
    'products': 'products.csv',
    'orders': 'orders.csv'
}

# ============================================
# DATABASE CONNECTION FUNCTIONS
# ============================================


def get_connection():
    """
    Create and return a PostgreSQL database connection.

    Returns:
    --------
    psycopg2.connection
        Database connection object
    """
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        return conn
    except psycopg2.OperationalError as e:
        print(f"❌ ERROR: Cannot connect to database!")
        print(f"   Details: {e}")
        print(f"\n💡 TROUBLESHOOTING:")
        print(f"   1. Is Docker container running? Check: docker ps")
        print(f"   2. Is PostgreSQL accessible? Check: docker logs ecommerce_postgres")
        print(f"   3. Are credentials correct?")
        sys.exit(1)


def get_table_row_count(table_name):
    """
    Get row count for a specific table.

    Parameters:
    -----------
    table_name : str
        Name of the table

    Returns:
    --------
    int
        Number of rows in table
    """
    query = sql.SQL(
        "SELECT COUNT(*) FROM {}").format(sql.Identifier(table_name))

    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute(query)
    count = cursor.fetchone()[0]
    cursor.close()
    conn.close()

    return count


# ============================================
# DATA LOADING FUNCTIONS
# ============================================

def truncate_table(table_name):
    """
    Truncate (clear) a table before loading new data.

    Parameters:
    -----------
    table_name : str
        Name of table to truncate
    """
    conn = get_connection()
    cursor = conn.cursor()

    try:
        # Use CASCADE to handle foreign key dependencies
        query = sql.SQL("TRUNCATE TABLE {} CASCADE").format(
            sql.Identifier(table_name))
        cursor.execute(query)
        conn.commit()
        print(f"  ✓ Truncated table: {table_name}")

    except Exception as e:
        conn.rollback()
        print(f"  ❌ Error truncating {table_name}: {e}")
        raise
    finally:
        cursor.close()
        conn.close()


def load_csv_to_table(csv_file, table_name, batch_size=1000):
    """
    Load CSV file into PostgreSQL table using batch inserts.

    Parameters:
    -----------
    csv_file : str
        Path to CSV file
    table_name : str
        Name of target table
    batch_size : int
        Number of rows to insert per batch (default: 1000)

    Returns:
    --------
    int
        Number of rows inserted
    """
    # Read CSV file
    print(f"\n📂 Reading CSV file: {csv_file}")
    df = pd.read_csv(csv_file)
    print(f"  ✓ Loaded {len(df):,} rows from CSV")

    # Get column names
    columns = df.columns.tolist()

    # Create INSERT query
    placeholders = ', '.join(['%s'] * len(columns))
    columns_str = ', '.join([f'"{col}"' for col in columns])
    insert_query = f"INSERT INTO {table_name} ({columns_str}) VALUES ({placeholders})"

    # Connect to database
    conn = get_connection()
    cursor = conn.cursor()

    rows_inserted = 0

    try:
        print(f"  → Inserting data in batches of {batch_size}...")

        # Insert data in batches
        for i in range(0, len(df), batch_size):
            batch = df.iloc[i:i+batch_size]

            # Convert DataFrame batch to list of tuples
            data = [tuple(row) for row in batch.values]

            # Execute batch insert
            cursor.executemany(insert_query, data)
            conn.commit()

            rows_inserted += len(batch)

            # Progress indicator
            if (i + batch_size) % 5000 == 0 or i + batch_size >= len(df):
                print(
                    f"  → Inserted {rows_inserted:,} / {len(df):,} rows ({(rows_inserted/len(df)*100):.1f}%)")

        print(
            f"  ✅ Successfully loaded {rows_inserted:,} rows into {table_name}")

    except Exception as e:
        conn.rollback()
        print(f"  ❌ Error loading data into {table_name}: {e}")
        raise
    finally:
        cursor.close()
        conn.close()

    return rows_inserted


def verify_data_load():
    """
    Verify that data was loaded correctly by checking row counts.
    """
    print("\n" + "="*60)
    print("DATA VERIFICATION")
    print("="*60)

    for table_name in ['customers', 'products', 'orders']:
        count = get_table_row_count(table_name)
        print(f"  {table_name:15} : {count:,} rows")

    # Verify foreign key relationships
    print("\n🔗 Checking foreign key relationships...")
    conn = get_connection()
    cursor = conn.cursor()

    # Check for orphaned orders
    cursor.execute("""
        SELECT COUNT(*) 
        FROM orders o
        LEFT JOIN customers c ON o.customer_id = c.customer_id
        WHERE c.customer_id IS NULL
    """)
    orphaned_customers = cursor.fetchone()[0]

    cursor.execute("""
        SELECT COUNT(*) 
        FROM orders o
        LEFT JOIN products p ON o.product_id = p.product_id
        WHERE p.product_id IS NULL
    """)
    orphaned_products = cursor.fetchone()[0]

    cursor.close()
    conn.close()

    if orphaned_customers == 0 and orphaned_products == 0:
        print("  ✅ All foreign key relationships are valid")
    else:
        print(
            f"  ⚠️  Found {orphaned_customers} orders with invalid customer_id")
        print(
            f"  ⚠️  Found {orphaned_products} orders with invalid product_id")


# ============================================
# MAIN EXECUTION
# ============================================

def main():
    """
    Main execution function - orchestrates entire data load process.
    """
    start_time = datetime.now()

    print("\n" + "="*60)
    print("CSV TO POSTGRESQL DATA LOADER")
    print("="*60)
    print(f"Start time: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*60)

    # Step 1: Clear existing data
    print("\n[1/4] TRUNCATING EXISTING DATA")
    print("-"*60)
    # Order matters (foreign keys)
    for table in ['orders', 'products', 'customers']:
        truncate_table(table)

    # Step 2: Load customers
    print("\n[2/4] LOADING CUSTOMERS")
    print("-"*60)
    customers_file = os.path.join(DATA_DIR, CSV_FILES['customers'])
    load_csv_to_table(customers_file, 'customers')

    # Step 3: Load products
    print("\n[3/4] LOADING PRODUCTS")
    print("-"*60)
    products_file = os.path.join(DATA_DIR, CSV_FILES['products'])
    load_csv_to_table(products_file, 'products')

    # Step 4: Load orders
    print("\n[4/4] LOADING ORDERS")
    print("-"*60)
    orders_file = os.path.join(DATA_DIR, CSV_FILES['orders'])
    load_csv_to_table(orders_file, 'orders')

    # Verify data
    verify_data_load()

    # Summary
    end_time = datetime.now()
    duration = (end_time - start_time).total_seconds()

    total_rows = (get_table_row_count('customers') +
                  get_table_row_count('products') +
                  get_table_row_count('orders'))

    print("\n" + "="*60)
    print("✅ DATA LOAD COMPLETE!")
    print("="*60)
    print(f"  End time: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"  Duration: {duration:.2f} seconds")
    print(f"  Records loaded: {total_rows:,}")
    print("="*60)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n⚠️  Data load interrupted by user (Ctrl+C)")
        print("Exiting...")
    except Exception as e:
        print(f"\n\n❌ ERROR: Data load failed!")
        print(f"Error details: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
