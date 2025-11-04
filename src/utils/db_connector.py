"""
Database Connection Utility
===========================
Provides reusable PostgreSQL connection functions.

Author: Abhiiram
Date: November 5, 2025
"""

import psycopg2
from psycopg2 import sql, OperationalError
import sys

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

# ============================================
# CONNECTION FUNCTIONS
# ============================================


def get_connection():
    """
    Create and return a PostgreSQL database connection.

    Returns:
    --------
    psycopg2.connection
        Database connection object

    Raises:
    -------
    OperationalError
        If connection cannot be established
    """
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        return conn
    except OperationalError as e:
        print(f"❌ ERROR: Cannot connect to database!")
        print(f"   Details: {e}")
        print(f"\n💡 TROUBLESHOOTING:")
        print(f"   1. Is Docker container running? Check: docker ps")
        print(f"   2. Is PostgreSQL accessible? Check: docker logs ecommerce_postgres")
        print(
            f"   3. Are credentials correct? User: {DB_CONFIG['user']}, DB: {DB_CONFIG['database']}")
        sys.exit(1)


def test_connection():
    """
    Test database connection and print status.

    Returns:
    --------
    bool
        True if connection successful, False otherwise
    """
    try:
        conn = get_connection()
        cursor = conn.cursor()

        # Test query
        cursor.execute("SELECT version();")
        version = cursor.fetchone()[0]

        print("✅ Database connection successful!")
        print(f"   PostgreSQL version: {version[:50]}...")

        cursor.close()
        conn.close()

        return True

    except Exception as e:
        print(f"❌ Database connection failed: {e}")
        return False


def execute_query(query, params=None, fetch=False):
    """
    Execute a SQL query with optional parameters.

    Parameters:
    -----------
    query : str
        SQL query to execute
    params : tuple, optional
        Query parameters (for parameterized queries)
    fetch : bool, optional
        If True, fetch and return results

    Returns:
    --------
    list or None
        Query results if fetch=True, None otherwise
    """
    conn = get_connection()
    cursor = conn.cursor()

    try:
        cursor.execute(query, params)

        if fetch:
            results = cursor.fetchall()
            conn.close()
            return results
        else:
            conn.commit()
            conn.close()
            return None

    except Exception as e:
        conn.rollback()
        conn.close()
        raise e


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
# MODULE TEST
# ============================================

if __name__ == "__main__":
    """
    Test database connection when script is run directly.
    """
    print("="*60)
    print("DATABASE CONNECTION TEST")
    print("="*60)
    print(f"\nAttempting to connect to:")
    print(f"  Host: {DB_CONFIG['host']}")
    print(f"  Port: {DB_CONFIG['port']}")
    print(f"  Database: {DB_CONFIG['database']}")
    print(f"  User: {DB_CONFIG['user']}")
    print("="*60)

    test_connection()
# ============================================
