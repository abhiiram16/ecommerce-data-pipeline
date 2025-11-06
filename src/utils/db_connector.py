"""
Database Connection Utility
===========================

Provides reusable PostgreSQL connection functions with error handling.

Uses centralized Config from config.py

Location: src/utils/db_connector.py
Author: Abhiiram
Date: November 6, 2025
"""

import psycopg2
from psycopg2 import sql, OperationalError
from loguru import logger
from typing import Optional, Any, List
import sys
import os

# Import config from same directory
from config import Config, get_db_config

# Configure logging
logger.add(
    f"{Config.LOGS_DIR}/db_connector_{{time:YYYY-MM-DD}}.log",
    level=Config.LOG_LEVEL
)

# ========================================
# CONNECTION FUNCTIONS
# ========================================


def get_connection() -> psycopg2.extensions.connection:
    """
    Create and return a PostgreSQL database connection.

    Returns:
        psycopg2.connection: Database connection object

    Raises:
        OperationalError: If connection cannot be established
    """
    try:
        db_config = get_db_config()
        conn = psycopg2.connect(**db_config)
        logger.info(
            f"✓ Connected to {db_config['database']}@{db_config['host']}")
        return conn

    except OperationalError as e:
        logger.error(f"✗ Database connection failed: {e}")
        raise
    except Exception as e:
        logger.error(f"✗ Unexpected error: {e}")
        raise


def execute_query(query: str, params: tuple = None) -> Any:
    """Execute a query and return results."""
    conn = None
    try:
        conn = get_connection()
        cursor = conn.cursor()

        if params:
            cursor.execute(query, params)
        else:
            cursor.execute(query)

        result = cursor.fetchall()
        cursor.close()
        conn.close()

        logger.info(f"✓ Query executed - {len(result)} rows")
        return result

    except Exception as e:
        logger.error(f"✗ Query failed: {e}")
        if conn:
            conn.close()
        raise


def execute_insert(table: str, data: dict) -> bool:
    """Insert single record into table."""
    conn = None
    try:
        conn = get_connection()
        cursor = conn.cursor()

        columns = data.keys()
        values = tuple(data.values())

        query = sql.SQL("INSERT INTO {} ({}) VALUES ({})").format(
            sql.Identifier(table),
            sql.SQL(', ').join(map(sql.Identifier, columns)),
            sql.SQL(', ').join(sql.Placeholder() * len(columns))
        )

        cursor.execute(query, values)
        conn.commit()
        cursor.close()
        conn.close()

        logger.info(f"✓ Inserted into {table}")
        return True

    except Exception as e:
        logger.error(f"✗ Insert failed: {e}")
        if conn:
            conn.rollback()
            conn.close()
        raise


def execute_batch_insert(table: str, data_list: list, batch_size: int = 1000) -> int:
    """Batch insert multiple records."""
    conn = None
    try:
        conn = get_connection()
        cursor = conn.cursor()

        if not data_list:
            return 0

        columns = data_list[0].keys()
        total_rows = 0

        for i in range(0, len(data_list), batch_size):
            batch = data_list[i:i+batch_size]

            for data in batch:
                values = tuple(data.values())
                query = sql.SQL("INSERT INTO {} ({}) VALUES ({})").format(
                    sql.Identifier(table),
                    sql.SQL(', ').join(map(sql.Identifier, columns)),
                    sql.SQL(', ').join(sql.Placeholder() * len(columns))
                )
                cursor.execute(query, values)

            conn.commit()
            total_rows += len(batch)
            logger.info(
                f"✓ Batch inserted {len(batch)} rows - Total: {total_rows}")

        cursor.close()
        conn.close()

        logger.info(f"✓ Batch insert complete - {total_rows} total rows")
        return total_rows

    except Exception as e:
        logger.error(f"✗ Batch insert failed: {e}")
        if conn:
            conn.rollback()
            conn.close()
        raise


def table_exists(table_name: str) -> bool:
    """Check if table exists in database."""
    try:
        conn = get_connection()
        cursor = conn.cursor()

        cursor.execute("""
            SELECT EXISTS(
                SELECT 1 FROM information_schema.tables 
                WHERE table_name = %s AND table_schema = 'public'
            )
        """, (table_name,))

        exists = cursor.fetchone()[0]
        cursor.close()
        conn.close()

        logger.info(f"✓ Table {table_name} exists: {exists}")
        return exists

    except Exception as e:
        logger.error(f"✗ Table check failed: {e}")
        raise


def get_row_count(table_name: str) -> int:
    """Get number of rows in table."""
    try:
        conn = get_connection()
        cursor = conn.cursor()

        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        count = cursor.fetchone()[0]
        cursor.close()
        conn.close()

        logger.info(f"✓ {table_name}: {count} rows")
        return count

    except Exception as e:
        logger.error(f"✗ Row count failed for {table_name}: {e}")
        raise


def truncate_table(table_name: str) -> bool:
    """Truncate (clear) a table."""
    conn = None
    try:
        conn = get_connection()
        cursor = conn.cursor()

        cursor.execute(f"TRUNCATE TABLE {table_name} CASCADE")
        conn.commit()
        cursor.close()
        conn.close()

        logger.info(f"✓ Truncated {table_name}")
        return True

    except Exception as e:
        logger.error(f"✗ Truncate failed: {e}")
        if conn:
            conn.rollback()
            conn.close()
        raise


if __name__ == "__main__":
    try:
        logger.info("Testing database connection...")
        conn = get_connection()
        print("✓ Database connection successful!")
        conn.close()
    except Exception as e:
        print(f"✗ Database connection failed: {e}")
        sys.exit(1)
