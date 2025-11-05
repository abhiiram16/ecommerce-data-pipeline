"""
Data Profiling Script
=====================
Analyzes data quality metrics across all tables.

Generates statistics on:
- Null value percentages
- Data type distributions
- Value ranges (min/max)
- Unique value counts
- Data completeness scores

Author: Abhiiram
Date: November 5, 2025
"""

import psycopg2
import pandas as pd
from datetime import datetime
import json

# Database connection
DB_CONFIG = {
    'host': 'localhost',
    'port': 5432,
    'database': 'ecommerce_db',
    'user': 'dataeng',
    'password': 'pipeline123'
}


def connect_db():
    """Connect to PostgreSQL database."""
    return psycopg2.connect(**DB_CONFIG)


def profile_table(conn, table_name):
    """
    Profile a single table.

    Returns:
        dict: Profiling statistics
    """
    print(f"\n📊 Profiling: {table_name}")
    print("-" * 60)

    # Get table data
    df = pd.read_sql(f"SELECT * FROM {table_name}", conn)

    profile = {
        'table_name': table_name,
        'row_count': len(df),
        'column_count': len(df.columns),
        'columns': {}
    }

    # Profile each column
    for col in df.columns:
        col_profile = {
            'dtype': str(df[col].dtype),
            'null_count': int(df[col].isnull().sum()),
            'null_percentage': round(df[col].isnull().sum() / len(df) * 100, 2),
            'unique_count': int(df[col].nunique()),
            'uniqueness_percentage': round(df[col].nunique() / len(df) * 100, 2)
        }

        # Numeric columns: add min/max/mean
        if pd.api.types.is_numeric_dtype(df[col]):
            col_profile.update({
                'min': float(df[col].min()) if pd.notna(df[col].min()) else None,
                'max': float(df[col].max()) if pd.notna(df[col].max()) else None,
                'mean': round(float(df[col].mean()), 2) if pd.notna(df[col].mean()) else None
            })

        # String columns: add sample values
        elif df[col].dtype == 'object':
            top_values = df[col].value_counts().head(5).to_dict()
            col_profile['top_5_values'] = {
                str(k): int(v) for k, v in top_values.items()}

        profile['columns'][col] = col_profile

        # Print summary
        print(f"  {col:30} | {col_profile['dtype']:15} | "
              f"Nulls: {col_profile['null_percentage']:>5}% | "
              f"Unique: {col_profile['uniqueness_percentage']:>5}%")

    # Overall completeness score
    total_cells = len(df) * len(df.columns)
    null_cells = df.isnull().sum().sum()
    completeness = round((1 - null_cells / total_cells) * 100, 2)

    profile['completeness_score'] = completeness

    print(f"\n  Total Rows: {profile['row_count']:,}")
    print(f"  Total Columns: {profile['column_count']}")
    print(f"  Completeness Score: {completeness}%")

    return profile


def profile_all_tables():
    """Profile all main tables."""
    conn = connect_db()

    tables = ['customers', 'products', 'orders',
              'customer_summary', 'product_summary',
              'daily_sales_summary', 'monthly_sales_summary']

    print("=" * 60)
    print("DATA PROFILING REPORT")
    print(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)

    profiles = {}

    for table in tables:
        try:
            profile = profile_table(conn, table)
            profiles[table] = profile
        except Exception as e:
            print(f"  ✗ Error profiling {table}: {e}")

    conn.close()

    # Save to JSON
    output_file = 'data_profile_report.json'
    with open(output_file, 'w') as f:
        json.dump(profiles, f, indent=2)

    print("\n" + "=" * 60)
    print(f"✓ Profile saved to: {output_file}")
    print("=" * 60)

    return profiles


if __name__ == "__main__":
    profile_all_tables()
