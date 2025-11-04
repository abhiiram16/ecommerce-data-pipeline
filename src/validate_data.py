"""
Quick data validation script
"""
import pandas as pd
import os

print("="*60)
print("DATA VALIDATION")
print("="*60)

# File paths
customers_file = 'data/raw/customers.csv'
products_file = 'data/raw/products.csv'
orders_file = 'data/raw/orders.csv'

# Load data
print("\n📂 Loading data files...")
customers_df = pd.read_csv(customers_file)
products_df = pd.read_csv(products_file)
orders_df = pd.read_csv(orders_file)
print("✓ All files loaded successfully")

# Validation checks
print("\n🔍 Running validation checks...\n")

# 1. Check for nulls
print("1. NULL VALUE CHECK:")
print(f"  Customers nulls: {customers_df.isnull().sum().sum()}")
print(f"  Products nulls: {products_df.isnull().sum().sum()}")
print(f"  Orders nulls: {orders_df.isnull().sum().sum()}")

# 2. Check for duplicates
print("\n2. DUPLICATE CHECK:")
print(
    f"  Duplicate customer IDs: {customers_df['customer_id'].duplicated().sum()}")
print(
    f"  Duplicate product IDs: {products_df['product_id'].duplicated().sum()}")
print(f"  Duplicate order IDs: {orders_df['order_id'].duplicated().sum()}")

# 3. Check foreign key integrity
print("\n3. FOREIGN KEY INTEGRITY:")
invalid_customers = orders_df[~orders_df['customer_id'].isin(
    customers_df['customer_id'])]
invalid_products = orders_df[~orders_df['product_id'].isin(
    products_df['product_id'])]
print(f"  Orders with invalid customer_id: {len(invalid_customers)}")
print(f"  Orders with invalid product_id: {len(invalid_products)}")

# 4. Check data ranges
print("\n4. DATA RANGE CHECKS:")
print(
    f"  Customer ages: {customers_df['age'].min()} - {customers_df['age'].max()}")
print(
    f"  Product prices: ₹{products_df['price'].min():,.2f} - ₹{products_df['price'].max():,.2f}")
print(
    f"  Order quantities: {orders_df['quantity'].min()} - {orders_df['quantity'].max()}")

# 5. Summary statistics
print("\n5. SUMMARY STATISTICS:")
print(f"  Total revenue: ₹{orders_df['total_amount'].sum():,.2f}")
print(f"  Average order value: ₹{orders_df['total_amount'].mean():,.2f}")
print(f"  Active customers: {orders_df['customer_id'].nunique()}")
print(f"  Product categories: {products_df['category'].nunique()}")

print("\n" + "="*60)
print("✅ VALIDATION COMPLETE!")
print("="*60)
