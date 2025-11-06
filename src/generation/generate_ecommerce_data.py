"""
E-Commerce Data Generator (With Auto-Incrementing Seed & Unique IDs)
====================================================================

Generates realistic synthetic e-commerce data for testing data pipelines.
Each run generates DIFFERENT data with UNIQUE IDs for cumulative growth.

Author: Abhiiram
Date: November 7, 2025
"""

from loguru import logger
from src.utils.config import Config
import pandas as pd
import numpy as np
from faker import Faker
from datetime import datetime, timedelta
import random
import os
import sys
import time

# Add parent directory to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../../'))

# Configure logging
logger.add(
    f"{Config.LOGS_DIR}/generation_{{time:YYYY-MM-DD}}.log",
    level=Config.LOG_LEVEL
)

# ========================================
# CONFIGURATION WITH AUTO-INCREMENTING SEED
# ========================================

# Initialize Faker
fake = Faker('en_IN')

# Auto-incrementing seed based on timestamp (changes every run)
RANDOM_SEED = int(os.getenv('RANDOM_SEED', int(time.time()) % 100000))

# Apply seed for reproducibility within this run
Faker.seed(RANDOM_SEED)
np.random.seed(RANDOM_SEED)
random.seed(RANDOM_SEED)

# Data generation parameters from config
NUM_CUSTOMERS = Config.NUM_CUSTOMERS
NUM_PRODUCTS = Config.NUM_PRODUCTS
NUM_ORDERS = Config.NUM_ORDERS

# ID OFFSETS BASED ON SEED - Ensures unique IDs each run
# 0 to 990,000 in steps of 10,000
CUSTOMER_ID_OFFSET = (RANDOM_SEED % 100) * 10000
# 0 to 95,000 in steps of 5,000
PRODUCT_ID_OFFSET = (RANDOM_SEED % 20) * 5000
ORDER_ID_OFFSET = (RANDOM_SEED % 1000) * 100    # 0 to 99,900 in steps of 100

# Date range
END_DATE = datetime.now()
START_DATE = END_DATE - timedelta(days=180)

# Indian cities
INDIAN_CITIES = [
    ('Mumbai', 'Maharashtra', '400001'),
    ('Delhi', 'Delhi', '110001'),
    ('Bangalore', 'Karnataka', '560001'),
    ('Hyderabad', 'Telangana', '500001'),
    ('Chennai', 'Tamil Nadu', '600001'),
    ('Kolkata', 'West Bengal', '700001'),
    ('Pune', 'Maharashtra', '411001'),
    ('Ahmedabad', 'Gujarat', '380001'),
    ('Jaipur', 'Rajasthan', '302001'),
    ('Lucknow', 'Uttar Pradesh', '226001')
]

# Product categories
PRODUCT_CATEGORIES = {
    'Electronics': ['Smartphones', 'Laptops', 'Tablets', 'Headphones', 'Smart Watches'],
    'Fashion': ['Mens Clothing', 'Womens Clothing', 'Kids Clothing', 'Footwear', 'Accessories'],
    'Home & Kitchen': ['Furniture', 'Kitchen Appliances', 'Home Decor', 'Bedding', 'Storage']
}

# Output directory
RAW_DATA_DIR = Config.DATA_RAW_DIR

logger.info("=" * 60)
logger.info("E-COMMERCE DATA GENERATOR")
logger.info("=" * 60)
logger.info(f"Random Seed: {RANDOM_SEED}")
logger.info(f"Customer ID Offset: {CUSTOMER_ID_OFFSET}")
logger.info(f"Product ID Offset: {PRODUCT_ID_OFFSET}")
logger.info(f"Order ID Offset: {ORDER_ID_OFFSET}")
logger.info(f"Customers: {NUM_CUSTOMERS:,}")
logger.info(f"Products: {NUM_PRODUCTS:,}")
logger.info(f"Orders: {NUM_ORDERS:,}")
logger.info(f"Date Range: {START_DATE.date()} to {END_DATE.date()}")
logger.info(f"Output: {RAW_DATA_DIR}")

print("=" * 60)
print("E-COMMERCE DATA GENERATOR")
print("=" * 60)
print(f"\nConfiguration:")
print(f" - Random Seed: {RANDOM_SEED}")
print(
    f" - Customer ID Range: {CUSTOMER_ID_OFFSET + 1001} to {CUSTOMER_ID_OFFSET + 1001 + NUM_CUSTOMERS - 1}")
print(
    f" - Product ID Range: {PRODUCT_ID_OFFSET + 2001} to {PRODUCT_ID_OFFSET + 2001 + NUM_PRODUCTS - 1}")
print(f" - Order ID Range: {ORDER_ID_OFFSET + 3001} onwards")
print(f" - Customers: {NUM_CUSTOMERS:,}")
print(f" - Products: {NUM_PRODUCTS:,}")
print(f" - Orders: {NUM_ORDERS:,}")
print(f" - Date Range: {START_DATE.date()} to {END_DATE.date()}")
print(f" - Output Directory: {RAW_DATA_DIR}")
print("=" * 60)

# ========================================
# FUNCTION 1: GENERATE CUSTOMERS
# ========================================


def generate_customers(num_customers=NUM_CUSTOMERS):
    """Generate synthetic customer data with Indian demographics."""

    logger.info(f"[1/3] Generating {num_customers:,} customers...")
    print(f"\n[1/3] Generating Customers...")
    print(f" → Creating {num_customers:,} customer records...")

    customers = []

    for i in range(num_customers):
        # UNIQUE ID: offset by seed
        customer_id = CUSTOMER_ID_OFFSET + 1001 + i
        first_name = fake.first_name()
        last_name = fake.last_name()
        email = f"{first_name.lower()}.{last_name.lower()}{random.randint(1, 999)}@{random.choice(['gmail.com', 'yahoo.com', 'outlook.com'])}"
        phone = f"+91-{random.randint(6000000000, 9999999999)}"

        city, state, pincode = random.choice(INDIAN_CITIES)

        days_ago = random.randint(0, 730)
        registration_date = END_DATE - timedelta(days=days_ago)

        age = random.randint(18, 70)
        gender = random.choice(['Male', 'Female', 'Other'])

        customer = {
            'customer_id': customer_id,
            'first_name': first_name,
            'last_name': last_name,
            'email': email,
            'phone': phone,
            'city': city,
            'state': state,
            'pincode': pincode,
            'registration_date': registration_date.strftime('%Y-%m-%d'),
            'age': age,
            'gender': gender
        }

        customers.append(customer)

        if (i + 1) % 2000 == 0:
            logger.info(f"  → Generated {i + 1:,} customers...")
            print(f" → Generated {i + 1:,} customers...")

    customers_df = pd.DataFrame(customers)
    logger.info(f"✓ Generated {len(customers_df):,} customers")
    print(f" ✓ Successfully generated {len(customers_df):,} customers")

    return customers_df

# ========================================
# FUNCTION 2: GENERATE PRODUCTS
# ========================================


def generate_products(num_products=NUM_PRODUCTS):
    """Generate synthetic product catalog with realistic pricing."""

    logger.info(f"[2/3] Generating {num_products:,} products...")
    print(f"\n[2/3] Generating Products...")
    print(f" → Creating {num_products:,} product records...")

    products = []

    price_ranges = {
        'Electronics': {
            'Smartphones': (8000, 150000),
            'Laptops': (25000, 120000),
            'Tablets': (10000, 80000),
            'Headphones': (500, 25000),
            'Smart Watches': (2000, 50000)
        },
        'Fashion': {
            'Mens Clothing': (500, 5000),
            'Womens Clothing': (600, 8000),
            'Kids Clothing': (300, 2000),
            'Footwear': (800, 8000),
            'Accessories': (300, 5000)
        },
        'Home & Kitchen': {
            'Furniture': (3000, 50000),
            'Kitchen Appliances': (1500, 40000),
            'Home Decor': (200, 5000),
            'Bedding': (500, 8000),
            'Storage': (300, 5000)
        }
    }

    suppliers = ['Tech Supplies India', 'Fashion Hub Pvt Ltd', 'Home Essentials Co',
                 'Elite Electronics', 'Style Distributors', 'Kitchen World',
                 'Mega Suppliers', 'Prime Products', 'Smart Solutions Ltd']

    # UNIQUE ID: offset by seed
    product_id_counter = PRODUCT_ID_OFFSET + 2001

    for category, subcategories in PRODUCT_CATEGORIES.items():
        products_per_category = num_products // len(PRODUCT_CATEGORIES)
        products_per_subcategory = products_per_category // len(subcategories)

        for subcategory in subcategories:
            for i in range(products_per_subcategory):
                product_id = product_id_counter
                product_id_counter += 1

                product_name = f"{subcategory} {random.choice(['Pro', 'Max', 'Plus', 'Ultra', 'Lite'])}"
                brand = subcategory.split()[0]

                min_price, max_price = price_ranges[category][subcategory]
                price = round(random.uniform(min_price, max_price), 2)

                cost_percentage = random.uniform(0.70, 0.85)
                cost = round(price * cost_percentage, 2)

                if category == 'Electronics':
                    stock_quantity = random.randint(20, 200)
                elif category == 'Fashion':
                    stock_quantity = random.randint(50, 500)
                else:
                    stock_quantity = random.randint(30, 300)

                supplier = random.choice(suppliers)

                product = {
                    'product_id': product_id,
                    'product_name': product_name,
                    'category': category,
                    'subcategory': subcategory,
                    'brand': brand,
                    'price': price,
                    'cost': cost,
                    'stock_quantity': stock_quantity,
                    'supplier': supplier
                }

                products.append(product)

    products_df = pd.DataFrame(products)
    logger.info(f"✓ Generated {len(products_df):,} products")
    print(f" ✓ Successfully generated {len(products_df):,} products")

    return products_df

# ========================================
# FUNCTION 3: GENERATE ORDERS
# ========================================


def generate_orders(customers_df, products_df, num_orders=NUM_ORDERS):
    """Generate synthetic order data with realistic patterns."""

    logger.info(f"[3/3] Generating {num_orders:,} orders...")
    print(f"\n[3/3] Generating Orders...")
    print(f" → Creating {num_orders:,} order records...")

    orders = []

    customer_ids = customers_df['customer_id'].tolist()
    product_data = products_df[['product_id',
                                'price', 'category']].to_dict('records')

    # Customer segments
    num_customers = len(customer_ids)
    vip_customers = customer_ids[:int(num_customers * 0.2)]
    regular_customers = customer_ids[int(
        num_customers * 0.2):int(num_customers * 0.7)]
    occasional_customers = customer_ids[int(num_customers * 0.7):]

    vip_orders = int(num_orders * 0.5)
    regular_orders = int(num_orders * 0.4)
    occasional_orders = num_orders - vip_orders - regular_orders

    customer_order_distribution = (
        [random.choice(vip_customers) for _ in range(vip_orders)] +
        [random.choice(regular_customers) for _ in range(regular_orders)] +
        [random.choice(occasional_customers) for _ in range(occasional_orders)]
    )

    random.shuffle(customer_order_distribution)

    # UNIQUE ID: offset by seed
    order_id_counter = ORDER_ID_OFFSET + 3001

    for i in range(num_orders):
        order_id = order_id_counter
        order_id_counter += 1

        customer_id = customer_order_distribution[i]

        days_range = (END_DATE - START_DATE).days
        random_days = random.randint(0, days_range)
        order_date = START_DATE + timedelta(days=random_days)

        hour_weights = [2, 1, 1, 1, 2, 3, 5, 7, 8, 10, 12, 15,
                        18, 20, 22, 24, 20, 25, 30, 35, 30, 25, 20, 15]
        order_hour = random.choices(range(24), weights=hour_weights)[0]
        order_minute = random.randint(0, 59)
        order_second = random.randint(0, 59)

        order_datetime = order_date.replace(
            hour=order_hour, minute=order_minute, second=order_second)

        product = random.choice(product_data)
        product_id = product['product_id']
        unit_price = product['price']

        quantity = random.choices(
            [1, 2, 3, 4, 5], weights=[50, 25, 15, 7, 3])[0]
        total_amount = round(quantity * unit_price, 2)

        payment_methods = ['UPI', 'Credit Card',
                           'Debit Card', 'Cash on Delivery', 'Net Banking']
        payment_method = random.choices(payment_methods, weights=[
                                        0.50, 0.20, 0.15, 0.10, 0.05])[0]

        order_statuses = ['Delivered', 'Shipped', 'Processing', 'Cancelled']
        order_status = random.choices(order_statuses, weights=[
                                      0.75, 0.15, 0.05, 0.05])[0]

        order = {
            'order_id': order_id,
            'customer_id': customer_id,
            'product_id': product_id,
            'quantity': quantity,
            'unit_price': unit_price,
            'total_amount': total_amount,
            'order_date': order_datetime.strftime('%Y-%m-%d %H:%M:%S'),
            'payment_method': payment_method,
            'order_status': order_status
        }

        orders.append(order)

        if (i + 1) % 5000 == 0:
            logger.info(f"  → Generated {i + 1:,} orders...")
            print(f" → Generated {i + 1:,} orders...")

    orders_df = pd.DataFrame(orders)
    logger.info(f"✓ Generated {len(orders_df):,} orders")
    print(f" ✓ Successfully generated {len(orders_df):,} orders")

    return orders_df

# ========================================
# SAVE TO CSV
# ========================================


def save_to_csv(customers_df, products_df, orders_df):
    """Save DataFrames to CSV files."""

    logger.info("Saving data to CSV files...")
    print("\n" + "=" * 60)
    print("SAVING TO CSV")
    print("=" * 60)

    try:
        os.makedirs(RAW_DATA_DIR, exist_ok=True)

        customers_file = os.path.join(RAW_DATA_DIR, 'customers.csv')
        customers_df.to_csv(customers_file, index=False)
        logger.info(f"✓ Saved customers to {customers_file}")
        print(f" ✓ Customers: {customers_file}")

        products_file = os.path.join(RAW_DATA_DIR, 'products.csv')
        products_df.to_csv(products_file, index=False)
        logger.info(f"✓ Saved products to {products_file}")
        print(f" ✓ Products: {products_file}")

        orders_file = os.path.join(RAW_DATA_DIR, 'orders.csv')
        orders_df.to_csv(orders_file, index=False)
        logger.info(f"✓ Saved orders to {orders_file}")
        print(f" ✓ Orders: {orders_file}")

        print("=" * 60)
        logger.info("✓ All data saved successfully")

    except Exception as e:
        logger.error(f"✗ Save failed: {e}")
        raise

# ========================================
# MAIN
# ========================================


def main():
    """Main execution."""

    try:
        start_time = datetime.now()

        # Generate data
        customers_df = generate_customers()
        products_df = generate_products()
        orders_df = generate_orders(customers_df, products_df)

        # Save to CSV
        save_to_csv(customers_df, products_df, orders_df)

        # Summary
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()

        logger.info("=" * 60)
        logger.info("✓ DATA GENERATION COMPLETE")
        logger.info("=" * 60)
        logger.info(f"Duration: {duration:.2f} seconds")
        logger.info(
            f"Total records: {len(customers_df) + len(products_df) + len(orders_df):,}")

        print("\n" + "=" * 60)
        print("✓ DATA GENERATION COMPLETE")
        print("=" * 60)
        print(f" Duration: {duration:.2f} seconds")
        print(
            f" Total records: {len(customers_df) + len(products_df) + len(orders_df):,}")
        print("=" * 60)

    except Exception as e:
        logger.error(f"✗ Generation failed: {e}")
        print(f"\n✗ Generation failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    Config.create_directories()
    main()
