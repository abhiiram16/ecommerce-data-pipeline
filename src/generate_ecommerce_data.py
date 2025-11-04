"""
E-Commerce Data Generator
========================
Generates realistic synthetic e-commerce data for testing data pipelines.

Generates:
- 10,000 customers
- 500 products  
- 50,000 orders (6 months of transactions)

Author: Abhiiram
Date: November 4, 2025
"""

# ============================================
# IMPORTS
# ============================================
import pandas as pd
import numpy as np
from faker import Faker
from datetime import datetime, timedelta
import random
import os

# ============================================
# CONFIGURATION
# ============================================

# Initialize Faker with Indian locale for realistic Indian names/addresses
fake = Faker('en_IN')

# Set random seed for reproducibility (same data each time you run)
Faker.seed(42)
np.random.seed(42)
random.seed(42)

# Data generation parameters
NUM_CUSTOMERS = 10000  # Number of customers to generate
NUM_PRODUCTS = 500     # Number of products to generate
NUM_ORDERS = 50000     # Number of orders to generate

# Date range for orders (last 6 months)
END_DATE = datetime.now()
START_DATE = END_DATE - timedelta(days=180)  # 6 months ago

# Indian cities for customer locations
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

# Product categories and subcategories
PRODUCT_CATEGORIES = {
    'Electronics': ['Smartphones', 'Laptops', 'Tablets', 'Headphones', 'Smart Watches'],
    'Fashion': ['Mens Clothing', 'Womens Clothing', 'Kids Clothing', 'Footwear', 'Accessories'],
    'Home & Kitchen': ['Furniture', 'Kitchen Appliances', 'Home Decor', 'Bedding', 'Storage']
}

# Indian payment methods and their distribution
PAYMENT_METHODS = ['UPI', 'Credit Card',
                   'Debit Card', 'Cash on Delivery', 'Net Banking']
PAYMENT_METHOD_WEIGHTS = [0.50, 0.20, 0.15,
                          0.10, 0.05]  # UPI is most popular (50%)

# Order statuses
ORDER_STATUSES = ['Delivered', 'Shipped', 'Processing', 'Cancelled']
ORDER_STATUS_WEIGHTS = [0.75, 0.15, 0.05, 0.05]  # Most orders delivered (75%)

# Output directories
RAW_DATA_DIR = os.path.join('data', 'raw')

print("=" * 60)
print("E-COMMERCE DATA GENERATOR")
print("=" * 60)
print(f"\nConfiguration:")
print(f"  - Customers: {NUM_CUSTOMERS:,}")
print(f"  - Products: {NUM_PRODUCTS:,}")
print(f"  - Orders: {NUM_ORDERS:,}")
print(f"  - Date Range: {START_DATE.date()} to {END_DATE.date()}")
print(f"  - Output Directory: {RAW_DATA_DIR}")
print("=" * 60)


# ============================================
# FUNCTION 1: GENERATE CUSTOMERS
# ============================================

def generate_customers(num_customers=NUM_CUSTOMERS):
    """
    Generate synthetic customer data with Indian demographics.

    Parameters:
    -----------
    num_customers : int
        Number of customers to generate (default: 10,000)

    Returns:
    --------
    pandas.DataFrame
        DataFrame containing customer information
    """
    print("\n[1/3] Generating Customers...")
    print(f"  → Creating {num_customers:,} customer records...")

    customers = []

    for i in range(num_customers):
        # Generate unique customer ID (starts from 1001)
        customer_id = 1001 + i

        # Generate Indian name
        first_name = fake.first_name()
        last_name = fake.last_name()

        # Generate email (lowercase, no spaces)
        email = f"{first_name.lower()}.{last_name.lower()}{random.randint(1, 999)}@{random.choice(['gmail.com', 'yahoo.com', 'outlook.com', 'hotmail.com'])}"

        # Generate Indian phone number (+91-XXXXXXXXXX)
        phone = f"+91-{random.randint(6000000000, 9999999999)}"

        # Assign random Indian city
        city, state, pincode = random.choice(INDIAN_CITIES)

        # Generate registration date (randomly distributed over past 2 years)
        days_ago = random.randint(0, 730)  # 0 to 2 years
        registration_date = END_DATE - timedelta(days=days_ago)

        # Generate age (18 to 70)
        age = random.randint(18, 70)

        # Generate gender
        gender = random.choice(['Male', 'Female', 'Other'])

        # Create customer record
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

        # Progress indicator (print every 2000 customers)
        if (i + 1) % 2000 == 0:
            print(f"  → Generated {i + 1:,} customers...")

    # Convert list of dictionaries to DataFrame
    customers_df = pd.DataFrame(customers)

    print(f"  ✓ Successfully generated {len(customers_df):,} customers")
    print(f"  ✓ Columns: {', '.join(customers_df.columns)}")

    return customers_df


# ============================================
# QUICK TEST - Comment out later
# ============================================
'''if __name__ == "__main__":
    # Test with just 10 customers (fast)
    test_df = generate_customers(10)
    print("\n📊 Sample Customer Data:")
    print(test_df.head())
    print(f"\n✓ Data types:\n{test_df.dtypes}")'''


# ============================================
# FUNCTION 2: GENERATE PRODUCTS
# ============================================

def generate_products(num_products=NUM_PRODUCTS):
    """
    Generate synthetic product catalog with realistic pricing.

    Parameters:
    -----------
    num_products : int
        Number of products to generate (default: 500)

    Returns:
    --------
    pandas.DataFrame
        DataFrame containing product information
    """
    print("\n[2/3] Generating Products...")
    print(f"  → Creating {num_products:,} product records...")

    products = []

    # Define realistic product templates for each category
    product_templates = {
        'Electronics': {
            'Smartphones': ['Samsung Galaxy', 'iPhone', 'OnePlus', 'Xiaomi Redmi', 'Realme', 'Vivo', 'Oppo'],
            'Laptops': ['HP Pavilion', 'Dell Inspiron', 'Lenovo IdeaPad', 'Asus VivoBook', 'Acer Aspire', 'MacBook'],
            'Tablets': ['iPad', 'Samsung Galaxy Tab', 'Lenovo Tab', 'Amazon Fire', 'Mi Pad'],
            'Headphones': ['Sony WH', 'JBL', 'boAt Rockerz', 'OnePlus Buds', 'Noise ColorFit', 'Realme Buds'],
            'Smart Watches': ['Apple Watch', 'Samsung Galaxy Watch', 'Fitbit', 'Amazfit', 'boAt Storm', 'Noise ColorFit']
        },
        'Fashion': {
            'Mens Clothing': ['Casual Shirt', 'Formal Shirt', 'T-Shirt', 'Jeans', 'Chinos', 'Jacket', 'Sweater'],
            'Womens Clothing': ['Saree', 'Kurti', 'Dress', 'Top', 'Jeans', 'Palazzo', 'Lehenga'],
            'Kids Clothing': ['Kids T-Shirt', 'Kids Jeans', 'Kids Dress', 'Kids Shorts', 'Kids Sweater'],
            'Footwear': ['Sneakers', 'Formal Shoes', 'Sandals', 'Boots', 'Slippers', 'Sports Shoes'],
            'Accessories': ['Watch', 'Belt', 'Wallet', 'Sunglasses', 'Cap', 'Bag', 'Scarf']
        },
        'Home & Kitchen': {
            'Furniture': ['Sofa', 'Bed', 'Dining Table', 'Wardrobe', 'Study Table', 'Chair', 'Bookshelf'],
            'Kitchen Appliances': ['Mixer Grinder', 'Microwave', 'Refrigerator', 'Induction Cooktop', 'Electric Kettle', 'Toaster'],
            'Home Decor': ['Wall Clock', 'Photo Frame', 'Curtain', 'Carpet', 'Vase', 'Lamp', 'Cushion'],
            'Bedding': ['Bedsheet', 'Pillow', 'Comforter', 'Blanket', 'Mattress', 'Quilt'],
            'Storage': ['Storage Box', 'Organizer', 'Rack', 'Cabinet', 'Basket', 'Drawer']
        }
    }

    # Price ranges for each category (in INR)
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

    # Supplier names
    suppliers = [
        'Tech Supplies India', 'Fashion Hub Pvt Ltd', 'Home Essentials Co',
        'Elite Electronics', 'Style Distributors', 'Kitchen World',
        'Mega Suppliers', 'Prime Products', 'Smart Solutions Ltd'
    ]

    product_id_counter = 2001  # Start product IDs from 2001

    for category, subcategories in PRODUCT_CATEGORIES.items():
        # Calculate how many products per category (roughly equal distribution)
        products_per_category = num_products // len(PRODUCT_CATEGORIES)
        products_per_subcategory = products_per_category // len(subcategories)

        for subcategory in subcategories:
            for i in range(products_per_subcategory):
                # Generate product ID
                product_id = product_id_counter
                product_id_counter += 1

                # Generate product name
                base_names = product_templates[category][subcategory]
                base_name = random.choice(base_names)

                # Add model/variant (like "Pro", "Max", "Plus", etc.)
                variants = ['', 'Pro', 'Max', 'Plus',
                            'Ultra', 'Lite', 'Classic', 'Premium']
                variant = random.choice(variants)

                # Add size/color for some products
                if category == 'Fashion':
                    sizes = ['S', 'M', 'L', 'XL', 'XXL']
                    colors = ['Black', 'White', 'Blue', 'Red', 'Green', 'Grey']
                    product_name = f"{base_name} - {random.choice(colors)} - {random.choice(sizes)}"
                elif variant:
                    product_name = f"{base_name} {variant}"
                else:
                    product_name = base_name

                # Extract brand from base_name (first word usually)
                brand = base_name.split()[0]

                # Generate price based on category and subcategory
                min_price, max_price = price_ranges[category][subcategory]
                price = round(random.uniform(min_price, max_price), 2)

                # Generate cost (70-85% of price for profit margin)
                cost_percentage = random.uniform(0.70, 0.85)
                cost = round(price * cost_percentage, 2)

                # Generate stock quantity (realistic inventory levels)
                if category == 'Electronics':
                    stock_quantity = random.randint(20, 200)
                elif category == 'Fashion':
                    stock_quantity = random.randint(50, 500)
                else:  # Home & Kitchen
                    stock_quantity = random.randint(30, 300)

                # Assign supplier
                supplier = random.choice(suppliers)

                # Create product record
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

    # Convert to DataFrame
    products_df = pd.DataFrame(products)

    print(f"  ✓ Successfully generated {len(products_df):,} products")
    print(f"  ✓ Categories: {products_df['category'].nunique()}")
    print(
        f"  ✓ Price range: ₹{products_df['price'].min():,.2f} - ₹{products_df['price'].max():,.2f}")

    return products_df


# ============================================
# QUICK TEST - Comment out later
# ============================================
'''if __name__ == "__main__":
    # Test products
    test_products = generate_products(30)
    print("\n📊 Sample Product Data:")
    print(test_products.head(10))
    print(f"\n📈 Products by Category:")
    print(test_products['category'].value_counts())
    print(f"\n💰 Price Statistics:")
    print(test_products[['price', 'cost']].describe())
'''
# ============================================


# ============================================
# FUNCTION 3: GENERATE ORDERS
# ============================================

def generate_orders(customers_df, products_df, num_orders=NUM_ORDERS):
    """
    Generate synthetic order/transaction data with realistic patterns.

    Includes:
    - Seasonal trends (Diwali sales spike)
    - Weekend patterns (higher sales on Sat/Sun)
    - Customer segmentation (VIP, Regular, Occasional)
    - Time-of-day patterns

    Parameters:
    -----------
    customers_df : pandas.DataFrame
        Customer data generated from generate_customers()
    products_df : pandas.DataFrame
        Product data generated from generate_products()
    num_orders : int
        Number of orders to generate (default: 50,000)

    Returns:
    --------
    pandas.DataFrame
        DataFrame containing order/transaction information
    """
    print("\n[3/3] Generating Orders...")
    print(f"  → Creating {num_orders:,} order records...")
    print("  → Applying realistic patterns (weekends, Diwali, customer segments)...")

    orders = []

    # Get customer and product IDs for random selection
    customer_ids = customers_df['customer_id'].tolist()
    product_data = products_df[['product_id',
                                'price', 'category']].to_dict('records')

    # Define customer segments (purchase frequency)
    # VIP: 20% of customers, make 50% of orders
    # Regular: 50% of customers, make 40% of orders
    # Occasional: 30% of customers, make 10% of orders

    num_customers = len(customer_ids)
    vip_customers = customer_ids[:int(num_customers * 0.2)]
    regular_customers = customer_ids[int(
        num_customers * 0.2):int(num_customers * 0.7)]
    occasional_customers = customer_ids[int(num_customers * 0.7):]

    # Calculate orders per segment
    vip_orders = int(num_orders * 0.5)
    regular_orders = int(num_orders * 0.4)
    occasional_orders = num_orders - vip_orders - regular_orders

    # Create order distribution by customer segment
    customer_order_distribution = (
        [random.choice(vip_customers) for _ in range(vip_orders)] +
        [random.choice(regular_customers) for _ in range(regular_orders)] +
        [random.choice(occasional_customers) for _ in range(occasional_orders)]
    )

    # Shuffle to mix segments
    random.shuffle(customer_order_distribution)

    # Diwali date range (example: October 24-November 13 for 2024)
    # Adjust this based on actual Diwali dates
    diwali_start = datetime(2024, 10, 24)
    diwali_end = datetime(2024, 11, 13)

    order_id_counter = 3001  # Start order IDs from 3001

    for i in range(num_orders):
        # Generate order ID
        order_id = order_id_counter
        order_id_counter += 1

        # Select customer (already distributed by segment)
        customer_id = customer_order_distribution[i]

        # Generate order date
        days_range = (END_DATE - START_DATE).days
        random_days = random.randint(0, days_range)
        order_date = START_DATE + timedelta(days=random_days)

        # Apply weekend pattern (30% more orders on weekends)
        weekday = order_date.weekday()  # Monday=0, Sunday=6
        if weekday >= 5:  # Saturday or Sunday
            # Increase probability of order on weekend
            if random.random() > 0.7:  # 70% chance to skip (simulating higher volume)
                pass  # Keep this order
            else:
                # Regenerate date to simulate weekend concentration
                weekend_bias = random.choice([5, 6])  # Saturday=5, Sunday=6
                order_date = order_date - \
                    timedelta(days=weekday) + timedelta(days=weekend_bias)

        # Apply Diwali spike (50% more orders during Diwali)
        if diwali_start <= order_date <= diwali_end:
            # During Diwali, increase order probability
            is_diwali_order = True
        else:
            is_diwali_order = False

        # Generate order time (realistic time-of-day distribution)
        # Peak hours: 8 PM (20:00), Low hours: 3 AM (03:00)
        hour_weights = [
            2, 1, 1, 1, 2, 3, 5, 7,    # 00-07: Late night to early morning
            8, 10, 12, 15, 18, 20, 22, 24,  # 08-15: Morning to afternoon
            # 16-23: Evening peak (20:00 highest)
            20, 25, 30, 35, 30, 25, 20, 15
        ]
        order_hour = random.choices(range(24), weights=hour_weights)[0]
        order_minute = random.randint(0, 59)
        order_second = random.randint(0, 59)

        order_datetime = order_date.replace(
            hour=order_hour,
            minute=order_minute,
            second=order_second
        )

        # Select product
        product = random.choice(product_data)
        product_id = product['product_id']
        unit_price = product['price']
        category = product['category']

        # Generate quantity (most orders are 1 item, some are 2-5)
        quantity_weights = [70, 20, 5, 3, 2]  # 70% buy 1 item, 20% buy 2, etc.
        quantity = random.choices([1, 2, 3, 4, 5], weights=quantity_weights)[0]

        # Calculate subtotal
        subtotal = unit_price * quantity

        # Apply discount (VIP customers get better discounts)
        if customer_id in vip_customers:
            # VIP: 10-30% discount
            discount_percentage = random.uniform(0.10, 0.30)
        elif customer_id in regular_customers:
            # Regular: 5-15% discount
            discount_percentage = random.uniform(0.05, 0.15)
        else:
            # Occasional: 0-10% discount
            discount_percentage = random.uniform(0.0, 0.10)

        # Diwali offers: Additional 10% discount
        if is_diwali_order:
            discount_percentage += 0.10
            discount_percentage = min(discount_percentage, 0.50)  # Cap at 50%

        discount_applied = round(subtotal * discount_percentage, 2)
        total_amount = round(subtotal - discount_applied, 2)

        # Payment method (weighted distribution)
        payment_method = random.choices(
            PAYMENT_METHODS, weights=PAYMENT_METHOD_WEIGHTS)[0]

        # Order status (weighted distribution)
        order_status = random.choices(
            ORDER_STATUSES, weights=ORDER_STATUS_WEIGHTS)[0]

        # Shipping city (randomly assign from INDIAN_CITIES)
        shipping_city = random.choice(INDIAN_CITIES)[0]

        # Create order record
        order = {
            'order_id': order_id,
            'customer_id': customer_id,
            'product_id': product_id,
            'order_date': order_datetime.strftime('%Y-%m-%d %H:%M:%S'),
            'quantity': quantity,
            'unit_price': unit_price,
            'total_amount': total_amount,
            'discount_applied': discount_applied,
            'payment_method': payment_method,
            'order_status': order_status,
            'shipping_city': shipping_city
        }

        orders.append(order)

        # Progress indicator (every 10,000 orders)
        if (i + 1) % 10000 == 0:
            print(f"  → Generated {i + 1:,} orders...")

    # Convert to DataFrame
    orders_df = pd.DataFrame(orders)

    # Calculate summary statistics
    total_revenue = orders_df['total_amount'].sum()
    avg_order_value = orders_df['total_amount'].mean()
    total_discount = orders_df['discount_applied'].sum()

    print(f"  ✓ Successfully generated {len(orders_df):,} orders")
    print(f"  ✓ Total Revenue: ₹{total_revenue:,.2f}")
    print(f"  ✓ Average Order Value: ₹{avg_order_value:,.2f}")
    print(f"  ✓ Total Discounts: ₹{total_discount:,.2f}")
    print(
        f"  ✓ Date Range: {orders_df['order_date'].min()} to {orders_df['order_date'].max()}")

    return orders_df
# ============================================


# ============================================
# MAIN FUNCTION
# ============================================

def main():
    """
    Main execution function.

    Orchestrates data generation and file output:
    1. Creates output directories
    2. Generates customer, product, and order data
    3. Saves data as CSV files
    4. Prints summary statistics
    """
    import time
    start_time = time.time()

    print("\n" + "="*60)
    print("STARTING DATA GENERATION")
    print("="*60)

    # ============================================
    # Step 1: Create output directories
    # ============================================
    print("\n[SETUP] Creating output directories...")

    if not os.path.exists(RAW_DATA_DIR):
        os.makedirs(RAW_DATA_DIR)
        print(f"  ✓ Created directory: {RAW_DATA_DIR}")
    else:
        print(f"  ✓ Directory exists: {RAW_DATA_DIR}")

    # ============================================
    # Step 2: Generate data
    # ============================================
    print("\n" + "-"*60)
    print("DATA GENERATION")
    print("-"*60)

    # Generate customers
    customers_df = generate_customers()

    # Generate products
    products_df = generate_products()

    # Generate orders (depends on customers and products)
    orders_df = generate_orders(customers_df, products_df)

    # ============================================
    # Step 3: Save data to CSV files
    # ============================================
    print("\n" + "-"*60)
    print("SAVING DATA FILES")
    print("-"*60)

    # Define file paths
    customers_file = os.path.join(RAW_DATA_DIR, 'customers.csv')
    products_file = os.path.join(RAW_DATA_DIR, 'products.csv')
    orders_file = os.path.join(RAW_DATA_DIR, 'orders.csv')

    # Save customers
    print(f"\n💾 Saving customers data...")
    customers_df.to_csv(customers_file, index=False)
    print(f"  ✓ Saved: {customers_file}")
    print(
        f"  ✓ Rows: {len(customers_df):,} | Columns: {len(customers_df.columns)}")

    # Save products
    print(f"\n💾 Saving products data...")
    products_df.to_csv(products_file, index=False)
    print(f"  ✓ Saved: {products_file}")
    print(
        f"  ✓ Rows: {len(products_df):,} | Columns: {len(products_df.columns)}")

    # Save orders
    print(f"\n💾 Saving orders data...")
    orders_df.to_csv(orders_file, index=False)
    print(f"  ✓ Saved: {orders_file}")
    print(f"  ✓ Rows: {len(orders_df):,} | Columns: {len(orders_df.columns)}")

    # ============================================
    # Step 4: Print summary
    # ============================================
    print("\n" + "="*60)
    print("GENERATION COMPLETE!")
    print("="*60)

    # Calculate file sizes
    customers_size = os.path.getsize(customers_file) / (1024 * 1024)  # MB
    products_size = os.path.getsize(products_file) / (1024 * 1024)    # MB
    orders_size = os.path.getsize(orders_file) / (1024 * 1024)        # MB
    total_size = customers_size + products_size + orders_size

    print(f"\n📊 SUMMARY:")
    print(
        f"  • Customers: {len(customers_df):,} records ({customers_size:.2f} MB)")
    print(
        f"  • Products: {len(products_df):,} records ({products_size:.2f} MB)")
    print(f"  • Orders: {len(orders_df):,} records ({orders_size:.2f} MB)")
    print(
        f"  • Total: {len(customers_df) + len(products_df) + len(orders_df):,} records ({total_size:.2f} MB)")

    print(f"\n📁 FILES LOCATION:")
    print(f"  → {os.path.abspath(RAW_DATA_DIR)}")

    print(f"\n💰 BUSINESS METRICS:")
    total_revenue = orders_df['total_amount'].sum()
    total_discount = orders_df['discount_applied'].sum()
    avg_order_value = orders_df['total_amount'].mean()
    total_customers = orders_df['customer_id'].nunique()
    total_products_sold = orders_df['product_id'].nunique()

    print(f"  • Total Revenue: ₹{total_revenue:,.2f}")
    print(f"  • Total Discounts Given: ₹{total_discount:,.2f}")
    print(f"  • Average Order Value: ₹{avg_order_value:,.2f}")
    print(f"  • Active Customers: {total_customers:,}")
    print(f"  • Unique Products Sold: {total_products_sold:,}")

    # Calculate execution time
    end_time = time.time()
    execution_time = end_time - start_time

    print(f"\n⏱️  EXECUTION TIME: {execution_time:.2f} seconds")

    print("\n" + "="*60)
    print("✅ SUCCESS! Your e-commerce data is ready for analysis!")
    print("="*60)
    print("\n💡 NEXT STEPS:")
    print("  1. Explore the CSV files in data/raw/")
    print("  2. Open files in Excel/DBeaver to verify data")
    print("  3. Commit changes to Git")
    print("  4. Move to Phase 3: Data Ingestion")
    print("="*60)


# ============================================
# SCRIPT ENTRY POINT
# ============================================

if __name__ == "__main__":
    """
    This block runs only when script is executed directly.
    Not when imported as a module.
    """
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n⚠️  Script interrupted by user (Ctrl+C)")
        print("Exiting gracefully...")
    except Exception as e:
        print(f"\n\n❌ ERROR: An unexpected error occurred!")
        print(f"Error details: {e}")
        print("\nPlease check your code and try again.")
        raise  # Re-raise the exception for debugging
# ============================================
