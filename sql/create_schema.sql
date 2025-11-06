-- ========================================
-- E-COMMERCE DATA PIPELINE - DATABASE SCHEMA
-- ========================================
-- Author: Abhiiram
-- Date: November 6, 2025
-- Description: Creates base tables for customers, products, and orders
-- ========================================
-- Drop tables if they exist (for clean re-runs)
DROP TABLE IF EXISTS orders CASCADE;
DROP TABLE IF EXISTS products CASCADE;
DROP TABLE IF EXISTS customers CASCADE;
-- ========================================
-- CUSTOMERS TABLE
-- ========================================
CREATE TABLE customers (
    customer_id INTEGER PRIMARY KEY,
    first_name VARCHAR(100) NOT NULL,
    last_name VARCHAR(100) NOT NULL,
    email VARCHAR(255) NOT NULL UNIQUE,
    phone VARCHAR(20),
    city VARCHAR(100),
    state VARCHAR(100),
    pincode VARCHAR(10),
    registration_date DATE,
    age INTEGER CHECK (
        age >= 18
        AND age <= 120
    ),
    gender VARCHAR(20),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX idx_customers_email ON customers(email);
CREATE INDEX idx_customers_city ON customers(city);
CREATE INDEX idx_customers_registration_date ON customers(registration_date);
-- ========================================
-- PRODUCTS TABLE
-- ========================================
CREATE TABLE products (
    product_id INTEGER PRIMARY KEY,
    product_name VARCHAR(255) NOT NULL,
    category VARCHAR(100) NOT NULL,
    subcategory VARCHAR(100),
    brand VARCHAR(100),
    price NUMERIC(10, 2) NOT NULL CHECK (price > 0),
    cost NUMERIC(10, 2) CHECK (cost > 0),
    stock_quantity INTEGER DEFAULT 0 CHECK (stock_quantity >= 0),
    supplier VARCHAR(255),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX idx_products_category ON products(category);
CREATE INDEX idx_products_brand ON products(brand);
CREATE INDEX idx_products_price ON products(price);
-- ========================================
-- ORDERS TABLE
-- ========================================
CREATE TABLE orders (
    order_id INTEGER PRIMARY KEY,
    customer_id INTEGER NOT NULL,
    product_id INTEGER NOT NULL,
    order_date TIMESTAMP NOT NULL,
    quantity INTEGER NOT NULL CHECK (quantity > 0),
    unit_price NUMERIC(10, 2) NOT NULL CHECK (unit_price > 0),
    total_amount NUMERIC(12, 2) NOT NULL CHECK (total_amount > 0),
    payment_method VARCHAR(50),
    order_status VARCHAR(50) DEFAULT 'Processing',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_orders_customer FOREIGN KEY (customer_id) REFERENCES customers(customer_id) ON DELETE CASCADE,
    CONSTRAINT fk_orders_product FOREIGN KEY (product_id) REFERENCES products(product_id) ON DELETE CASCADE
);
CREATE INDEX idx_orders_customer_id ON orders(customer_id);
CREATE INDEX idx_orders_product_id ON orders(product_id);
CREATE INDEX idx_orders_order_date ON orders(order_date);
CREATE INDEX idx_orders_status ON orders(order_status);
CREATE INDEX idx_orders_total_amount ON orders(total_amount);
-- ========================================
-- VERIFICATION QUERIES
-- ========================================
SELECT 'Tables created successfully!' AS status;
SELECT table_name,
    (
        SELECT COUNT(*)
        FROM information_schema.columns
        WHERE table_name = t.table_name
    ) AS column_count
FROM information_schema.tables t
WHERE table_schema = 'public'
    AND table_type = 'BASE TABLE'
ORDER BY table_name;