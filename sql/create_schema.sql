-- ============================================
-- E-Commerce Data Pipeline - Database Schema
-- ============================================
-- Author: Abhiiram
-- Date: November 5, 2025
-- Description: Creates tables for customers, products, and orders
-- Drop tables if they exist (for clean re-runs)
DROP TABLE IF EXISTS orders CASCADE;
DROP TABLE IF EXISTS products CASCADE;
DROP TABLE IF EXISTS customers CASCADE;
-- ============================================
-- CUSTOMERS TABLE
-- ============================================
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
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
-- Add index on email for faster lookups
CREATE INDEX idx_customers_email ON customers(email);
CREATE INDEX idx_customers_city ON customers(city);
-- ============================================
-- PRODUCTS TABLE
-- ============================================
CREATE TABLE products (
    product_id INTEGER PRIMARY KEY,
    product_name VARCHAR(255) NOT NULL,
    category VARCHAR(100) NOT NULL,
    subcategory VARCHAR(100),
    brand VARCHAR(100),
    price DECIMAL(10, 2) NOT NULL CHECK (price >= 0),
    cost DECIMAL(10, 2) CHECK (cost >= 0),
    stock_quantity INTEGER DEFAULT 0 CHECK (stock_quantity >= 0),
    supplier VARCHAR(255),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
-- Add indexes for common queries
CREATE INDEX idx_products_category ON products(category);
CREATE INDEX idx_products_price ON products(price);
-- ============================================
-- ORDERS TABLE
-- ============================================
CREATE TABLE orders (
    order_id INTEGER PRIMARY KEY,
    customer_id INTEGER NOT NULL,
    product_id INTEGER NOT NULL,
    order_date TIMESTAMP NOT NULL,
    quantity INTEGER NOT NULL CHECK (quantity > 0),
    unit_price DECIMAL(10, 2) NOT NULL CHECK (unit_price >= 0),
    total_amount DECIMAL(10, 2) NOT NULL CHECK (total_amount >= 0),
    discount_applied DECIMAL(10, 2) DEFAULT 0 CHECK (discount_applied >= 0),
    payment_method VARCHAR(50),
    order_status VARCHAR(50),
    shipping_city VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    -- Foreign key constraints
    CONSTRAINT fk_customer FOREIGN KEY (customer_id) REFERENCES customers(customer_id) ON DELETE CASCADE,
    CONSTRAINT fk_product FOREIGN KEY (product_id) REFERENCES products(product_id) ON DELETE CASCADE
);
-- Add indexes for foreign keys and common queries
CREATE INDEX idx_orders_customer ON orders(customer_id);
CREATE INDEX idx_orders_product ON orders(product_id);
CREATE INDEX idx_orders_date ON orders(order_date);
CREATE INDEX idx_orders_status ON orders(order_status);
-- ============================================
-- SUMMARY VIEWS (OPTIONAL - FOR ANALYTICS)
-- ============================================
-- View: Customer order summary
CREATE OR REPLACE VIEW customer_order_summary AS
SELECT c.customer_id,
    c.first_name,
    c.last_name,
    c.email,
    COUNT(o.order_id) AS total_orders,
    SUM(o.total_amount) AS total_spent,
    AVG(o.total_amount) AS avg_order_value,
    MAX(o.order_date) AS last_order_date
FROM customers c
    LEFT JOIN orders o ON c.customer_id = o.customer_id
GROUP BY c.customer_id,
    c.first_name,
    c.last_name,
    c.email;
-- View: Product sales summary
CREATE OR REPLACE VIEW product_sales_summary AS
SELECT p.product_id,
    p.product_name,
    p.category,
    p.brand,
    p.price,
    COUNT(o.order_id) AS times_ordered,
    SUM(o.quantity) AS total_units_sold,
    SUM(o.total_amount) AS total_revenue
FROM products p
    LEFT JOIN orders o ON p.product_id = o.product_id
GROUP BY p.product_id,
    p.product_name,
    p.category,
    p.brand,
    p.price;
-- ============================================
-- VERIFICATION QUERIES
-- ============================================
-- Count tables
SELECT 'Tables created successfully!' AS status;
-- Show table structure
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