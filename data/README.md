# Data Directory

This directory contains e-commerce data files generated for the pipeline.

## Structure

data/
├── raw/ # Original unprocessed data (CSV files)
├── processed/ # Cleaned and transformed data
└── archive/ # Historical backups


## Generated Data Files

**Location**: `data/raw/`

### customers.csv
- **Rows**: 10,000
- **Columns**: 11 (customer_id, first_name, last_name, email, phone, city, state, pincode, registration_date, age, gender)
- **Description**: Synthetic customer data with Indian demographics

**Sample:**
customer_id,first_name,last_name,email,phone,city,state,pincode,registration_date,age,gender
1001,Rajesh,Kumar,rajesh.kumar347@gmail.com,+91-9876543210,Mumbai,Maharashtra,400001,2023-08-15,28,Male
1002,Priya,Sharma,priya.sharma82@yahoo.com,+91-8123456789,Delhi,Delhi,110001,2024-01-20,25,Female


### products.csv
- **Rows**: 495
- **Columns**: 9 (product_id, product_name, category, subcategory, brand, price, cost, stock_quantity, supplier)
- **Description**: Product catalog across Electronics, Fashion, and Home & Kitchen

**Sample:**
product_id,product_name,category,subcategory,brand,price,cost,stock_quantity,supplier
2001,Samsung Galaxy Pro,Electronics,Smartphones,Samsung,74999.00,55000.00,150,Tech Supplies India
2002,Casual Shirt - Blue - L,Fashion,Mens Clothing,Casual,1299.50,974.62,250,Fashion Hub Pvt Ltd


### orders.csv
- **Rows**: 50,000
- **Columns**: 11 (order_id, customer_id, product_id, order_date, quantity, unit_price, total_amount, discount_applied, payment_method, order_status, shipping_city)
- **Description**: Transaction data with realistic patterns (weekends, Diwali, customer segments)

**Sample:**
order_id,customer_id,product_id,order_date,quantity,unit_price,total_amount,discount_applied,payment_method,order_status,shipping_city
3001,1523,2145,2024-08-15 20:35:42,1,74999.00,56249.25,18749.75,UPI,Delivered,Mumbai
3002,2847,2089,2024-09-02 14:22:18,2,1299.50,2339.10,259.90,Credit Card,Delivered,Bangalore


## How to Generate Data

Run the data generator script:

python src/generate_ecommerce_data.py


This will create all CSV files in `data/raw/` directory.

## Data Characteristics

### Realistic Patterns:
- **Customer Segmentation**: VIP (20%), Regular (50%), Occasional (30%)
- **Seasonal Trends**: 50% more orders during Diwali period
- **Weekend Patterns**: 30% more orders on Saturdays and Sundays
- **Time Patterns**: Peak orders at 8 PM, low at 3 AM
- **Payment Methods**: UPI (50%), Credit Card (20%), Debit Card (15%), COD (10%), Net Banking (5%)

### Data Quality:
- ✅ No NULL values
- ✅ No duplicate IDs
- ✅ Valid foreign key relationships
- ✅ Realistic value ranges
- ✅ Proper date formats

## Validation

Run data validation:

python src/validate_data.py


---

**Last Generated**: November 4, 2025  
**Script**: `src/generate_ecommerce_data.py`  
**Validation**: `src/validate_data.py`




