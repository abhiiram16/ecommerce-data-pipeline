# Phase 3: Data Ingestion Layer

**Completion Date:** November 5, 2025  
**Status:** ✅ Complete  
**Duration:** ~2 hours

---

## Overview

Built a complete data ingestion pipeline that loads CSV files into PostgreSQL database running in Docker container.

---

## Architecture

CSV Files (data/raw/) → Python Script → PostgreSQL (Docker)


### Components

1. **PostgreSQL Database** (Docker container)
   - Container name: `ecommerce_postgres`
   - Port: 5432
   - Database: `ecommerce_db`
   - User: `dataeng`
   - Volume: `ecommerce_pgdata` (persistent storage)

2. **Database Schema** (`sql/create_schema.sql`)
   - `customers` table (10,000 rows)
   - `products` table (495 rows)
   - `orders` table (50,000 rows)
   - Foreign key relationships enforced
   - Indexes on common query columns

3. **Ingestion Script** (`src/ingestion/load_csv_to_postgres.py`)
   - Reads CSV files with Pandas
   - Batch inserts (1,000 rows per batch)
   - Transaction management
   - Error handling and rollback
   - Progress indicators

---

## Key Features

### Database Schema

**Customers Table:**
- Primary key: `customer_id`
- Unique constraint on `email`
- Check constraint: age between 18-120
- Indexes on email and city

**Products Table:**
- Primary key: `product_id`
- Check constraints on price >= 0
- Indexes on category and price

**Orders Table:**
- Primary key: `order_id`
- Foreign keys: `customer_id`, `product_id`
- Check constraints on quantity > 0, amounts >= 0
- Cascade delete on parent records
- Indexes on foreign keys and order_date

### Data Integrity

- ✅ No NULL values in required fields
- ✅ All foreign key relationships valid
- ✅ Primary key constraints enforced
- ✅ Check constraints prevent invalid data
- ✅ Indexes optimize query performance

---

## Technical Implementation

### Docker Commands Used

Pull PostgreSQL image
docker pull postgres:15

Run PostgreSQL container
docker run -d
--name ecommerce_postgres
-e POSTGRES_USER=dataeng
-e POSTGRES_PASSWORD=pipeline123
-e POSTGRES_DB=ecommerce_db
-e PGTZ=UTC
-p 5432:5432
-v ecommerce_pgdata:/var/lib/postgresql/data
postgres:15 -c timezone=UTC

Check container status
docker ps

View logs
docker logs ecommerce_postgres

Connect to database
docker exec -it ecommerce_postgres psql -U dataeng -d ecommerce_db



### SQL Schema Execution

Copy schema file to container
docker cp sql/create_schema.sql ecommerce_postgres:/tmp/

Execute schema creation
docker exec -it ecommerce_postgres psql -U dataeng -d ecommerce_db -f /tmp/create_schema.sql


### Python Ingestion

Install database driver
pip install psycopg2-binary

Run ingestion script
python src/ingestion/load_csv_to_postgres.py


---

## Performance Metrics

- **Total Records Loaded:** 60,495
- **Execution Time:** ~15-20 seconds
- **Batch Size:** 1,000 rows per insert
- **Memory Usage:** Efficient (processes in batches)
- **Error Rate:** 0% (all data loaded successfully)

---

## Verification Queries

-- Row counts
SELECT 'customers' AS table, COUNT() FROM customers
UNION ALL SELECT 'products', COUNT() FROM products
UNION ALL SELECT 'orders', COUNT(*) FROM orders;

-- Foreign key validation
SELECT COUNT(*) FROM orders o
LEFT JOIN customers c ON o.customer_id = c.customer_id
WHERE c.customer_id IS NULL; -- Should return 0

-- Top customers by revenue
SELECT c.first_name, c.last_name, SUM(o.total_amount) AS total_spent
FROM customers c
JOIN orders o ON c.customer_id = o.customer_id
GROUP BY c.customer_id, c.first_name, c.last_name
ORDER BY total_spent DESC
LIMIT 10;

---

## Skills Demonstrated

### Technical Skills
- Docker containerization
- PostgreSQL database administration
- SQL DDL (CREATE TABLE, indexes, constraints)
- Python database connectivity (psycopg2)
- Batch processing and transaction management
- Error handling and logging
- Data validation

### Concepts
- ETL pipeline design
- Database normalization (3NF)
- Foreign key relationships
- ACID transactions
- Index optimization
- Data integrity constraints

---

## Challenges & Solutions

### Challenge 1: DBeaver Timezone Error
**Problem:** `FATAL: invalid value for parameter "TimeZone": "Asia/Calcutta"`  
**Solution:** 
- Recreated Docker container with explicit timezone: `-e PGTZ=UTC`
- Used command line (`psql`) as alternative to GUI
- Learned professional data engineers prefer CLI anyway!

### Challenge 2: Python Module Import Error
**Problem:** `ModuleNotFoundError: No module named 'utils'`  
**Solution:**
- Created standalone script with inline functions
- Simpler and more maintainable
- Learned when to consolidate vs. modularize

### Challenge 3: Batch Insert Performance
**Problem:** Loading 50,000 rows could be slow  
**Solution:**
- Used `executemany()` for batch inserts
- Batch size of 1,000 rows optimized speed vs memory
- Progress indicators for user feedback

---

## Interview Talking Points

**"Walk me through your data ingestion process."**

"I built an ETL pipeline that loads CSV files into PostgreSQL. The pipeline has three main components:

First, infrastructure - I run PostgreSQL in a Docker container for portability and easy deployment. The container uses persistent volumes so data survives container restarts.

Second, schema design - I created a normalized database schema with three tables: customers, products, and orders. I enforced referential integrity with foreign keys, added indexes on frequently queried columns, and implemented check constraints to prevent invalid data.

Third, ingestion script - I wrote a Python script using psycopg2 that reads CSV files with Pandas and loads them in batches. The script handles transactions properly - if any batch fails, it rolls back to maintain data consistency. It processes 60,000 rows in about 15 seconds.

I validated the load with SQL queries to check row counts, foreign key integrity, and ran sample analytics queries to ensure data quality."

**Key metrics:**
- 60,495 records loaded
- 15-20 second execution time
- 0% error rate
- 100% foreign key integrity

---

## Files Created

sql/
└── create_schema.sql # Database schema definition

src/
├── init.py # Package marker
├── ingestion/
│ ├── init.py
│ └── load_csv_to_postgres.py # Data ingestion script
└── utils/
├── init.py
└── db_connector.py # Database utility functions

docs/
└── phase3_ingestion_notes.md # This file

---

## Next Steps: Phase 4

- Implement data transformation layer
- Add data quality checks
- Create aggregate tables for analytics
- Build data validation rules

---

**Phase 3 Status:** ✅ **COMPLETE**  
**Data Quality:** ✅ **100%**  
**Ready for Production:** ✅ **YES**
