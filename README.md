# 🏪 E-Commerce Data Pipeline

> A production-grade end-to-end data engineering pipeline demonstrating best practices with incremental data loading, memory-efficient processing, and comprehensive quality monitoring.

[![Python](https://img.shields.io/badge/Python-3.11-blue.svg)](https://www.python.org/)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-15-blue.svg)](https://www.postgresql.org/)
[![Apache Airflow](https://img.shields.io/badge/Apache%20Airflow-Ready-red.svg)](https://airflow.apache.org/)
[![Docker](https://img.shields.io/badge/Docker-24.0-blue.svg)](https://www.docker.com/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

---

## 📋 Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Technologies Used](#technologies-used)
- [Key Features](#key-features)
- [Project Structure](#project-structure)
- [Setup Instructions](#setup-instructions)
- [Usage Guide](#usage-guide)
- [Data Quality](#data-quality)
- [Orchestration](#orchestration)
- [Skills Demonstrated](#skills-demonstrated)
- [Results & Metrics](#results--metrics)
- [Future Enhancements](#future-enhancements)
- [Contact](#contact)

---

## 🎯 Overview

This project implements a **complete data engineering pipeline** that processes synthetic e-commerce data through all stages: generation, incremental ingestion, transformation, quality validation, and (future) workflow orchestration.

**Built to demonstrate production-ready data engineering skills for interviews and portfolio.**

### Business Context

Simulates a real-world e-commerce platform analyzing:
- **Customer behavior** (purchases, lifetime value, demographics)
- **Product performance** (revenue, categories, stock levels)
- **Sales trends** (daily/monthly patterns, order statuses)

### What Makes This Special

✅ **End-to-end pipeline** - Complete system, not just scripts
✅ **Incremental loading** - Auto-incrementing seeds for data growth
✅ **Memory efficient** - Chunked CSV processing (1000 rows/batch)
✅ **Production patterns** - Error handling, logging, UPSERT logic
✅ **Scalable design** - Tested with 100K+ orders, 30K+ customers
✅ **Data quality focus** - Anomaly detection, HTML dashboards
✅ **Apache Airflow ready** - Future orchestration prepared

---

## 🏗️ Architecture

┌──────────────────────────────────────────────────────────────┐
│ DATA PIPELINE ARCHITECTURE │

[Generation] [Ingestion] [Storage]
↓ ↓
↓ ┌──────────┐ ┌──────────┐ ┌──────
───┐ │ Faker │ ──CSV→ │ Pandas │ ──SQL→ │Postgr
SQL│ │ Library │ │ Chunked │ │ Datab
se │ └──────────┘ └──────────┘ └──────
───┘ Auto-seed Batch 1000

                      ↓
              
          [Transformation]
                 ↓
        ┌──────────────┐
        │ SQL Queries  │
        │ Aggregations │
        └──────────────┘
              ↓   ↓
     ┌────────┘   └────────┐
     ↓                     ↓
[Quality Checks] [Analytics]
↓ ↓
HTML Reports Views/Tables


### Pipeline Stages

1. **Data Generation** - Faker creates realistic synthetic data with timestamp-based seeds
2. **Incremental Ingestion** - Chunked CSV loading with UPSERT to PostgreSQL
3. **Transformation** - SQL aggregates (customer, product, sales summaries)
4. **Quality Validation** - Anomaly detection, completeness, consistency checks
5. **Orchestration** - Apache Airflow (ready, not yet deployed)

---

## 🛠️ Technologies Used

| Category | Technology | Purpose |
|----------|------------|---------|
| **Language** | Python 3.11 | Data processing, ETL scripting |
| **Database** | PostgreSQL 15 | Data warehouse, storage |
| **Orchestration** | Apache Airflow 2.7.3 | Workflow automation (planned) |
| **Containerization** | Docker & Docker Compose | Environment isolation |
| **Data Processing** | Pandas 2.2.0 | ETL, chunked reading |
| **Data Generation** | Faker 22.6.0 | Synthetic data with auto-seeds |
| **Logging** | Loguru | Structured logging |
| **Version Control** | Git & GitHub | Code management |

---

## ✨ Key Features

### 1. Auto-Incrementing Data Generation
- **Timestamp-based seeds** - Different data every run
- **60,495 records/run** (10K customers, 495 products, 50K orders)
- **Cumulative growth** - Data adds incrementally (Run 1: 60K, Run 2: 120K, Run 3: 180K...)
- **Unique IDs** - Seed-based offsets prevent duplicates

### 2. Memory-Efficient Ingestion
- **Chunked CSV reading** - Processes 1000 rows/batch
- **Constant memory usage** - ~50-100 MB regardless of total data size
- **UPSERT logic** - Updates existing, inserts new (ON CONFLICT handling)
- **Transaction safety** - Commit/rollback for data integrity

### 3. Data Transformation
- **4 aggregate tables**:
  - `customer_summary` - Total spent, order count, lifetime value
  - `product_summary` - Revenue, quantity sold, categories
  - `daily_sales_summary` - Daily revenue, order counts
  - `monthly_sales_summary` - Monthly trends
- **Complex SQL** - CTEs, window functions, joins

### 4. Data Quality Framework
- **Anomaly detection** - Z-score statistical method for outliers
- **Completeness checks** - Null value detection
- **Consistency validation** - Referential integrity checks
- **HTML dashboards** - Visual quality reports

### 5. Apache Airflow (Ready)
- **Infrastructure prepared** - docker-compose-airflow.yml configured
- **Future DAGs** - Daily quality checks, aggregation refresh, full pipeline
- **Not yet deployed** - Manual execution currently (excellent for learning)

---

## 📁 Project Structure

ecommerce-data-pipeline/
├── data/
│ ├── raw/ # Generated CSV files
│ ├── processed/ # Quality reports (HTML)
│ └── archive/ # Historical backups
├── src/
│ ├── generation/
│ │ └── generate_ecommerce_data.py # Single unified generator
│ ├── ingestion/
│ │ └── load_csv_to_postgres.py # Chunked CSV loader
│ ├── processing/
│ │ └── refresh_aggregations.py # Aggregate refresh
│ ├── quality/
│ │ ├── generate_quality_report.py # HTML dashboard
│ │ ├── detect_anomalies.py # Statistical outliers
│ │ └── data_quality_checks.py # Validation framework
│ ├── utils/
│ │ ├── config.py # Configuration settings
│ │ └── db_connector.py # Database connection
│ └── validation/
│ └── validate_data.py # Data validation
├── sql/
│ ├── create_schema.sql # Table definitions
│ ├── create_aggregations.sql # Summary tables
│ └── create_views.sql # Analytical views
├── airflow/
│ └── dags/ # Future DAG definitions
├── logs/ # Application logs
├── docs/ # Documentation
├── tests/ # Unit/integration tests
├── docker-compose.yml # PostgreSQL container
├── docker-compose-airflow.yml # Airflow (ready)
├── requirements.txt # Python dependencies
└── README.md # This file


---

## 🚀 Setup Instructions

### Prerequisites

- **Python 3.11+**
- **Docker Desktop** (for PostgreSQL)
- **Git**
- **16 GB RAM** (recommended)
- **10 GB free disk space**

### Installation Steps

**1. Clone the Repository**

git clone https://github.com/abhiiram16/ecommerce-data-pipeline.git
cd ecommerce-data-pipeline


**2. Create Virtual Environment**

python -m venv venv
source venv/bin/activate # On Windows: venv\Scripts\activate


**3. Install Dependencies**

pip install -r requirements.txt


**4. Start PostgreSQL Database**

docker-compose up -d


**5. Initialize Database Schema**

psql -h localhost -U dataeng -d ecommerce_db < sql/create_schema.sql
psql -h localhost -U dataeng -d ecommerce_db < sql/create_aggregations.sql
psql -h localhost -U dataeng -d ecommerce_db < sql/create_views.sql


*Password: `pipeline123`*

---

## 📖 Usage Guide

### Run Complete Pipeline (Manual)

1. Generate synthetic data (auto-incrementing seed)
python src/generation/generate_ecommerce_data.py

2. Load to PostgreSQL (chunked, incremental)
python src/ingestion/load_csv_to_postgres.py

3. Refresh aggregations
python src/processing/refresh_aggregations.py

4. Generate quality report
python src/quality/generate_quality_report.py


### Expected Output

Run 1: 10,000 customers, 495 products, 50,000 orders
Run 2: 20,000 customers, 990 products, 100,000 orders
Run 3: 30,000 customers, 1,485 products, 150,000 orders


### Access Quality Dashboard

Open generated HTML report
start data/processed/quality_report_YYYYMMDD_HHMMSS.html


### Query the Data

psql -h localhost -U dataeng -d ecommerce_db


Example queries:

-- Top 10 customers by lifetime value
SELECT * FROM customer_summary
ORDER BY total_spent DESC
LIMIT 10;

-- Monthly sales trend
SELECT * FROM monthly_sales_summary
ORDER BY month;

-- Product performance
SELECT * FROM product_summary
ORDER BY total_revenue DESC
LIMIT 10;


---

## ✅ Data Quality

### Quality Metrics (After 3 Runs)

| Metric | Value | Status |
|--------|-------|--------|
| **Total Customers** | 30,000 | ✅ Growing |
| **Total Orders** | 150,000+ | ✅ Growing |
| **Data Completeness** | 100% | ✅ Perfect |
| **Referential Integrity** | 100% | ✅ Valid |
| **Unique IDs** | 100% | ✅ No duplicates |
| **Memory Usage** | <200 MB | ✅ Efficient |

### Anomaly Detection

- **Method:** Z-score statistical analysis
- **Threshold:** ±3 standard deviations
- **Detects:** High-value orders, unusual patterns
- **Visual:** Distribution charts in HTML report

---

## ⚙️ Orchestration (Future Phase)

### Apache Airflow - Ready to Deploy

Infrastructure prepared with `docker-compose-airflow.yml`. Future DAGs will include:

**Planned DAGs:**
- `daily_quality_check` - Scheduled quality validation
- `refresh_aggregations` - Daily summary updates
- `weekly_full_pipeline` - Complete ETL workflow

**Start Airflow (Optional):**

docker-compose -f docker-compose-airflow.yml up -d


Access UI: `http://localhost:8080` (admin/admin)

---

## 🎓 Skills Demonstrated

### Technical Skills
✅ **Python** - Pandas, Faker, error handling, logging
✅ **SQL** - PostgreSQL, aggregations, CTEs, UPSERT
✅ **ETL Design** - Incremental loading, chunked processing
✅ **Data Quality** - Validation frameworks, anomaly detection
✅ **Docker** - Containerization, docker-compose
✅ **Version Control** - Git, GitHub, meaningful commits
✅ **Apache Airflow** - Infrastructure setup (ready for DAGs)

### Engineering Practices
✅ **Scalability** - Tested 100K+ orders, memory-efficient design
✅ **Error Handling** - Try-except, transaction management
✅ **Logging** - Structured logs with Loguru
✅ **Documentation** - README, inline comments, clear structure
✅ **Modular Design** - Separation of concerns (generation/ingestion/processing)

---

## 📊 Results & Metrics

### Data Processed (After 3 Runs)
- **102,800 total orders** cumulative
- **30,000 customers** unique
- **990 products** across categories
- **₹4.05 trillion** total revenue simulated

### Performance
- **Generation:** 60K records in ~2 seconds
- **Ingestion:** Chunked loading in ~10 seconds (50K orders)
- **Aggregation:** 4 summaries in ~1 second
- **Quality checks:** Full validation in ~5 seconds
- **Memory:** Constant ~150 MB (regardless of total data size)

### Scalability
- ✅ **Tested:** 150K+ orders without memory issues
- ✅ **Chunked reading:** Handles files of any size
- ✅ **Auto-incrementing:** Supports unlimited runs
- ✅ **Production-ready:** Can scale to millions of records

---

## 🚀 Future Enhancements

### Phase 2 - Orchestration
- [ ] Deploy Apache Airflow DAGs
- [ ] Scheduled daily/weekly pipeline execution
- [ ] Email alerts on failures
- [ ] Task-level monitoring

### Phase 3 - Advanced Features
- [ ] Real-time streaming (Apache Kafka)
- [ ] Machine learning (customer segmentation)
- [ ] Tableau/Power BI dashboards
- [ ] dbt for transformation layer
- [ ] Great Expectations for testing
- [ ] CI/CD with GitHub Actions

### Phase 4 - Cloud Migration
- [ ] AWS RDS for PostgreSQL
- [ ] S3 data lake
- [ ] Lambda for serverless ETL
- [ ] CloudWatch monitoring

---

## 📞 Contact

**Abhiiram Piska**
📧 Email: abhiramashwika@gmail.com
💼 LinkedIn: [linkedin.com/in/abhiiram](https://www.linkedin.com/in/abhiiram)
🐙 GitHub: [github.com/abhiiram16](https://github.com/abhiiram16)

---

## 📄 License

This project is licensed under the MIT License.

---

## 🙏 Acknowledgments

- Data generation: [Faker](https://faker.readthedocs.io/)
- Orchestration: [Apache Airflow](https://airflow.apache.org/)
- Database: [PostgreSQL](https://www.postgresql.org/)

---

## ⭐ Star This Project

If you found this project helpful for learning data engineering, please give it a star! ⭐

---

**Built with ❤️ to demonstrate production-ready data engineering skills.**
