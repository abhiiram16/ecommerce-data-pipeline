# 🏪 E-Commerce Data Pipeline

> A production-grade end-to-end data pipeline demonstrating data engineering best practices with synthetic e-commerce data.

[![Python](https://img.shields.io/badge/Python-3.11-blue.svg)](https://www.python.org/)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-15-blue.svg)](https://www.postgresql.org/)
[![Apache Airflow](https://img.shields.io/badge/Apache%20Airflow-2.7.3-red.svg)](https://airflow.apache.org/)
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
- [Cloud Deployment](#cloud-deployment)
- [Skills Demonstrated](#skills-demonstrated)
- [Results & Metrics](#results--metrics)
- [Future Enhancements](#future-enhancements)
- [Contact](#contact)

---

## 🎯 Overview

This project implements a complete **data engineering pipeline** that processes synthetic e-commerce data through all stages of the data lifecycle: generation, ingestion, transformation, quality validation, and orchestration.

**Built to demonstrate production-ready data engineering skills for interviews and portfolio showcase.**

### Business Context

Simulates a real-world e-commerce platform analyzing:
- **Customer behavior** (purchases, lifetime value, RFM segmentation)
- **Product performance** (revenue, margin, bestsellers)
- **Sales trends** (daily/monthly patterns, seasonality)

### What Makes This Special

✅ **End-to-end pipeline** - Not just a script, a complete system  
✅ **Production patterns** - Error handling, logging, monitoring  
✅ **Scalable architecture** - Designed for growth (60K → 6M records)  
✅ **Automated workflows** - Apache Airflow orchestration  
✅ **Data quality focus** - 4-dimension validation framework  
✅ **Cloud-ready** - AWS deployment strategy documented  

---

## 🏗️ Architecture

┌─────────────────────────────────────────────────────────────────┐
│ DATA PIPELINE ARCHITECTURE │

[Data Generation] [Ingestion] [Storage]
↓ ↓
↓ ┌──────────┐ ┌──────────┐ ┌──────────
───┐ │ Faker │ ────CSV──→ │ Python │ ───SQL──→│ PostgreS
L │ │ Library │ │ Pandas │ │ Databas
│ └──────────┘ └──────────┘ └──────────
───┘ 60K records Batch Load 15 tables
v
ews


↓

┌──────────────┐
│Transforma
ion│ │
QL Queries │ └────
─┬───────┘
↓ ┌────────────────────────────────────────────────────┐
│ │ ↓
↓ ┌─────────┐

### Pipeline Stages

1. **Data Generation** - Faker library creates realistic synthetic data
2. **Ingestion** - Python scripts load CSV to PostgreSQL
3. **Transformation** - SQL creates aggregates and analytical views
4. **Quality Validation** - 4-dimension checks (completeness, validity, consistency, uniqueness)
5. **Orchestration** - Apache Airflow schedules and monitors workflows

---

## 🛠️ Technologies Used

| Category | Technology | Purpose |
|----------|------------|---------|
| **Language** | Python 3.11 | Data processing, scripting |
| **Database** | PostgreSQL 15 | Data warehouse, storage |
| **Orchestration** | Apache Airflow 2.7.3 | Workflow automation |
| **Containerization** | Docker & Docker Compose | Environment isolation |
| **Data Processing** | Pandas 2.2.0 | ETL transformations |
| **Data Generation** | Faker 22.6.0 | Synthetic data creation |
| **Version Control** | Git & GitHub | Code management |
| **Documentation** | Markdown | Project docs |

---

## ✨ Key Features

### 1. Synthetic Data Generation
- **60,495 total records** across 3 entities
- **10,000 customers** with realistic demographics
- **495 products** across multiple categories
- **50,000 orders** with temporal patterns

### 2. Automated ETL Pipeline
- Batch processing with error handling
- Transaction management for data integrity
- Logging for observability
- Retry logic for resilience

### 3. Data Transformation
- **4 aggregate tables** (customer summary, product summary, daily sales, monthly sales)
- **5 analytical views** for reporting
- Complex SQL with CTEs, window functions, joins

### 4. Data Quality Framework
- **4-dimension validation**:
  - Completeness: 100%
  - Validity: 100%
  - Consistency: 99%
  - Uniqueness: 100%
- **Anomaly detection** using Z-score statistical method
- **HTML quality dashboard** with visual reports

### 5. Workflow Orchestration
- **3 Apache Airflow DAGs**:
  - `daily_quality_check` - Parallel quality validation
  - `refresh_aggregations` - Fan-out-fan-in pattern
  - `weekly_full_pipeline` - Sequential 6-stage ETL
- Scheduled execution (cron expressions)
- Error handling & automatic retries
- Monitoring UI with task-level visibility

### 6. Cloud Architecture (Documented)
- AWS deployment strategy
- Cost analysis ($18.67/month)
- Scalability plan (100x growth path)
- Security best practices
- Migration roadmap

---

## 📁 Project Structure

ecommerce-data-pipeline/
├── data/
│ ├── raw/ # Generated CSV files
│ └── processed/ # Quality reports
├── src/
│ ├── generation/ # Data generators
│ │ ├── generate_customers.py
│ │ ├── generate_products.py
│ │ └── generate_orders.py
│ ├── ingestion/ # ETL scripts
│ │ ├── load_customers.py
│ │ ├── load_products.py
│ │ └── load_orders.py
│ ├── processing/ # Transformation scripts
│ │ └── refresh_aggregations.py
│ └── quality/ # Quality validation
│ ├── data_quality_checks.py
│ ├── detect_anomalies.py
│ └── generate_quality_report.py
├── sql/
│ ├── schema.sql # Table definitions
│ ├── aggregations.sql # Summary tables
│ └── views.sql # Analytical views
├── airflow/
│ └── dags/ # Airflow DAG definitions
│ ├── daily_quality_check.py
│ ├── refresh_aggregations.py
│ └── weekly_full_pipeline.py
├── docs/ # Phase documentation
├── docker-compose.yml # PostgreSQL container
├── docker-compose-airflow.yml # Airflow containers
├── requirements.txt # Python dependencies
└── README.md # This file

---

## 🚀 Setup Instructions

### Prerequisites

- **Python 3.11+**
- **Docker Desktop** (for PostgreSQL & Airflow)
- **Git**
- **VS Code** (recommended)

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

psql -h localhost -U dataeng -d ecommerce_db < sql/schema.sql
psql -h localhost -U dataeng -d ecommerce_db < sql/aggregations.sql
psql -h localhost -U dataeng -d ecommerce_db < sql/views.sql


*Password: `pipeline123`*

**6. Generate Synthetic Data**

python src/generation/generate_customers.py
python src/generation/generate_products.py
python src/generation/generate_orders.py


**7. Load Data to Database**

python src/ingestion/load_customers.py
python src/ingestion/load_products.py
python src/ingestion/load_orders.py

**8. Start Apache Airflow (Optional)**

docker-compose -f docker-compose-airflow.yml up -d


Access Airflow UI: `http://localhost:8080` (admin/admin)

---

## 📖 Usage Guide

### Run Complete Pipeline

Generate data
python src/generation/generate_customers.py
python src/generation/generate_products.py
python src/generation/generate_orders.py

Ingest to database
python src/ingestion/load_customers.py
python src/ingestion/load_products.py
python src/ingestion/load_orders.py

Refresh aggregations
python src/processing/refresh_aggregations.py

Run quality checks
python src/quality/data_quality_checks.py

Detect anomalies
python src/quality/detect_anomalies.py

Generate HTML report
python src/quality/generate_quality_report.py

### Access Quality Dashboard

Open: `data/processed/data_quality_report.html` in browser

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

### Validation Dimensions

| Dimension | Score | Description |
|-----------|-------|-------------|
| **Completeness** | 100% | No null values in critical fields |
| **Validity** | 100% | All values within expected ranges |
| **Consistency** | 99% | Referential integrity maintained |
| **Uniqueness** | 100% | No duplicate primary keys |

### Overall Score: 95% (Grade A)

### Anomaly Detection

- Z-score statistical method
- Identifies outliers in order amounts
- Flags suspicious patterns
- Visual distribution charts

---

## ⚙️ Orchestration

### Apache Airflow DAGs

#### 1. Daily Quality Check
- **Schedule:** Every day at 2 AM IST
- **Tasks:** 5 (1 schema + 3 parallel validations + 1 scoring)
- **Pattern:** Fan-out (parallel execution)
- **Duration:** ~7 minutes

#### 2. Refresh Aggregations
- **Schedule:** Every day at 3 AM IST
- **Tasks:** 5 (4 parallel refreshes + 1 verification)
- **Pattern:** Fan-out-fan-in
- **Duration:** ~2 seconds

#### 3. Weekly Full Pipeline
- **Schedule:** Every Sunday at 4 AM IST
- **Tasks:** 6 (sequential stages)
- **Pattern:** Linear pipeline
- **Duration:** ~5 seconds

### Monitoring

Access Airflow UI at `http://localhost:8080` for:
- Real-time task status
- Execution logs
- Task duration metrics
- Failure alerts

---

## ☁️ Cloud Deployment

AWS deployment architecture documented in `docs/cloud_deployment_strategy.md`

**Highlights:**
- **Cost:** $18.67/month (RDS + S3 + Lambda)
- **Scalability:** 100x growth path (60K → 6M records)
- **Services:** RDS PostgreSQL, S3 Data Lake, Lambda ETL, CloudWatch Monitoring
- **Migration:** 4-day deployment plan
- **Security:** VPC isolation, encryption at rest/transit, IAM roles

---

## 🎓 Skills Demonstrated

### Technical Skills
✅ **Python** - Pandas, Faker, scripting, error handling  
✅ **SQL** - PostgreSQL, complex queries, CTEs, window functions  
✅ **Apache Airflow** - DAG design, scheduling, orchestration patterns  
✅ **Docker** - Containerization, Docker Compose  
✅ **ETL/ELT** - Data pipelines, transformation logic  
✅ **Data Quality** - Validation frameworks, anomaly detection  
✅ **Version Control** - Git, GitHub, meaningful commits  

### Engineering Practices
✅ **Modular design** - Separation of concerns  
✅ **Error handling** - Try-except, logging  
✅ **Documentation** - README, inline comments, phase notes  
✅ **Testing** - Data validation, quality checks  
✅ **Scalability thinking** - Cloud architecture, growth planning  

---

## 📊 Results & Metrics

### Data Processed
- **60,495 total records** across 3 entities
- **15 database objects** (tables + views)
- **4 aggregate tables** with business metrics
- **100% data quality** maintained

### Performance
- **Ingestion:** 60K records in ~30 seconds
- **Aggregation:** 4 summaries in ~15 seconds
- **Quality checks:** 4 dimensions in ~10 seconds
- **End-to-end pipeline:** <2 minutes total

### Automation
- **100% automated** workflow execution
- **3 Airflow DAGs** running on schedule
- **0 manual interventions** required
- **Real-time monitoring** enabled

---

## 🚀 Future Enhancements

### Phase 2 Features
- [ ] Real-time streaming with Apache Kafka
- [ ] Machine learning (customer churn prediction)
- [ ] Tableau/Power BI dashboards
- [ ] dbt for transformation layer
- [ ] Great Expectations for data testing
- [ ] CI/CD pipeline with GitHub Actions
- [ ] Unit tests with pytest
- [ ] API layer (FastAPI)

### Scalability Improvements
- [ ] Partitioning large tables by date
- [ ] Implement data lake (Parquet files)
- [ ] Add caching layer (Redis)
- [ ] Multi-region deployment
- [ ] Kubernetes for Airflow

---

## 📞 Contact

**Abhiiram**  
📧 Email: abhiramashwika@gmail.com  
💼 LinkedIn: [linkedin.com/in/abhiiram](https://www.linkedin.com/in/abhiiram)  
🐙 GitHub: [github.com/abhiiram16](https://github.com/abhiiram16)

---

## 📄 License

This project is licensed under the MIT License - see the LICENSE file for details.

---

## 🙏 Acknowledgments

- Data generated using [Faker](https://faker.readthedocs.io/)
- Orchestration powered by [Apache Airflow](https://airflow.apache.org/)
- Database: [PostgreSQL](https://www.postgresql.org/)

---

## ⭐ Star This Project

If you found this project helpful for learning data engineering, please give it a star! ⭐

---

**Built with ❤️ for learning data engineering and showcasing production-ready skills.**
