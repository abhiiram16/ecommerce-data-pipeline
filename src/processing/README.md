# Data Processing Module

This module contains scripts for transforming raw data into analytics-ready aggregates.

## Scripts

### `refresh_aggregations.py`

Automated refresh of all aggregate tables with latest transactional data.

**What it does:**
- Drops and recreates 4 materialized aggregate tables
- customer_summary (8,893 rows)
- product_summary (495 rows)
- daily_sales_summary (181 rows)
- monthly_sales_summary (7 rows)

**Usage:**
python src/processing/refresh_aggregations.py


**Performance:**
- Typical execution: ~0.8 seconds
- All tables + indexes rebuilt from scratch
- Suitable for scheduled execution (daily/hourly)

**Scheduling:**
Linux cron (daily at 1 AM)
0 1 * * * /path/to/venv/bin/python /path/to/refresh_aggregations.py

Windows Task Scheduler
Action: python C:\projects\ecommerce-data-pipeline\src\processing\refresh_aggregations.py



**Future Enhancements:**
- Add email notifications on failure
- Integrate with Apache Airflow DAG
- Add incremental refresh logic
- Create config file for DB credentials
