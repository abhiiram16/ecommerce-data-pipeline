# Cloud Deployment Strategy

**Project:** E-Commerce Data Pipeline  
**Date:** November 5, 2025  
**Status:** Architecture Design (Not Yet Deployed)  
**Author:** Abhiiram

---

## 🎯 Objective

Design a scalable, cost-effective cloud architecture for deploying the e-commerce data pipeline from local development to production cloud environment.

---

## 📊 Current Architecture (Local Development)

┌─────────────────────────────────────────────────────┐
│ DEVELOPMENT SETUP │
├─────────────────────────────────────────────────────┤
│ │
│ [CSV Files] → [Python Scripts] → [PostgreSQL] │
│ (local FS) (manual run) (Docker) │
│ │
│ Limitations: │
│ - Single machine (no HA) │
│ - Manual execution │
│ - No monitoring │
│ - Not scalable beyond 100K records │
│ │


**Current Metrics:**
- Data Volume: 60,495 records
- Database: PostgreSQL 15 (1 container)
- Storage: ~10 MB CSV files
- Compute: Single Python process
- Execution: Manual trigger

---

## ☁️ Proposed Cloud Architecture (AWS)

### High-Level Design

┌────────────────────────────────────────────────────────────────┐
│ AWS CLOUD ARCHITECTURE │

                   [Data Sources]
                        │
                        ↓
                ┌──────────────┐
                │   AWS S3     │  ← CSV Files / Data Lake
                │  (Storage)   │     Versioning Enabled
                └──────┬───────┘
                       │
                       ↓
                ┌──────────────┐
                │ AWS Lambda   │  ← ETL Processing
                │  (Compute)   │     Triggered by S3 events
                └──────┬───────┘
                       │
                       ↓
                ┌──────────────┐
                │   AWS RDS    │  ← PostgreSQL Database
                │ (PostgreSQL) │     Multi-AZ for HA
                └──────┬───────┘
                       │
                ┌──────┴───────┐
                │              │
         ┌──────▼──────┐  ┌───▼──────┐
         │ CloudWatch  │  │   SNS    │
         │ (Monitoring)│  │ (Alerts) │
         └─────────────┘  └──────────┘

---

## 🛠️ Component Breakdown

### 1. Data Storage: AWS S3

**Purpose:** Data lake for raw and processed data

**Structure:**
s3://ecommerce-data-lake/
├── raw/
│ ├── customers/
│ │ └── 2025-11-05-customers.csv
│ ├── products/
│ │ └── 2025-11-05-products.csv
│ └── orders/
│ └── 2025-11-05-orders.csv
├── processed/
│ ├── aggregates/
│ └── quality-reports/
└── archive/



**Features:**
- Versioning enabled (data lineage)
- Lifecycle policies (move to Glacier after 90 days)
- Server-side encryption (AES-256)
- Access logs enabled

**Cost Estimate:**
- First 5 GB: Free (Free Tier)
- Additional storage: $0.023/GB/month
- **Total (10GB): ~$0.12/month**

---

### 2. Database: AWS RDS PostgreSQL

**Purpose:** Transactional database + data warehouse

**Configuration:**
- Instance Type: `db.t3.micro` (2 vCPUs, 1 GB RAM)
- Storage: 20 GB General Purpose SSD (gp3)
- Multi-AZ: Yes (High Availability)
- Automated Backups: 7-day retention
- Maintenance Window: Sunday 2-3 AM IST

**Security:**
- VPC with private subnets
- Security group: Allow port 5432 from Lambda only
- Encryption at rest (AWS KMS)
- SSL/TLS for connections

**Cost Estimate:**
- db.t3.micro: $0.018/hour × 730 hours = ~$13.14/month
- Storage (20GB): $2.30/month
- Backup storage (20GB): Free (within DB size)
- **Total: ~$15.44/month**

---

### 3. Compute: AWS Lambda

**Purpose:** Serverless ETL processing

**Functions:**

**a) Data Ingestion Lambda**

**Features:**
- Versioning enabled (data lineage)
- Lifecycle policies (move to Glacier after 90 days)
- Server-side encryption (AES-256)
- Access logs enabled

**Cost Estimate:**
- First 5 GB: Free (Free Tier)
- Additional storage: $0.023/GB/month
- **Total (10GB): ~$0.12/month**

---

### 2. Database: AWS RDS PostgreSQL

**Purpose:** Transactional database + data warehouse

**Configuration:**
- Instance Type: `db.t3.micro` (2 vCPUs, 1 GB RAM)
- Storage: 20 GB General Purpose SSD (gp3)
- Multi-AZ: Yes (High Availability)
- Automated Backups: 7-day retention
- Maintenance Window: Sunday 2-3 AM IST

**Security:**
- VPC with private subnets
- Security group: Allow port 5432 from Lambda only
- Encryption at rest (AWS KMS)
- SSL/TLS for connections

**Cost Estimate:**
- db.t3.micro: $0.018/hour × 730 hours = ~$13.14/month
- Storage (20GB): $2.30/month
- Backup storage (20GB): Free (within DB size)
- **Total: ~$15.44/month**

---

### 3. Compute: AWS Lambda

**Purpose:** Serverless ETL processing

**Functions:**

**a) Data Ingestion Lambda**

    lambda_ingestion.py
import boto3
import psycopg2

def lambda_handler(event, context):
"""
Triggered when CSV uploaded to S3.
Loads data into RDS PostgreSQL.
"""
s3_bucket = event['Records']['s3']['bucket']['name']
s3_key = event['Records']['s3']['object']['key']

# Download from S3
# Load to RDS
# Send SNS notification



**b) Aggregation Lambda**
    
    lambda_aggregation.py
def lambda_handler(event, context):
"""
Runs daily aggregation queries.
Updates customer_summary, product_summary, etc.
"""
# Execute refresh_aggregations.py logic



**c) Quality Check Lambda**
    
    lambda_quality.py
def lambda_handler(event, context):
"""
Runs data quality checks.
Generates HTML report → upload to S3.
"""
# Execute data_quality_checks.py logic


**Configuration:**
- Runtime: Python 3.14
- Memory: 512 MB
- Timeout: 5 minutes
- Environment Variables: RDS endpoint, credentials
- Trigger: S3 event / CloudWatch Events (schedule)

**Cost Estimate:**
- Free Tier: 1M requests/month, 400K GB-seconds compute
- Expected usage: ~10K requests/month
- **Total: $0 (within free tier)**

---

### 4. Orchestration: AWS Step Functions

**Purpose:** Workflow coordination

**Daily Pipeline Workflow:**

StartExecution
↓
[Upload Check] → Check if new data in S3
↓
[Ingestion] → Load data to RDS
↓
[Quality Check] → Run validation
↓
[Conditional] → Quality > 95%?
├─ Yes → [Aggregation] → [Report]
└─ No → [Send Alert] → [Manual Review]


**Cost Estimate:**
- First 4,000 state transitions/month: Free
- **Total: $0 (within free tier)**

---

### 5. Monitoring: CloudWatch + SNS

**CloudWatch Metrics:**
- Lambda execution duration
- RDS CPU/memory usage
- S3 request metrics
- Custom metrics (data quality score)

**CloudWatch Alarms:**
RDS CPU > 80% for 5 min → SNS alert

Lambda errors > 5 in 10 min → SNS alert

Quality score < 90% → SNS alert

No data ingested for 24 hours → SNS alert


**SNS Topics:**
- `pipeline-critical-alerts` → Email + SMS
- `pipeline-info-logs` → Email only

**Cost Estimate:**
- CloudWatch Logs: First 5GB free
- SNS: First 1,000 emails free
- **Total: $0-2/month**

---

## 💰 Total Cost Analysis

### Monthly Cost Breakdown

| Component | Specification | Monthly Cost |
|-----------|---------------|--------------|
| **S3 Storage** | 10 GB standard | $0.23 |
| **RDS PostgreSQL** | db.t3.micro, 20GB | $15.44 |
| **Lambda** | 10K executions | $0 (free tier) |
| **Step Functions** | 1K transitions | $0 (free tier) |
| **CloudWatch** | Logs + metrics | $2.00 |
| **SNS** | 100 emails/month | $0 (free tier) |
| **Data Transfer** | Minimal | $1.00 |
| | **TOTAL** | **$18.67/month** |

### First Year Cost (with Free Tier):
- Months 1-12: Free Tier eligible
- Estimated: **$10-15/month** (RDS only after free tier expires)

### Annual Cost: **~$120-180/year**

---

## 📈 Scalability Strategy

### Current → 10x Scale (60K → 600K records)

**No Changes Needed:**
- db.t3.micro handles up to 1M rows easily
- Lambda auto-scales

### Current → 100x Scale (60K → 6M records)

**Required Changes:**

**1. Database Scaling:**

db.t3.micro → db.t3.medium (2 vCPUs → 4 vCPUs, 1GB → 4GB)

Add Read Replica for analytics queries
Cost increase: +$30/month


**2. Data Partitioning:**

-- Partition orders table by month
CREATE TABLE orders_2025_11 PARTITION OF orders
FOR VALUES FROM ('2025-11-01') TO ('2025-12-01');


**3. Caching Layer:**

Add Amazon ElastiCache (Redis)
Cache frequently accessed aggregates
Cost: +$15/month (cache.t3.micro)


**4. Compute Optimization:**

Lambda → AWS Fargate (containerized)
Better for long-running ETL jobs
Cost: +$10/month


**Total Cost at 100x Scale: ~$75/month**

---

## 🔒 Security Best Practices

### 1. Network Security
- ✅ VPC with public/private subnets
- ✅ RDS in private subnet (no internet access)
- ✅ Security groups: Least privilege access
- ✅ NAT Gateway for Lambda internet access

### 2. Data Security
- ✅ Encryption at rest (S3, RDS, EBS)
- ✅ Encryption in transit (SSL/TLS)
- ✅ S3 bucket policies: Deny public access
- ✅ RDS: Force SSL connections

### 3. Access Control
- ✅ IAM roles (no hardcoded credentials)
- ✅ Secrets Manager for database passwords
- ✅ MFA for AWS console access
- ✅ CloudTrail for audit logging

### 4. Compliance
- ✅ GDPR: Data encryption, access logs
- ✅ Backup & Recovery: 7-day retention
- ✅ Disaster Recovery: Multi-AZ RDS

---

## 🚀 Migration Steps

### Phase 1: Infrastructure Setup (Day 1)

1. **Create VPC & Subnets**
aws ec2 create-vpc --cidr-block 10.0.0.0/16
aws ec2 create-subnet --vpc-id vpc-xxx --cidr-block 10.0.1.0/24


2. **Launch RDS Instance**

aws rds create-db-instance
--db-instance-identifier ecommerce-db
--db-instance-class db.t3.micro
--engine postgres
--master-username admin
--master-user-password [SecurePassword]
--allocated-storage 20


3. **Create S3 Bucket**

aws s3 mb s3://ecommerce-data-lake
aws s3api put-bucket-versioning
--bucket ecommerce-data-lake
--versioning-configuration Status=Enabled


### Phase 2: Data Migration (Day 2)

1. **Export Local PostgreSQL**

pg_dump -U dataeng ecommerce_db > ecommerce_backup.sql


2. **Upload to S3**

aws s3 cp ecommerce_backup.sql s3://ecommerce-data-lake/migration/

3. **Restore to RDS**

psql -h ecommerce-db.xxx.rds.amazonaws.com -U admin -d ecommerce_db < ecommerce_backup.sql


4. **Upload CSV Files**

aws s3 sync data/raw/ s3://ecommerce-data-lake/raw/


### Phase 3: Deploy Lambda Functions (Day 3)

1. **Package Python Code**

zip -r lambda_ingestion.zip src/ingestion/

2. **Deploy to AWS Lambda**

aws lambda create-function
--function-name ingest-data
--runtime python3.14
--role arn:aws:iam::xxx:role/lambda-execution
--handler lambda_ingestion.lambda_handler
--zip-file fileb://lambda_ingestion.zip

3. **Set up S3 Trigger**

aws s3api put-bucket-notification-configuration
--bucket ecommerce-data-lake
--notification-configuration file://s3-trigger-config.json


### Phase 4: Testing & Validation (Day 4)

1. Upload test CSV → S3
2. Verify Lambda execution logs
3. Check RDS data
4. Run quality checks
5. Validate alerts

---

## 🎯 Success Metrics

### Performance Targets:
- Ingestion latency: < 5 minutes
- Query response time: < 500ms
- Pipeline success rate: > 99%
- Quality score: > 95%

### Monitoring Dashboards:
- CloudWatch Dashboard: Real-time metrics
- Weekly email reports: Quality trends
- Monthly cost analysis: Budget tracking

---

## 📋 Rollback Plan

**If issues arise:**

1. **Database Issues:**
- Restore from automated backup (point-in-time)
- Fallback to local PostgreSQL

2. **Lambda Failures:**
- CloudWatch Logs for debugging
- Manual Python script execution
- DLQ (Dead Letter Queue) for retry

3. **Data Corruption:**
- S3 versioning: Restore previous version
- RDS snapshot: Restore to last known good state

---

## 📚 Future Enhancements

### Phase 2 (After 6 Months):
- ✅ Add real-time streaming (Kinesis Data Streams)
- ✅ Implement Apache Airflow on AWS MWAA
- ✅ Add machine learning (SageMaker for churn prediction)
- ✅ Build Tableau/Superset dashboards

### Phase 3 (After 1 Year):
- ✅ Multi-region deployment (disaster recovery)
- ✅ Data catalog (AWS Glue)
- ✅ Advanced analytics (Redshift Spectrum)
- ✅ API Gateway for external data access

---

## 🎤 Interview Talking Points

**"How would you deploy this to production?"**

*"I designed a cloud-native architecture on AWS with RDS for the database, S3 for the data lake, and Lambda for serverless ETL. I chose these services because they scale automatically and fit within a $20/month budget.*

*For scalability, the architecture supports 100x growth by adding read replicas, partitioning tables by date, and introducing Redis caching. I also documented a complete migration strategy with rollback plans.*

*Security is built-in with VPC isolation, encryption at rest and in transit, IAM roles, and CloudWatch monitoring. The entire deployment can be automated with infrastructure-as-code using Terraform or CloudFormation."*

---

## ✅ Conclusion

This cloud architecture demonstrates:
- ✅ Production-ready thinking
- ✅ Cost-conscious design ($18/month)
- ✅ Scalability planning (100x growth path)
- ✅ Security best practices
- ✅ Migration strategy
- ✅ Disaster recovery planning

**Status:** Architecture documented, ready for implementation when budget permits.

**Next Steps:**
1. Validate locally using LocalStack (AWS emulator)
2. Create Terraform/CloudFormation templates
3. Deploy to AWS (when ready)

