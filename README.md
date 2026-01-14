# 👋 Hi, I'm Max Kuzminets

**Data & Analytics Engineer | Fraud Detection Specialist**

10+ years building data platforms, fraud detection systems, and analytics infrastructure at companies like **Upwork**, **Semrush**, and **MailerLite**.

---

## 🛠️ Skills

| Category | Technologies |
|----------|--------------|
| **SQL** | BigQuery, Snowflake, PostgreSQL, ClickHouse |
| **Transformations** | dbt, Airflow, Airbyte |
| **Python** | Pandas, NumPy, Scikit-learn |
| **BI** | Looker, Metabase, Looker Studio |
| **Cloud** | GCP (Vertex AI, BigQuery), AWS (SageMaker, S3) |
| **Fraud/ML** | XGBoost, Risk Scoring, Anomaly Detection |

---

## 📂 Projects

### 🛡️ [Fraud Detection ML Pipeline](./fraud_ml_pipeline)

Production-grade signup fraud detection with ML — based on experience reducing scam incidents by **70%** at Upwork.

| Component | Description |
|-----------|-------------|
| **40+ Features** | Email entropy, behavioral signals, device fingerprints, velocity |
| **ML Models** | XGBoost & LightGBM with imbalanced data handling |
| **FastAPI** | Real-time scoring API with risk levels |
| **Docker** | Ready for production deployment |

[View Project →](./fraud_ml_pipeline)

---

### 💳 [Chargeback Analytics Dashboard](./chargeback_analytics)

Payment fraud monitoring and chargeback analytics — based on experience reducing chargebacks from **7% to 1%** at Semrush.

| Component | Description |
|-----------|-------------|
| **dbt Models** | Staging → Marts with daily summaries |
| **Streamlit** | Interactive dashboard with KPIs |
| **Reason Codes** | Visa/Mastercard classification |
| **Metrics** | Rate, win rate, fraud %, by country/plan |

```bash
streamlit run dashboard/app.py
# Dashboard at http://localhost:8501
```

[View Project →](./chargeback_analytics)

---

### 🔄 [Airflow Data Pipelines](./airflow_pipelines)

Collection of **10 production-grade** Apache Airflow DAGs demonstrating real-world data engineering patterns.

| Category | Projects |
|----------|----------|
| **ETL Pipelines** | [postgres_bigquery_sync](./airflow_pipelines/postgres_bigquery_sync) • [api_warehouse_sync](./airflow_pipelines/api_warehouse_sync) • [gdpr_anonymization](./airflow_pipelines/gdpr_anonymization) • [ecommerce_analytics](./airflow_pipelines/ecommerce_analytics) • [monthly_snapshots](./airflow_pipelines/monthly_snapshots) • [clickhouse_sync](./airflow_pipelines/clickhouse_sync) |
| **Operations** | [data_quality](./airflow_pipelines/data_quality) • [alerting](./airflow_pipelines/alerting) • [backfill_manager](./airflow_pipelines/backfill_manager) • [data_lineage](./airflow_pipelines/data_lineage) |
| **Infrastructure** | [docker/](./airflow_pipelines/docker) • [terraform/](./airflow_pipelines/terraform) • [CI/CD](./airflow_pipelines/.github/workflows) |

**Key Patterns:** Chunked processing, resumable state, atomic table swap, MERGE/UPSERT, gap filling, Great Expectations validation, OpenLineage integration.

[View All Pipelines →](./airflow_pipelines)

---

### 🛡️ [dbt Fraud Analytics](./dbt_fraud_analytics)

Production-ready dbt project for fraud detection and risk analytics.

- **10 models**: staging → intermediate → marts
- **Star schema**: fact tables + dimensions
- **Risk scoring**: transaction & user risk signals
- **Testing**: unique, not_null, relationships, accepted_values

```
models/
├── staging/      → stg_transactions, stg_users, stg_devices
├── intermediate/ → int_transaction_features, int_user_risk_signals  
└── marts/        → fct_transactions, fct_fraud_events, dim_users
```

[View Project →](./dbt_fraud_analytics)

---

### 🐳 [Multi-Database Dev Environment](./docker_postgres_ch_mongodb)

Docker Compose setup for local development with multiple databases.

| Database | Purpose | Port |
|----------|---------|------|
| **PostgreSQL** | OLTP, pg_cron scheduler | 5432 |
| **ClickHouse** | OLAP analytics | 8123, 9000 |
| **MongoDB** | Document storage | 27017 |

```bash
cd docker_postgres_ch_mongodb
docker-compose up -d
```

---

## 🎤 Speaking

- **MRC Conference** (Amsterdam) — Fraud Prevention Strategies
- **MRC Conference** (Dublin) — Payment Fraud Analytics

---

## 📫 Contact

- 💼 [LinkedIn](https://www.linkedin.com/in/maxkuzminets)
- ✈️ [Telegram](https://t.me/maximystic)
- 📧 maxkuzminets@yahoo.com

---

*Open to remote opportunities in Data Engineering, Analytics Engineering, and Fraud/Trust & Safety roles.*
