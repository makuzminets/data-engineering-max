# Docker Compose for Local Airflow

Complete local development environment for Airflow DAGs with sample databases.

## 🎯 What's Included

| Service | Port | Description |
|---------|------|-------------|
| **airflow-webserver** | 8080 | Airflow UI |
| **airflow-scheduler** | - | DAG scheduler |
| **postgres** | 5432 | Airflow metadata DB |
| **source-postgres** | 5433 | Sample source database |
| **clickhouse** | 8123, 9000 | OLAP database |
| **marquez** | 5000, 5001 | Data lineage UI |

## 🚀 Quick Start

### 1. Start Services

```bash
cd docker

# Create secrets directory (for GCP credentials)
mkdir -p secrets logs

# Start all services
docker-compose up -d

# Wait for initialization
docker-compose logs -f airflow-init
```

### 2. Access Airflow UI

Open http://localhost:8080

- **Username**: admin
- **Password**: admin

### 3. Explore DAGs

All DAGs from the projects will be available in the UI:
- `postgres_to_bigquery_activity_logs`
- `api_affiliate_partners_sync`
- `gdpr_anonymize_user_data`
- `ecommerce_integration_usage_daily`
- `data_quality_checks`
- And more...

## 📁 Directory Structure

```
docker/
├── docker-compose.yml        # Main compose file
├── init-scripts/
│   └── 01-create-tables.sql  # Sample DB schema
├── secrets/                  # GCP credentials (gitignored)
│   └── gcp-credentials.json
├── logs/                     # Airflow logs (gitignored)
└── README.md
```

## 🔧 Configuration

### Add GCP Credentials

For BigQuery connectivity:

```bash
# Copy your service account key
cp ~/path/to/service-account.json docker/secrets/gcp-credentials.json
```

### Add Airflow Connections

Via UI (Admin → Connections) or CLI:

```bash
# Source PostgreSQL
docker-compose exec airflow-webserver airflow connections add 'postgres_default' \
    --conn-type 'postgres' \
    --conn-host 'source-postgres' \
    --conn-login 'source' \
    --conn-password 'source' \
    --conn-schema 'source_db' \
    --conn-port 5432

# ClickHouse
docker-compose exec airflow-webserver airflow connections add 'clickhouse_default' \
    --conn-type 'generic' \
    --conn-host 'clickhouse' \
    --conn-port 9000 \
    --conn-login 'default'

# Slack (for alerts)
docker-compose exec airflow-webserver airflow connections add 'slack_webhook' \
    --conn-type 'http' \
    --conn-host 'https://hooks.slack.com/services' \
    --conn-password '/YOUR/WEBHOOK/URL'
```

## 🧪 Testing DAGs

### Trigger a DAG

```bash
docker-compose exec airflow-webserver \
    airflow dags trigger data_quality_checks
```

### Check DAG Status

```bash
docker-compose exec airflow-webserver \
    airflow dags list-runs -d data_quality_checks
```

### View Logs

```bash
docker-compose logs -f airflow-scheduler
```

## 📊 Data Lineage

Access Marquez UI at http://localhost:5001 to visualize data lineage.

## 🛑 Cleanup

```bash
# Stop services
docker-compose down

# Stop and remove volumes (reset everything)
docker-compose down -v

# Remove all data
rm -rf logs/*
```

## 🔗 Related

- [Main README](../README.md) - All projects overview
- [Terraform](../terraform/) - GCP infrastructure
