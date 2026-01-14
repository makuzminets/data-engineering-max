# Terraform Infrastructure for Airflow Pipelines

Infrastructure as Code for provisioning GCP resources needed by the Airflow pipelines.

## 🎯 What's Provisioned

| Resource | Description |
|----------|-------------|
| **BigQuery Datasets** | analytics, staging, affiliate |
| **BigQuery Tables** | sync_control, backfill_log |
| **Service Account** | airflow-pipelines with required IAM roles |
| **Cloud Storage** | Bucket for DAG files |
| **Secret Manager** | Secrets for connections |
| **Cloud Composer** | Managed Airflow (optional) |

## 🚀 Quick Start

### 1. Prerequisites

```bash
# Install Terraform
brew install terraform  # macOS
# or download from https://terraform.io

# Authenticate with GCP
gcloud auth application-default login
```

### 2. Configure Variables

```bash
cd terraform
cp terraform.tfvars.example terraform.tfvars

# Edit with your values
vim terraform.tfvars
```

### 3. Deploy

```bash
# Initialize Terraform
terraform init

# Preview changes
terraform plan

# Apply changes
terraform apply
```

## 📁 Files

```
terraform/
├── main.tf                    # Main configuration
├── variables.tf               # Variable definitions
├── terraform.tfvars.example   # Example variable values
└── README.md
```

## 🔧 Configuration

### Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `project_id` | GCP Project ID | (required) |
| `region` | GCP Region | us-central1 |
| `bigquery_location` | BigQuery location | US |
| `environment` | Environment name | dev |

### Remote State (Production)

Uncomment in `main.tf`:

```hcl
backend "gcs" {
  bucket = "your-terraform-state-bucket"
  prefix = "airflow-pipelines"
}
```

## 📊 Created Resources

### BigQuery Datasets

```sql
-- Created datasets
analytics    -- Main processed data
staging      -- Temporary tables (7 day expiration)
affiliate    -- Partner analytics
```

### Service Account Permissions

| Role | Purpose |
|------|---------|
| `bigquery.admin` | Full BigQuery access |
| `storage.admin` | GCS bucket access |
| `secretmanager.secretAccessor` | Read secrets |

### Sync Control Table

```sql
CREATE TABLE analytics.sync_control (
    table_name STRING,
    sync_date DATE,
    last_max_id INT64,
    is_finished BOOL,
    synced_at TIMESTAMP
);
```

## ☁️ Cloud Composer (Optional)

To create managed Airflow, uncomment the `google_composer_environment` resource in `main.tf`.

**Note**: Cloud Composer creation takes 25-45 minutes.

```bash
# After uncommenting
terraform plan
terraform apply
```

## 🔒 Secrets

Add secret values via Console or CLI:

```bash
# Postgres connection
echo -n '{"host":"your-host","password":"secret"}' | \
  gcloud secrets versions add airflow-postgres-connection --data-file=-

# Slack webhook
echo -n 'https://hooks.slack.com/services/XXX' | \
  gcloud secrets versions add airflow-slack-webhook --data-file=-
```

## 🗑️ Cleanup

```bash
# Destroy all resources
terraform destroy

# Or destroy specific resources
terraform destroy -target=google_bigquery_dataset.staging
```

## 🔗 Related

- [Docker Setup](../docker/) - Local development
- [Main README](../README.md) - Project overview
