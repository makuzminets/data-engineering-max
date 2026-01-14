# Terraform Configuration for Airflow Infrastructure on GCP
# ==========================================================
# Provisions all GCP resources needed for the Airflow pipelines:
# - BigQuery datasets and tables
# - Cloud Composer (managed Airflow)
# - Service accounts and IAM
# - Cloud Storage for DAGs

terraform {
  required_version = ">= 1.0"
  
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
  }
  
  # Remote state (uncomment for production)
  # backend "gcs" {
  #   bucket = "your-terraform-state-bucket"
  #   prefix = "airflow-pipelines"
  # }
}

provider "google" {
  project = var.project_id
  region  = var.region
}

# Enable required APIs
resource "google_project_service" "required_apis" {
  for_each = toset([
    "bigquery.googleapis.com",
    "composer.googleapis.com",
    "storage.googleapis.com",
    "secretmanager.googleapis.com",
    "cloudresourcemanager.googleapis.com",
  ])
  
  service            = each.value
  disable_on_destroy = false
}

# =============================================================================
# BigQuery Datasets
# =============================================================================

resource "google_bigquery_dataset" "analytics" {
  dataset_id    = "analytics"
  friendly_name = "Analytics Data"
  description   = "Main analytics dataset for processed data"
  location      = var.bigquery_location
  
  labels = {
    environment = var.environment
    managed_by  = "terraform"
  }
  
  # 90 day default expiration for tables (optional)
  # default_table_expiration_ms = 7776000000
}

resource "google_bigquery_dataset" "staging" {
  dataset_id    = "staging"
  friendly_name = "Staging Data"
  description   = "Temporary staging tables"
  location      = var.bigquery_location
  
  # Short expiration for staging tables
  default_table_expiration_ms = 604800000  # 7 days
  
  labels = {
    environment = var.environment
    managed_by  = "terraform"
  }
}

resource "google_bigquery_dataset" "affiliate" {
  dataset_id    = "affiliate"
  friendly_name = "Affiliate Data"
  description   = "Affiliate partner analytics"
  location      = var.bigquery_location
  
  labels = {
    environment = var.environment
    managed_by  = "terraform"
  }
}

# =============================================================================
# BigQuery Tables (Sync Control)
# =============================================================================

resource "google_bigquery_table" "sync_control" {
  dataset_id = google_bigquery_dataset.analytics.dataset_id
  table_id   = "sync_control"
  
  description = "Tracks sync progress for resumable ETL pipelines"
  
  schema = jsonencode([
    {
      name = "table_name"
      type = "STRING"
      mode = "REQUIRED"
      description = "Name of table being synced"
    },
    {
      name = "sync_date"
      type = "DATE"
      mode = "REQUIRED"
      description = "Date being processed"
    },
    {
      name = "last_max_id"
      type = "INT64"
      mode = "REQUIRED"
      description = "Last processed record ID"
    },
    {
      name = "is_finished"
      type = "BOOL"
      mode = "REQUIRED"
      description = "Whether sync is complete for this date"
    },
    {
      name = "synced_at"
      type = "TIMESTAMP"
      mode = "REQUIRED"
      description = "When this record was created"
    }
  ])
  
  labels = {
    environment = var.environment
    managed_by  = "terraform"
  }
}

resource "google_bigquery_table" "backfill_log" {
  dataset_id = google_bigquery_dataset.analytics.dataset_id
  table_id   = "backfill_log"
  
  description = "Audit log of backfill operations"
  
  schema = jsonencode([
    { name = "backfill_id", type = "STRING", mode = "REQUIRED" },
    { name = "table_name", type = "STRING", mode = "REQUIRED" },
    { name = "start_date", type = "DATE", mode = "REQUIRED" },
    { name = "end_date", type = "DATE", mode = "REQUIRED" },
    { name = "status", type = "STRING", mode = "REQUIRED" },
    { name = "rows_affected", type = "INT64", mode = "NULLABLE" },
    { name = "error_message", type = "STRING", mode = "NULLABLE" },
    { name = "started_at", type = "TIMESTAMP", mode = "REQUIRED" },
    { name = "completed_at", type = "TIMESTAMP", mode = "NULLABLE" },
    { name = "triggered_by", type = "STRING", mode = "NULLABLE" }
  ])
  
  labels = {
    environment = var.environment
    managed_by  = "terraform"
  }
}

# =============================================================================
# Service Account for Airflow
# =============================================================================

resource "google_service_account" "airflow" {
  account_id   = "airflow-pipelines"
  display_name = "Airflow Pipelines Service Account"
  description  = "Service account for Airflow DAGs to access GCP resources"
}

# BigQuery permissions
resource "google_project_iam_member" "airflow_bigquery_admin" {
  project = var.project_id
  role    = "roles/bigquery.admin"
  member  = "serviceAccount:${google_service_account.airflow.email}"
}

# Storage permissions
resource "google_project_iam_member" "airflow_storage_admin" {
  project = var.project_id
  role    = "roles/storage.admin"
  member  = "serviceAccount:${google_service_account.airflow.email}"
}

# Secret Manager access
resource "google_project_iam_member" "airflow_secret_accessor" {
  project = var.project_id
  role    = "roles/secretmanager.secretAccessor"
  member  = "serviceAccount:${google_service_account.airflow.email}"
}

# =============================================================================
# Cloud Storage for DAGs
# =============================================================================

resource "google_storage_bucket" "airflow_dags" {
  name     = "${var.project_id}-airflow-dags"
  location = var.region
  
  uniform_bucket_level_access = true
  
  versioning {
    enabled = true
  }
  
  lifecycle_rule {
    condition {
      num_newer_versions = 5
    }
    action {
      type = "Delete"
    }
  }
  
  labels = {
    environment = var.environment
    managed_by  = "terraform"
  }
}

# =============================================================================
# Cloud Composer (Managed Airflow) - Optional
# =============================================================================

# Uncomment to create Cloud Composer environment
# Note: Cloud Composer can take 25-45 minutes to create

# resource "google_composer_environment" "airflow" {
#   name   = "airflow-pipelines"
#   region = var.region
#   
#   config {
#     software_config {
#       image_version = "composer-2.5.0-airflow-2.7.0"
#       
#       pypi_packages = {
#         "faker"             = ">=18.0.0"
#         "clickhouse-driver" = ">=0.2.0"
#       }
#       
#       env_variables = {
#         OPENLINEAGE_URL = "https://your-marquez-url"
#       }
#     }
#     
#     workloads_config {
#       scheduler {
#         cpu        = 2
#         memory_gb  = 4
#         storage_gb = 10
#         count      = 1
#       }
#       
#       web_server {
#         cpu        = 2
#         memory_gb  = 4
#         storage_gb = 10
#       }
#       
#       worker {
#         cpu        = 2
#         memory_gb  = 4
#         storage_gb = 10
#         min_count  = 1
#         max_count  = 3
#       }
#     }
#     
#     node_config {
#       service_account = google_service_account.airflow.email
#     }
#   }
#   
#   labels = {
#     environment = var.environment
#     managed_by  = "terraform"
#   }
# }

# =============================================================================
# Secrets for Airflow Connections
# =============================================================================

resource "google_secret_manager_secret" "postgres_connection" {
  secret_id = "airflow-postgres-connection"
  
  replication {
    auto {}
  }
  
  labels = {
    environment = var.environment
    managed_by  = "terraform"
  }
}

resource "google_secret_manager_secret" "slack_webhook" {
  secret_id = "airflow-slack-webhook"
  
  replication {
    auto {}
  }
  
  labels = {
    environment = var.environment
    managed_by  = "terraform"
  }
}

# =============================================================================
# Outputs
# =============================================================================

output "bigquery_datasets" {
  description = "Created BigQuery datasets"
  value = {
    analytics = google_bigquery_dataset.analytics.dataset_id
    staging   = google_bigquery_dataset.staging.dataset_id
    affiliate = google_bigquery_dataset.affiliate.dataset_id
  }
}

output "service_account_email" {
  description = "Airflow service account email"
  value       = google_service_account.airflow.email
}

output "dags_bucket" {
  description = "Cloud Storage bucket for DAGs"
  value       = google_storage_bucket.airflow_dags.name
}

# output "composer_airflow_uri" {
#   description = "Cloud Composer Airflow Web UI URL"
#   value       = google_composer_environment.airflow.config[0].airflow_uri
# }
