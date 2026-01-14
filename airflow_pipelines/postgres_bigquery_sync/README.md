# Postgres to BigQuery ETL Pipeline

Production-grade ETL pipeline for syncing large tables from PostgreSQL to BigQuery with resumable state management.

## 🎯 Key Features

- **Chunked Processing**: Handles large tables by processing 250K rows per chunk
- **Resumable Sync**: Tracks progress in `sync_control` table - survives failures
- **Gap Detection**: Automatically finds and fills missing dates in historical data
- **Date Partitioning**: Processes data day-by-day for efficient backfills

## 🏗️ Architecture

```
┌─────────────────┐     ┌──────────────────┐     ┌─────────────────┐
│   PostgreSQL    │────▶│   Airflow DAG    │────▶│    BigQuery     │
│  (Source DB)    │     │  (Orchestrator)  │     │  (Data Warehouse)│
└─────────────────┘     └──────────────────┘     └─────────────────┘
                              │
                              ▼
                        ┌──────────────────┐
                        │  sync_control    │
                        │  (State Table)   │
                        └──────────────────┘
```

## 📁 Project Structure

```
postgres_bigquery_sync/
├── dags/
│   └── activity_logs_sync.py    # Main DAG
├── includes/
│   ├── __init__.py
│   ├── connections.py           # Database connection factories
│   └── sync_control.py          # Resumable state management
└── README.md
```

## 🔧 Setup

### 1. Create sync_control table in BigQuery

```sql
CREATE TABLE your_dataset.sync_control (
    table_name STRING,
    sync_date DATE,
    last_max_id INT64,
    is_finished BOOL,
    synced_at TIMESTAMP
);
```

### 2. Configure Airflow Connections

- `postgres_default`: PostgreSQL connection
- `google_cloud_default`: GCP connection with BigQuery access

### 3. Deploy DAG

Copy `dags/` and `includes/` to your Airflow home directory.

## 🔄 How It Works

### Sync Algorithm

1. **Find Start Date**: Query `sync_control` for first unprocessed or incomplete date
2. **Fetch Chunk**: Get next 250K rows from Postgres where `id > last_processed_id`
3. **Insert to BigQuery**: Bulk insert chunk using load jobs
4. **Update Progress**: Mark current position in `sync_control`
5. **Repeat**: Continue until no more rows for current date
6. **Mark Complete**: Set `is_finished = true` for the date
7. **Next Date**: Move to next date and repeat

### Gap Detection

The pipeline automatically detects gaps in your sync history:

```sql
-- Example: If you have data for Jan 1, 2, 4, 5
-- The pipeline will identify Jan 3 as a gap and process it first
```

## 📊 Monitoring

Check sync status:

```sql
SELECT 
    table_name,
    sync_date,
    is_finished,
    last_max_id,
    synced_at
FROM sync_control
WHERE table_name = 'activity_logs'
ORDER BY sync_date DESC
LIMIT 10;
```

## 🚀 Performance Tips

1. **Index source table**: Ensure `(created_at, id)` is indexed
2. **Adjust chunk size**: Increase for stable connections, decrease for unreliable ones
3. **Use date partitioning**: Partition BigQuery table by `created_at` for query performance

## 🔗 Related Patterns

- [Incremental Sync Pattern](../ecommerce_analytics/) - Daily incremental loads
- [API to Warehouse](../api_warehouse_sync/) - Syncing from external APIs
