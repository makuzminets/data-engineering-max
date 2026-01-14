# Managed Backfill Pipeline

Controlled backfill operations with parameterization, validation, and progress tracking.

## 🎯 Key Features

- **Parameterized Dates**: Configure date range via Airflow UI
- **Dry-Run Mode**: Test backfill logic without writing data
- **Chunked Processing**: Split large ranges into manageable pieces
- **Validation**: Prevent invalid or dangerous backfill requests
- **Audit Trail**: Track all backfill operations

## 🏗️ Architecture

```
┌─────────────────┐     ┌──────────────────┐     ┌─────────────────┐
│   Airflow UI    │────▶│  Validate Params │────▶│  Generate Chunks│
│   (Parameters)  │     │                  │     │                 │
│                 │     │  - Date format   │     │  - 7 days each  │
│  start: 01-01   │     │  - Range check   │     │  - Sequential   │
│  end: 01-31     │     │  - Table exists  │     │                 │
│  dry_run: true  │     └──────────────────┘     └────────┬────────┘
└─────────────────┘                                       │
                                                          ▼
                        ┌──────────────────┐     ┌─────────────────┐
                        │    Summary       │◀────│ Process Chunks  │
                        │                  │     │                 │
                        │  - Total rows    │     │  - Delete old   │
                        │  - Status        │     │  - Reload data  │
                        └──────────────────┘     └─────────────────┘
```

## 📁 Project Structure

```
backfill_manager/
├── dags/
│   └── managed_backfill.py       # Main DAG with params
├── includes/
│   ├── __init__.py
│   ├── backfill_utils.py         # Validation & tracking
│   └── connections.py            # BigQuery connection
└── README.md
```

## 🔧 Usage

### 1. Trigger via Airflow UI

```
DAG: managed_backfill
├── start_date: "2024-01-01"
├── end_date: "2024-01-31"
├── target_table: "transactions"
├── dry_run: true
└── chunk_size_days: 7
```

### 2. Trigger via API

```bash
curl -X POST \
  http://localhost:8080/api/v1/dags/managed_backfill/dagRuns \
  -H 'Content-Type: application/json' \
  -d '{
    "conf": {
      "start_date": "2024-01-01",
      "end_date": "2024-01-31",
      "target_table": "transactions",
      "dry_run": false
    }
  }'
```

## 📊 Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `start_date` | string | Required | Start of backfill (YYYY-MM-DD) |
| `end_date` | string | Required | End of backfill (YYYY-MM-DD) |
| `target_table` | enum | Required | Table to backfill |
| `dry_run` | boolean | `true` | Simulate without writing |
| `chunk_size_days` | integer | `7` | Days per processing chunk |

## ✅ Validation Rules

1. **Date Format**: Must be YYYY-MM-DD
2. **Date Order**: start_date ≤ end_date
3. **Not Future**: end_date ≤ today
4. **Range Limit**: Max 30 days (configurable)
5. **Table Exists**: Must be in allowed list

## 📝 Backfill Log

Track all operations in `backfill_log` table:

```sql
SELECT 
    backfill_id,
    table_name,
    start_date,
    end_date,
    status,
    rows_affected,
    started_at,
    completed_at
FROM backfill_log
WHERE table_name = 'transactions'
ORDER BY started_at DESC;
```

## 🔒 Safety Features

| Feature | Description |
|---------|-------------|
| **Dry Run** | Default true - must explicitly enable writes |
| **Max Days** | Prevents accidentally backfilling years of data |
| **Chunking** | Processes in small batches for recoverability |
| **Audit Log** | Records who/what/when for all backfills |

## 🧪 Testing

1. **Dry Run First**: Always test with `dry_run=true`
2. **Small Range**: Start with 1-2 days
3. **Check Logs**: Review what would be processed
4. **Then Execute**: Set `dry_run=false`

## 🔗 Related Patterns

- [Postgres to BigQuery](../postgres_bigquery_sync/) - Source sync logic
- [Data Quality](../data_quality/) - Validate after backfill
