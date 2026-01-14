# Data Quality Checks Pipeline

Automated data quality validation inspired by Great Expectations, integrated with Airflow for continuous monitoring.

## 🎯 Key Features

- **Schema Validation**: Column types, nullability, required fields
- **Freshness Checks**: Detect stale data automatically
- **Value Validation**: Ranges, enums, null percentages
- **Duplicate Detection**: Find duplicate records by key
- **Referential Integrity**: Validate foreign key relationships
- **Alerting**: Slack notifications for failures

## 🏗️ Architecture

```
┌─────────────────┐     ┌──────────────────┐     ┌─────────────────┐
│   BigQuery      │────▶│  Data Quality    │────▶│     Slack       │
│   Tables        │     │  Validator       │     │   Alerts        │
└─────────────────┘     └──────────────────┘     └─────────────────┘
                               │
                               ▼
                        ┌──────────────────┐
                        │  Check Types:    │
                        │  - Schema        │
                        │  - Freshness     │
                        │  - Values        │
                        │  - Duplicates    │
                        │  - Referential   │
                        └──────────────────┘
```

## 📁 Project Structure

```
data_quality/
├── dags/
│   └── data_quality_checks.py    # Main DAG
├── includes/
│   ├── __init__.py
│   ├── connections.py            # BigQuery connection
│   ├── expectations.py           # Validation logic
│   └── alerting.py               # Slack notifications
└── README.md
```

## 🔧 Configuration

### Define Table Expectations

```python
# In expectations.py
SCHEMAS = {
    "transactions": {
        "id": "INT64",
        "amount": "FLOAT64",
        "status": "STRING",
    }
}

VALUE_CHECKS = {
    "transactions": [
        {"type": "null_percentage", "column": "user_id", "max_percentage": 1},
        {"type": "range", "column": "amount", "min": 0, "max": 1000000},
        {"type": "enum", "column": "status", "values": ["pending", "completed"]},
    ]
}
```

### Configure Slack Alerts

```bash
airflow connections add 'slack_data_quality' \
    --conn-type 'slack' \
    --conn-host 'https://hooks.slack.com/services/YOUR/WEBHOOK/URL'
```

## 📊 Check Types

| Check | Description | Example |
|-------|-------------|---------|
| **Schema** | Column existence and types | `id` must be `INT64` |
| **Freshness** | Data recency | Updated within 24h |
| **Null %** | Maximum null percentage | `user_id` < 1% null |
| **Range** | Numeric boundaries | `amount` between 0-1M |
| **Enum** | Valid categorical values | `status` in ['active', 'inactive'] |
| **Duplicates** | Unique key violations | No duplicate `id` |
| **Referential** | FK relationships | All `user_id` exist in `users` |

## 🔔 Alert Levels

| Level | Trigger | Action |
|-------|---------|--------|
| 🔴 Critical | Failed checks | Immediate Slack alert |
| 🟡 Warning | Value issues | Daily summary |
| 🟢 Info | All passed | Log only |

## 🧪 Example Output

```
📊 Validating: transactions
   ✅ Schema valid
   ✅ Data fresh (last update: 2024-01-15 10:30:00)
   ⚠️ Value issues: ['amount: 0.5% null (max: 0%)']
   ✅ No duplicates

📊 Validating: users
   ✅ Schema valid
   ❌ Stale data: 36h old
   ✅ Values valid
   ✅ Referential integrity OK

📈 Summary: {'total_checks': 8, 'passed': 6, 'failed': 1, 'warnings': 1}
```

## 🔗 Related Patterns

- [Alerting System](../alerting/) - Custom notification framework
- [Postgres to BigQuery](../postgres_bigquery_sync/) - Source data pipelines
