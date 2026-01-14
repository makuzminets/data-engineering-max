# E-commerce Analytics Pipeline

Production pipelines for e-commerce analytics aggregation, featuring daily integration metrics and rolling window statistics.

## 🎯 Key Features

- **Daily Integration Usage**: Track orders per shop/integration with gap-filling
- **30-Day Rolling Stats**: Weekly rebuild of sending statistics
- **TaskFlow API**: Modern Airflow pattern with typed task parameters
- **Incremental Processing**: Only process new data from last checkpoint

## 🏗️ Architecture

### Integration Usage Daily

```
┌─────────────────┐     ┌──────────────────┐     ┌─────────────────┐
│   PostgreSQL    │────▶│  Aggregate per   │────▶│    BigQuery     │
│                 │     │   shop + day     │     │                 │
│  ecommerce_     │     │                  │     │ integration_    │
│  shops + orders │     │  - Gap filling   │     │ usage_daily     │
└─────────────────┘     │  - Batch insert  │     └─────────────────┘
                        └──────────────────┘
```

### 30-Day Rolling Stats

```
┌─────────────────┐     ┌──────────────────┐     ┌─────────────────┐
│   PostgreSQL    │────▶│  Aggregate last  │────▶│    BigQuery     │
│                 │     │   30 days        │     │                 │
│     emails      │     │                  │     │ sending_stats_  │
│                 │     │  WRITE_TRUNCATE  │     │     30d         │
└─────────────────┘     └──────────────────┘     └─────────────────┘
```

## 📁 Project Structure

```
ecommerce_analytics/
├── dags/
│   ├── integration_usage_daily.py   # Daily shop metrics
│   └── sending_stats_30d.py         # Weekly rolling stats
├── includes/
│   ├── __init__.py
│   └── connections.py               # Database connection factories
└── README.md
```

## 🔧 DAGs Overview

### 1. Integration Usage Daily

**Schedule**: Daily at 4 AM UTC  
**Purpose**: Track daily order counts per e-commerce integration

| Feature | Description |
|---------|-------------|
| Incremental | Continues from last processed date |
| Gap Filling | Creates zero-count rows for inactive days |
| Batch Processing | Processes 100 shops at a time |
| Memory Efficient | Inserts every 10K rows |

**Output Schema**:
```sql
CREATE TABLE integration_usage_daily (
    shop_id INT64,
    account_id INT64,
    integration_created_at TIMESTAMP,
    platform STRING,
    day DATE,
    orders_count INT64
)
```

### 2. Sending Stats 30-Day

**Schedule**: Weekly  
**Purpose**: Rolling 30-day email sending statistics

| Feature | Description |
|---------|-------------|
| Full Rebuild | Drops and recreates table |
| Category Breakdown | automation, campaign, transactional |
| Simple & Reliable | No incremental logic to maintain |

**Output Schema**:
```sql
CREATE TABLE sending_stats_30d (
    account_id INT64,
    emails_30d INT64,
    automation_emails_30d INT64,
    campaign_emails_30d INT64,
    transactional_emails_30d INT64,
    updated_at TIMESTAMP
)
```

## 🔄 Gap Filling Algorithm

Ensures continuous time series for analytics:

```python
# Input: [('2024-01-01', 5), ('2024-01-03', 3)]
# Output: [
#   ('2024-01-01', 5),
#   ('2024-01-02', 0),  # Gap filled
#   ('2024-01-03', 3)
# ]

def fill_date_gaps(collection, start_day, end_day):
    existing_days = {row.day for row in collection}
    current = start_day
    
    while current <= end_day:
        if current not in existing_days:
            collection.append(Row(day=current, orders=0))
        current += timedelta(days=1)
    
    return sorted(collection, key=lambda r: r.day)
```

## 📊 Example Queries

### Top Shops by Order Volume

```sql
SELECT 
    shop_id,
    platform,
    SUM(orders_count) AS total_orders,
    COUNT(DISTINCT day) AS active_days
FROM integration_usage_daily
WHERE day >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
GROUP BY 1, 2
ORDER BY total_orders DESC
LIMIT 10;
```

### Sending Trends by Type

```sql
SELECT 
    SUM(automation_emails_30d) AS automation,
    SUM(campaign_emails_30d) AS campaigns,
    SUM(transactional_emails_30d) AS transactional
FROM sending_stats_30d;
```

## 🔗 Related Patterns

- [Postgres to BigQuery](../postgres_bigquery_sync/) - Sync with chunking
- [API to Warehouse](../api_warehouse_sync/) - External data sources
- [GDPR Anonymization](../gdpr_anonymization/) - Data privacy
