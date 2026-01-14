# Postgres to ClickHouse Sync Pipeline

Production pipeline for syncing data from PostgreSQL to ClickHouse, a columnar OLAP database optimized for analytical queries.

## 🎯 Key Features

- **DataFrame Batching**: Efficient data transfer using pandas DataFrames
- **ClickHouse Optimizations**: ReplicatedMergeTree, partitioning, native protocol
- **JSON Handling**: Serialize nested objects for ClickHouse storage
- **Resumable Sync**: Track progress in ClickHouse sync_control table

## 🏗️ Architecture

```
┌─────────────────┐     ┌──────────────────┐     ┌─────────────────┐
│   PostgreSQL    │────▶│   Airflow DAG    │────▶│   ClickHouse    │
│   (OLTP)        │     │                  │     │   (OLAP)        │
│                 │     │  - Chunked reads │     │                 │
│  activity_logs  │     │  - JSON serialize│     │  activity_logs  │
│  (row-based)    │     │  - Batch insert  │     │  (columnar)     │
└─────────────────┘     └──────────────────┘     └─────────────────┘
                                                        │
                              10-100x faster queries ◄──┘
```

## 📁 Project Structure

```
clickhouse_sync/
├── dags/
│   └── postgres_to_clickhouse_sync.py   # Main DAG
├── includes/
│   ├── __init__.py
│   ├── connections.py                   # PG + ClickHouse connections
│   └── sync_control.py                  # Sync state management + DDL
└── README.md
```

## 🔧 ClickHouse Setup

### Create Tables

```sql
-- Sync control table (distributed)
CREATE TABLE analytics.sync_control ON CLUSTER default
(
    table_name   String,
    sync_date    Date,
    last_max_id  Int64,
    synced_at    DateTime
)
ENGINE = ReplicatedMergeTree()
ORDER BY (table_name, sync_date, synced_at);

-- Activity logs (partitioned by month)
CREATE TABLE analytics.activity_logs ON CLUSTER default
(
    id           Int64,
    created_at   DateTime,
    event_name   String,
    user_id      Int64,
    properties   String  -- JSON as string
)
ENGINE = ReplicatedMergeTree()
PARTITION BY toYYYYMM(created_at)
ORDER BY (created_at, id);
```

### Configure Airflow Connection

```bash
airflow connections add 'clickhouse_default' \
    --conn-type 'generic' \
    --conn-host 'clickhouse.example.com' \
    --conn-port 9000 \
    --conn-login 'default' \
    --conn-password 'password' \
    --conn-schema 'analytics'
```

## 🚀 Why ClickHouse?

### Performance Comparison

| Query Type | PostgreSQL | ClickHouse | Speedup |
|------------|------------|------------|---------|
| COUNT(*) 1B rows | 45 min | 2 sec | 1350x |
| GROUP BY date (1M rows) | 8 sec | 0.1 sec | 80x |
| Full scan filter | 120 sec | 3 sec | 40x |

### Best Use Cases

- ✅ Time-series analytics
- ✅ Log aggregation
- ✅ Real-time dashboards
- ✅ Ad-hoc analytical queries
- ❌ OLTP (use PostgreSQL)
- ❌ Frequent updates (use PostgreSQL)

## 📊 Query Examples

### Event Counts by Day

```sql
SELECT 
    toDate(created_at) AS day,
    event_name,
    count() AS events
FROM analytics.activity_logs
WHERE created_at >= today() - 30
GROUP BY day, event_name
ORDER BY day, events DESC;
```

### User Activity Funnel

```sql
SELECT 
    user_id,
    countIf(event_name = 'page_view') AS views,
    countIf(event_name = 'add_to_cart') AS carts,
    countIf(event_name = 'purchase') AS purchases
FROM analytics.activity_logs
WHERE created_at >= today() - 7
GROUP BY user_id
HAVING views > 0
ORDER BY purchases DESC
LIMIT 100;
```

### JSON Property Extraction

```sql
SELECT 
    JSONExtractString(properties, 'source') AS source,
    count() AS events
FROM analytics.activity_logs
WHERE created_at >= today() - 1
GROUP BY source
ORDER BY events DESC;
```

## ⚡ Performance Tips

1. **Partition by time**: `PARTITION BY toYYYYMM(created_at)`
2. **Order by query patterns**: Match ORDER BY to common WHERE clauses
3. **Use native protocol**: `clickhouse-driver` instead of HTTP
4. **Batch inserts**: 100K-500K rows per insert

## 🔗 Related Patterns

- [Postgres to BigQuery](../postgres_bigquery_sync/) - Similar pattern for BigQuery
- [E-commerce Analytics](../ecommerce_analytics/) - Analytics use cases
