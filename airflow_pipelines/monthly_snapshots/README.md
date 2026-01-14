# Monthly Snapshots with MERGE Pattern

Production pipeline for monthly metrics aggregation using MERGE (upsert) for idempotent data updates.

## 🎯 Key Features

- **MERGE (Upsert)**: Update existing + insert new in single atomic operation
- **Staging Table Pattern**: Load to temp, then MERGE to target
- **Late-Arriving Data**: Safely re-aggregate without duplicates
- **Monthly Aggregation**: DATE_TRUNC for consistent monthly buckets

## 🏗️ Architecture

```
┌─────────────────┐     ┌──────────────────┐     ┌─────────────────┐
│   PostgreSQL    │────▶│   Aggregate      │────▶│  staging_table  │
│                 │     │   Monthly        │     └────────┬────────┘
│    campaigns    │     └──────────────────┘              │
└─────────────────┘                                       │ MERGE
                                                          ▼
                                               ┌─────────────────┐
                                               │  target_table   │
                                               │                 │
                                               │ - UPDATE if     │
                                               │   exists        │
                                               │ - INSERT if new │
                                               └─────────────────┘
```

## 📁 Project Structure

```
monthly_snapshots/
├── dags/
│   └── campaigns_monthly_snapshot.py    # Main DAG
├── includes/
│   ├── __init__.py
│   └── connections.py
└── README.md
```

## 🔄 MERGE Pattern Explained

### The Problem

Running monthly aggregations multiple times can cause:
- Duplicate records
- Incorrect totals
- Data quality issues

### The Solution: MERGE

```sql
MERGE target_table T
USING staging_table S
ON T.year_month = S.year_month AND T.account_id = S.account_id

WHEN MATCHED THEN UPDATE SET
    campaigns_total = S.campaigns_total,
    updated_at = CURRENT_TIMESTAMP()

WHEN NOT MATCHED THEN INSERT (
    year_month, account_id, campaigns_total, updated_at
) VALUES (
    S.year_month, S.account_id, S.campaigns_total, CURRENT_TIMESTAMP()
)
```

### Benefits

| Feature | Description |
|---------|-------------|
| **Idempotent** | Safe to re-run anytime |
| **Atomic** | All-or-nothing operation |
| **Efficient** | Single pass through data |
| **Audit Trail** | `updated_at` tracks changes |

## 📊 Output Schema

```sql
CREATE TABLE campaigns_monthly (
    year_month DATE,           -- First day of month
    account_id INT64,
    campaigns_total INT64,     -- All campaigns
    campaigns_sent INT64,      -- Status = 'sent'
    campaigns_draft INT64,     -- Status = 'draft'
    campaigns_scheduled INT64, -- Status = 'scheduled'
    updated_at TIMESTAMP       -- Last update time
)
```

## 🧪 Testing Idempotency

```sql
-- Run the DAG twice for the same month
-- Then verify no duplicates:
SELECT year_month, account_id, COUNT(*)
FROM campaigns_monthly
GROUP BY 1, 2
HAVING COUNT(*) > 1;
-- Should return 0 rows
```

## 📈 Example Queries

### Month-over-Month Comparison

```sql
SELECT 
    year_month,
    SUM(campaigns_sent) AS total_sent,
    LAG(SUM(campaigns_sent)) OVER (ORDER BY year_month) AS prev_month,
    ROUND(
        (SUM(campaigns_sent) - LAG(SUM(campaigns_sent)) OVER (ORDER BY year_month)) 
        / LAG(SUM(campaigns_sent)) OVER (ORDER BY year_month) * 100, 
        2
    ) AS growth_pct
FROM campaigns_monthly
GROUP BY 1
ORDER BY 1;
```

### Completion Rate by Month

```sql
SELECT 
    year_month,
    SUM(campaigns_sent) AS sent,
    SUM(campaigns_total) AS total,
    ROUND(SUM(campaigns_sent) / SUM(campaigns_total) * 100, 2) AS completion_rate
FROM campaigns_monthly
GROUP BY 1;
```

## 🔗 Related Patterns

- [Postgres to BigQuery](../postgres_bigquery_sync/) - Incremental sync
- [E-commerce Analytics](../ecommerce_analytics/) - Daily aggregations
