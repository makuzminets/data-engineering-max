# API to Data Warehouse Sync Pipeline

Production pipeline for syncing data from external REST APIs to BigQuery with currency conversion and atomic table swaps.

## 🎯 Key Features

- **Paginated API Fetching**: Handles cursor/page-based pagination automatically
- **Currency Conversion**: Converts multi-currency amounts to USD using live exchange rates
- **Atomic Table Swap**: Zero-downtime deployments using temp table pattern
- **Duplicate Handling**: Tracks processed IDs to handle duplicates across pages

## 🏗️ Architecture

```
┌─────────────────┐     ┌──────────────────┐     ┌─────────────────┐
│  External API   │────▶│   Airflow DAG    │────▶│    BigQuery     │
│  (Affiliate     │     │                  │     │                 │
│   Stripe, etc)  │     │  - Pagination    │     │  partners_tmp   │
└─────────────────┘     │  - Transform     │     │      ↓          │
                        │  - Currency Conv │     │   partners      │
┌─────────────────┐     └──────────────────┘     └─────────────────┘
│ Exchange Rate   │────▶         │
│     API         │              │
└─────────────────┘              ▼
                        ┌──────────────────┐
                        │ Atomic Swap      │
                        │ tmp → production │
                        └──────────────────┘
```

## 📁 Project Structure

```
api_warehouse_sync/
├── dags/
│   └── affiliate_partners_sync.py   # Main DAG
├── includes/
│   ├── __init__.py
│   ├── api_client.py                # API client with pagination
│   ├── connections.py               # BigQuery connection factory
│   └── currency.py                  # Currency conversion service
└── README.md
```

## 🔧 Setup

### 1. Set Airflow Variables

```python
from airflow.models import Variable

Variable.set("AFFILIATE_API_URL", "https://api.affiliate-platform.com/v1")
Variable.set("AFFILIATE_API_KEY", "your-api-key")
Variable.set("EXCHANGE_RATE_API_URL", "https://api.exchangerate-api.com/v4/latest")
```

### 2. Create BigQuery Dataset

```sql
CREATE SCHEMA affiliate;
```

## 🔄 How It Works

### Sync Process

1. **Initialize**: Create temp table, truncate any existing data
2. **Paginate**: Loop through all API pages
   - Fetch page of partners
   - Get detailed data for each partner
   - Transform and enrich (currency conversion, calculated fields)
   - Insert batch to temp table
3. **Atomic Swap**: Rename temp → production in single transaction

### Currency Conversion

```python
# Input formats supported:
{"EUR": 1500.00}  # Converted to USD
{"USD": 1500.00}  # Kept as-is
1500.00           # Assumed USD
```

### Atomic Table Swap

```sql
BEGIN TRANSACTION;
  DROP TABLE IF EXISTS partners_old;
  ALTER TABLE partners RENAME TO partners_old;
  ALTER TABLE partners_tmp RENAME TO partners;
  DROP TABLE IF EXISTS partners_old;
COMMIT;
```

This ensures:
- Zero downtime during updates
- Consistent snapshot of data
- Easy rollback if needed

## 📊 Output Schema

| Column | Type | Description |
|--------|------|-------------|
| partner_id | STRING | Unique partner identifier |
| name | STRING | Partner full name |
| email | STRING | Contact email |
| signups | INT64 | Total referred signups |
| customers | INT64 | Converted customers |
| revenue_usd | FLOAT64 | Total revenue in USD |
| conversion_rate | FLOAT64 | customers/signups * 100 |

## 🧪 Testing

Use `MockAffiliateAPIClient` for testing without real API calls:

```python
from includes.api_client import MockAffiliateAPIClient

client = MockAffiliateAPIClient()
partners = client.get_partners()  # Returns 250 mock partners
```

## 🔗 Related Patterns

- [Postgres to BigQuery](../postgres_bigquery_sync/) - Database sync with chunking
- [GDPR Anonymization](../gdpr_anonymization/) - Data privacy compliance
