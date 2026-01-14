# GDPR Data Anonymization Pipeline

Production pipeline for GDPR-compliant data anonymization, implementing the "right to be forgotten" for users who request account deletion.

## 🎯 Key Features

- **Multi-Dataset Anonymization**: Updates PII across multiple datasets in a single run
- **Consistent Fake Data**: Uses Faker with deterministic seeding for reproducibility
- **Batch Processing**: Temp table + UPDATE JOIN for efficient batch updates
- **Audit Trail**: Preserves anonymized_at timestamp for compliance

## 🏗️ Architecture

```
┌─────────────────┐
│  Accounts Table │
│  status='       │
│  forgotten'     │
└────────┬────────┘
         │
         ▼
┌─────────────────────────────────────────────────────────────┐
│                     Airflow DAG                              │
│                                                              │
│  1. Find forgotten accounts                                  │
│  2. Generate fake data (Faker)                              │
│  3. For each dataset:                                        │
│     - Create temp mapping table                              │
│     - UPDATE target SET pii = fake FROM temp                │
│     - Drop temp table                                        │
│  4. Mark accounts as anonymized                              │
└─────────────────────────────────────────────────────────────┘
         │
         ▼
┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐
│    Payments     │  │      CRM        │  │    Support      │
│   customers     │  │    contacts     │  │    tickets      │
│                 │  │                 │  │                 │
│ name → ANON_X   │  │ name → ANON_X   │  │ name → ANON_X   │
│ email → deleted │  │ email → deleted │  │ email → deleted │
└─────────────────┘  └─────────────────┘  └─────────────────┘
```

## 📁 Project Structure

```
gdpr_anonymization/
├── dags/
│   └── anonymize_user_data.py    # Main DAG
├── includes/
│   ├── __init__.py
│   └── connections.py            # BigQuery connection factory
└── README.md
```

## 🔧 Configuration

### Define Datasets to Anonymize

```python
DATASETS_TO_ANONYMIZE = [
    {
        "dataset": "payments",
        "table": "customers",
        "display_name": "Payment Customers",
        "pii_fields": ["name", "email", "phone", "address"],
    },
    # Add more datasets as needed
]
```

### Required Account Status

Users must be marked with `status = 'forgotten'` in the accounts table:

```sql
UPDATE accounts 
SET status = 'forgotten' 
WHERE id = 12345;
```

## 🔄 How It Works

### 1. Find Forgotten Accounts

```sql
SELECT id AS account_id
FROM accounts
WHERE status = 'forgotten'
  AND anonymized_at IS NULL
```

### 2. Generate Fake Data

```python
from faker import Faker

fake_data = {
    account_id: {
        "fake_name": "ANON_XKCD1234",
        "fake_email": "deleted_abc123@anonymized.local",
        "fake_phone": "000-000-0000",
    }
}
```

### 3. Batch Update with Temp Table

```sql
-- Create mapping table
CREATE TABLE temp_anonymization_mapping (
    account_id INT64,
    fake_name STRING,
    fake_email STRING
);

-- Bulk insert mappings
INSERT INTO temp_anonymization_mapping ...

-- Update target table using JOIN
UPDATE customers AS t
SET t.name = m.fake_name,
    t.email = m.fake_email
FROM temp_anonymization_mapping AS m
WHERE t.account_id = m.account_id;

-- Cleanup
DROP TABLE temp_anonymization_mapping;
```

## 📊 Compliance Features

| Feature | Description |
|---------|-------------|
| Anonymization, not deletion | Data structure preserved for analytics |
| Consistent identifiers | Same fake data across all datasets |
| Audit trail | `anonymized_at` timestamp recorded |
| Idempotent | Won't re-anonymize already processed accounts |

## 🔒 Security Considerations

1. **Run during low-traffic hours** (3:30 AM UTC default)
2. **Limit DAG permissions** - only necessary BigQuery access
3. **Log retention** - ensure logs don't contain PII
4. **Backup before first run** - test on non-prod first

## 🧪 Testing

Test with a single account first:

```sql
-- Mark test account
UPDATE accounts SET status = 'forgotten' WHERE id = 999999;

-- Run DAG manually
-- Check results:
SELECT * FROM customers WHERE account_id = 999999;
-- Should show: name = 'ANON_XXXXXXXX', email = 'deleted_xxx@anonymized.local'
```

## 🔗 Related Patterns

- [Postgres to BigQuery](../postgres_bigquery_sync/) - Source data sync
- [API to Warehouse](../api_warehouse_sync/) - Third-party data handling
