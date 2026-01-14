# Data Lineage with OpenLineage

Automatic data lineage tracking using OpenLineage for data governance and impact analysis.

## 🎯 Key Features

- **Automatic Lineage**: Extract from SQL statements
- **Column-Level Tracking**: Know which columns derive from where
- **OpenLineage Standard**: Compatible with Marquez, DataHub, Atlan
- **Custom Events**: Add metadata and business context

## 🏗️ Architecture

```
┌─────────────────┐     ┌──────────────────┐     ┌─────────────────┐
│   Airflow DAG   │────▶│  OpenLineage     │────▶│    Marquez      │
│                 │     │  Integration     │     │    (UI)         │
│  @task(inlets,  │     │                  │     │                 │
│   outlets)      │     │  - Auto extract  │     │  - Visualize    │
│                 │     │  - SQL parsing   │     │  - Impact       │
│                 │     │  - Custom events │     │  - History      │
└─────────────────┘     └──────────────────┘     └─────────────────┘
```

## 📁 Project Structure

```
data_lineage/
├── dags/
│   └── lineage_enabled_etl.py    # DAG with lineage decorators
├── includes/
│   ├── __init__.py
│   ├── connections.py
│   └── lineage_utils.py          # Lineage helpers
└── README.md
```

## 🔧 Setup

### 1. Install OpenLineage Provider

```bash
pip install apache-airflow-providers-openlineage
```

### 2. Configure OpenLineage

```bash
# Environment variables
export OPENLINEAGE_URL=http://localhost:5000  # Marquez API
export OPENLINEAGE_NAMESPACE=airflow
```

Or in `airflow.cfg`:

```ini
[openlineage]
transport = {"type": "http", "url": "http://localhost:5000"}
namespace = airflow
```

### 3. Start Marquez (Optional UI)

```bash
docker run -p 5000:5000 -p 5001:5001 marquezproject/marquez
```

## 📊 Lineage Declaration

### Using Task Decorators

```python
from airflow.lineage.entities import Table

@task(
    inlets=[
        Table(cluster="bigquery", database="analytics", name="source_table"),
    ],
    outlets=[
        Table(cluster="bigquery", database="analytics", name="target_table"),
    ],
)
def transform_data():
    ...
```

### Column-Level Lineage

```python
from includes.lineage_utils import emit_lineage_event

emit_lineage_event(
    job_name="build_fact_table",
    inputs=["stg_transactions"],
    outputs=["fct_transactions"],
    column_lineage={
        "transaction_id": ["stg_transactions.id"],
        "value_tier": ["stg_transactions.amount"],  # Derived
    },
)
```

### Context Manager

```python
from includes.lineage_utils import LineageContext

with LineageContext(job_name="my_transform") as ctx:
    ctx.add_input("source_table")
    # ... do work ...
    ctx.add_output("target_table")
    ctx.add_column_lineage("new_col", ["source_table.old_col"])
```

## 🔍 SQL Lineage Extraction

```python
from includes.lineage_utils import extract_sql_lineage

sql = """
    SELECT t.id, u.name
    FROM transactions t
    JOIN users u ON t.user_id = u.id
"""

lineage = extract_sql_lineage(sql)
# {'inputs': ['transactions', 'users'], 'outputs': []}
```

## 📈 Use Cases

| Use Case | How Lineage Helps |
|----------|-------------------|
| **Impact Analysis** | Know what breaks if source changes |
| **Debugging** | Trace data issues back to source |
| **Compliance** | Document data flows for GDPR/SOX |
| **Documentation** | Auto-generate data catalogs |
| **Optimization** | Identify redundant pipelines |

## 🖥️ Marquez UI

View lineage in Marquez at `http://localhost:5001`:

```
raw_transactions ──┐
                   ├──▶ stg_transactions ──▶ fct_transactions ──▶ daily_summary
raw_users ─────────┘
```

## 🔗 Related Patterns

- [Data Quality](../data_quality/) - Validate lineage endpoints
- [Alerting](../alerting/) - Notify on lineage changes
