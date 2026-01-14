"""
Data Lineage with OpenLineage
==============================
This DAG demonstrates OpenLineage integration for automatic
data lineage tracking across your data pipelines.

Pattern includes:
- Automatic lineage extraction from SQL
- Custom lineage events for complex transforms
- Integration with Marquez for visualization
- Dataset-level metadata tracking

Use Case: Data governance, impact analysis, debugging data issues,
regulatory compliance (GDPR, SOX).
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.decorators import task
from airflow.lineage import AUTO
from airflow.lineage.entities import Table, File
from includes.lineage_utils import (
    emit_lineage_event,
    extract_sql_lineage,
    LineageContext,
)
from includes.connections import get_bigquery_connection

# Configuration
DATASET = "analytics"


with DAG(
    dag_id="lineage_enabled_etl",
    description="ETL pipeline with OpenLineage tracking",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["lineage", "etl", "governance"],
    # Enable OpenLineage integration
    # Requires: pip install apache-airflow-providers-openlineage
    # And OPENLINEAGE_URL environment variable
) as dag:
    
    @task(
        # Declare input/output datasets for lineage
        inlets=[
            Table(
                cluster="bigquery",
                database=DATASET,
                name="raw_transactions",
            ),
            Table(
                cluster="bigquery",
                database=DATASET,
                name="raw_users",
            ),
        ],
        outlets=[
            Table(
                cluster="bigquery",
                database=DATASET,
                name="stg_transactions",
            ),
        ],
    )
    def stage_transactions():
        """
        Stage raw transactions with lineage tracking.
        
        OpenLineage automatically captures:
        - Input datasets (raw_transactions, raw_users)
        - Output dataset (stg_transactions)
        - Schema information
        - Row counts
        """
        bq_conn = get_bigquery_connection(dataset=DATASET)
        
        sql = """
            SELECT
                t.id,
                t.user_id,
                t.amount,
                t.currency,
                t.status,
                t.created_at,
                u.email AS user_email,
                u.country AS user_country
            FROM raw_transactions t
            LEFT JOIN raw_users u ON t.user_id = u.id
            WHERE t.created_at >= CURRENT_DATE - 1
        """
        
        # Extract lineage from SQL (for custom tracking)
        lineage = extract_sql_lineage(sql)
        print(f"📊 Lineage: {lineage['inputs']} -> {lineage['outputs']}")
        
        # Execute
        result = bq_conn.execute(f"""
            CREATE OR REPLACE TABLE stg_transactions AS
            {sql}
        """)
        
        print("✅ Staged transactions with lineage tracking")
        return {"rows_processed": 1000}  # Would get from result
    
    @task(
        inlets=[
            Table(cluster="bigquery", database=DATASET, name="stg_transactions"),
        ],
        outlets=[
            Table(cluster="bigquery", database=DATASET, name="fct_transactions"),
        ],
    )
    def build_fact_table(stage_result: dict):
        """
        Build fact table with transformations.
        
        Custom lineage metadata includes:
        - Transformation logic
        - Data quality metrics
        - Column-level lineage
        """
        bq_conn = get_bigquery_connection(dataset=DATASET)
        
        # Build fact table
        sql = """
            SELECT
                id AS transaction_id,
                user_id,
                amount,
                currency,
                CASE 
                    WHEN amount > 1000 THEN 'high_value'
                    WHEN amount > 100 THEN 'medium_value'
                    ELSE 'low_value'
                END AS value_tier,
                status,
                created_at,
                user_email,
                user_country,
                CURRENT_TIMESTAMP() AS _etl_loaded_at
            FROM stg_transactions
        """
        
        bq_conn.execute(f"""
            CREATE OR REPLACE TABLE fct_transactions AS
            {sql}
        """)
        
        # Emit custom lineage event with column-level info
        emit_lineage_event(
            job_name="build_fact_table",
            inputs=["stg_transactions"],
            outputs=["fct_transactions"],
            column_lineage={
                "transaction_id": ["stg_transactions.id"],
                "value_tier": ["stg_transactions.amount"],  # Derived column
            },
        )
        
        print("✅ Built fact table with column-level lineage")
        return {"status": "success"}
    
    @task(
        inlets=[
            Table(cluster="bigquery", database=DATASET, name="fct_transactions"),
        ],
        outlets=[
            Table(cluster="bigquery", database=DATASET, name="daily_transaction_summary"),
        ],
    )
    def create_summary(fact_result: dict):
        """Create daily summary aggregation."""
        bq_conn = get_bigquery_connection(dataset=DATASET)
        
        bq_conn.execute("""
            CREATE OR REPLACE TABLE daily_transaction_summary AS
            SELECT
                DATE(created_at) AS date,
                user_country,
                value_tier,
                COUNT(*) AS transaction_count,
                SUM(amount) AS total_amount,
                AVG(amount) AS avg_amount
            FROM fct_transactions
            GROUP BY 1, 2, 3
        """)
        
        print("✅ Created daily summary")
    
    # DAG Flow
    staged = stage_transactions()
    fact = build_fact_table(staged)
    create_summary(fact)
