"""
Monthly Snapshots with MERGE (Upsert) Pattern
==============================================
This DAG demonstrates monthly aggregation with MERGE pattern,
enabling incremental updates without duplicates.

Pattern includes:
- Monthly aggregation with date_trunc
- MERGE (upsert) for idempotent updates
- Staging table pattern for atomic operations
- Multiple metrics in single aggregation

Use Case: Monthly KPIs, campaign performance snapshots,
historical trend analysis with late-arriving data support.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.decorators import task
from includes.connections import get_postgres_connection, get_bigquery_connection

# Configuration
BIGQUERY_DATASET = "analytics"
TARGET_TABLE = "campaigns_monthly"


with DAG(
    dag_id="campaigns_monthly_snapshot",
    description="Monthly campaign metrics snapshot with MERGE pattern",
    start_date=datetime(2024, 1, 1),
    schedule="@weekly",
    catchup=False,
    tags=["analytics", "monthly", "snapshot"],
    default_args={
        "retries": 1,
        "retry_delay": timedelta(minutes=15),
        "execution_timeout": timedelta(minutes=15),
    },
) as dag:
    
    @task()
    def create_monthly_snapshot():
        """
        Create monthly campaign snapshot with MERGE.
        
        MERGE (upsert) ensures:
        - Existing records are updated with latest values
        - New records are inserted
        - No duplicates even with reruns
        """
        pg_conn = get_postgres_connection()
        bq_conn = get_bigquery_connection(dataset=BIGQUERY_DATASET)
        
        print("📊 Aggregating monthly campaign metrics...")
        
        # Aggregate last month's data
        results = pg_conn.fetch("""
            WITH monthly_campaigns AS (
                SELECT
                    DATE_TRUNC('month', created_at)::DATE AS year_month,
                    account_id,
                    COUNT(DISTINCT id) AS campaigns_total
                FROM campaigns
                WHERE created_at >= DATE_TRUNC('month', CURRENT_DATE - INTERVAL '1 month')
                  AND created_at < DATE_TRUNC('month', CURRENT_DATE)
                GROUP BY 1, 2
            ),
            campaigns_by_status AS (
                SELECT
                    DATE_TRUNC('month', created_at)::DATE AS year_month,
                    account_id,
                    COUNT(DISTINCT CASE WHEN status = 'sent' THEN id END) AS campaigns_sent,
                    COUNT(DISTINCT CASE WHEN status = 'draft' THEN id END) AS campaigns_draft,
                    COUNT(DISTINCT CASE WHEN status = 'scheduled' THEN id END) AS campaigns_scheduled
                FROM campaigns
                WHERE created_at >= DATE_TRUNC('month', CURRENT_DATE - INTERVAL '1 month')
                  AND created_at < DATE_TRUNC('month', CURRENT_DATE)
                GROUP BY 1, 2
            )
            SELECT
                mc.year_month,
                mc.account_id,
                mc.campaigns_total,
                COALESCE(cs.campaigns_sent, 0) AS campaigns_sent,
                COALESCE(cs.campaigns_draft, 0) AS campaigns_draft,
                COALESCE(cs.campaigns_scheduled, 0) AS campaigns_scheduled
            FROM monthly_campaigns mc
            LEFT JOIN campaigns_by_status cs 
                ON mc.year_month = cs.year_month 
                AND mc.account_id = cs.account_id
        """)
        
        if not results:
            print("ℹ️ No campaigns found for last month")
            return
        
        print(f"📈 Found {len(results)} account-month records")
        
        # Prepare rows for staging
        rows_to_insert = [
            {
                "year_month": row.year_month.isoformat(),
                "account_id": row.account_id,
                "campaigns_total": row.campaigns_total,
                "campaigns_sent": row.campaigns_sent,
                "campaigns_draft": row.campaigns_draft,
                "campaigns_scheduled": row.campaigns_scheduled,
            }
            for row in results
        ]
        
        staging_table = f"{TARGET_TABLE}_staging"
        
        # Create/replace staging table
        bq_conn.execute(f"""
            CREATE OR REPLACE TABLE {staging_table} (
                year_month DATE,
                account_id INT64,
                campaigns_total INT64,
                campaigns_sent INT64,
                campaigns_draft INT64,
                campaigns_scheduled INT64
            )
        """)
        print(f"📝 Created staging table: {staging_table}")
        
        # Insert to staging
        bq_conn.insert_bulk(
            rows=rows_to_insert,
            schema=[
                ("year_month", "DATE"),
                ("account_id", "INT64"),
                ("campaigns_total", "INT64"),
                ("campaigns_sent", "INT64"),
                ("campaigns_draft", "INT64"),
                ("campaigns_scheduled", "INT64"),
            ],
            table=staging_table,
            write_disposition="WRITE_TRUNCATE",
        )
        print(f"📤 Inserted {len(rows_to_insert)} rows to staging")
        
        # Ensure target table exists
        bq_conn.execute(f"""
            CREATE TABLE IF NOT EXISTS {TARGET_TABLE} (
                year_month DATE,
                account_id INT64,
                campaigns_total INT64,
                campaigns_sent INT64,
                campaigns_draft INT64,
                campaigns_scheduled INT64,
                updated_at TIMESTAMP
            )
        """)
        
        # MERGE: Update existing, insert new
        bq_conn.execute(f"""
            MERGE {TARGET_TABLE} T
            USING {staging_table} S
            ON T.year_month = S.year_month AND T.account_id = S.account_id
            
            WHEN MATCHED THEN UPDATE SET
                campaigns_total = S.campaigns_total,
                campaigns_sent = S.campaigns_sent,
                campaigns_draft = S.campaigns_draft,
                campaigns_scheduled = S.campaigns_scheduled,
                updated_at = CURRENT_TIMESTAMP()
            
            WHEN NOT MATCHED THEN INSERT (
                year_month, account_id, campaigns_total, 
                campaigns_sent, campaigns_draft, campaigns_scheduled, updated_at
            ) VALUES (
                S.year_month, S.account_id, S.campaigns_total,
                S.campaigns_sent, S.campaigns_draft, S.campaigns_scheduled, CURRENT_TIMESTAMP()
            )
        """)
        print(f"✅ MERGE complete - upserted {len(rows_to_insert)} records")
        
        # Cleanup staging
        bq_conn.execute(f"DROP TABLE IF EXISTS {staging_table}")
        print("🗑️ Dropped staging table")
    
    create_monthly_snapshot()
