"""
30-Day Rolling Stats Aggregation
=================================
This DAG demonstrates a rolling window aggregation pattern,
rebuilding a summary table with the last 30 days of data.

Pattern includes:
- Full table rebuild (WRITE_TRUNCATE)
- Rolling window calculation
- Breakdown by category (automation vs campaign)

Use Case: Dashboard metrics, performance summaries, 
rolling KPIs that need to reflect current 30-day window.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.decorators import task
from includes.connections import get_postgres_connection, get_bigquery_connection

# Configuration
BIGQUERY_DATASET = "analytics"
TARGET_TABLE = "sending_stats_30d"


with DAG(
    dag_id="ecommerce_sending_stats_30d",
    description="Weekly rebuild of 30-day sending statistics",
    start_date=datetime(2024, 1, 1),
    schedule="@weekly",
    catchup=False,
    tags=["analytics", "stats", "weekly"],
) as dag:
    
    @task()
    def rebuild_30d_stats():
        """
        Rebuild 30-day sending statistics table.
        
        Full table rebuild ensures:
        - Accurate 30-day rolling window
        - No data duplication issues
        - Simple, idempotent operation
        """
        pg_conn = get_postgres_connection()
        bq_conn = get_bigquery_connection(dataset=BIGQUERY_DATASET)
        
        print("📊 Fetching 30-day sending statistics...")
        
        # Aggregate from source
        results = pg_conn.fetch("""
            SELECT 
                account_id,
                COUNT(id) AS emails_30d,
                COUNT(CASE WHEN type = 'automation' THEN id END) AS automation_emails_30d,
                COUNT(CASE WHEN type = 'campaign' THEN id END) AS campaign_emails_30d,
                COUNT(CASE WHEN type = 'transactional' THEN id END) AS transactional_emails_30d
            FROM emails
            WHERE created_at >= NOW() - INTERVAL '30 days'
              AND status = 'sent'
            GROUP BY account_id
            HAVING COUNT(id) > 0
        """)
        
        if not results:
            print("❌ No data found for last 30 days")
            return
        
        print(f"📈 Found {len(results)} accounts with sending activity")
        
        # Recreate target table (ensures clean state)
        bq_conn.execute(f"DROP TABLE IF EXISTS {TARGET_TABLE}")
        bq_conn.execute(f"""
            CREATE TABLE {TARGET_TABLE} (
                account_id INT64,
                emails_30d INT64,
                automation_emails_30d INT64,
                campaign_emails_30d INT64,
                transactional_emails_30d INT64,
                updated_at TIMESTAMP
            )
        """)
        
        # Prepare rows
        rows_to_insert = [
            {
                "account_id": row.account_id,
                "emails_30d": row.emails_30d,
                "automation_emails_30d": row.automation_emails_30d,
                "campaign_emails_30d": row.campaign_emails_30d,
                "transactional_emails_30d": row.transactional_emails_30d,
                "updated_at": datetime.now().isoformat(),
            }
            for row in results
        ]
        
        # Bulk insert
        bq_conn.insert_bulk(
            rows=rows_to_insert,
            schema=[
                ("account_id", "INT64"),
                ("emails_30d", "INT64"),
                ("automation_emails_30d", "INT64"),
                ("campaign_emails_30d", "INT64"),
                ("transactional_emails_30d", "INT64"),
                ("updated_at", "TIMESTAMP"),
            ],
            table=TARGET_TABLE,
            write_disposition="WRITE_TRUNCATE",
        )
        
        print(f"✅ Inserted {len(rows_to_insert)} rows to {TARGET_TABLE}")
    
    rebuild_30d_stats()
