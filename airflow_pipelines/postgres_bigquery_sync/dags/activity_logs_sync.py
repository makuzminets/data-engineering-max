"""
Postgres to BigQuery ETL Pipeline with Chunked Sync
====================================================
This DAG demonstrates a production-grade pattern for syncing large tables
from PostgreSQL to BigQuery with:
- Chunked processing for large datasets (250K rows per chunk)
- Resumable sync with sync_control table tracking
- Date-based partitioning for efficient backfills
- Gap detection to handle missing dates

Use Case: Syncing activity logs, audit trails, or any time-series data
where you need incremental, resumable data transfers.
"""

from datetime import datetime, timedelta, date
import time
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from dateutil.relativedelta import relativedelta
from includes.sync_control import SyncControl
from includes.connections import get_postgres_connection, get_bigquery_connection

# Configuration
TARGET_DATASET = "analytics"
TARGET_TABLE = "activity_logs"
CHUNK_SIZE = 250000
DEFAULT_START_DATE = date(2023, 1, 1)

TARGET_TABLE_SCHEMA = [
    ("id", "INT64"),
    ("created_at", "TIMESTAMP"),
    ("updated_at", "TIMESTAMP"),
    ("event_name", "STRING"),
    ("event_type", "STRING"),
    ("user_id", "INT64"),
    ("entity_id", "INT64"),
    ("entity_type", "STRING"),
    ("properties", "JSON"),
    ("session_id", "STRING"),
]


def sync_activity_logs(**kwargs):
    """
    Main sync function with chunked processing and resumable state.
    
    Algorithm:
    1. Find first unprocessed date (gaps or continue from last)
    2. For each date: fetch data in chunks of CHUNK_SIZE
    3. Insert each chunk to BigQuery
    4. Track progress in sync_control table
    5. Mark date as finished when complete
    """
    pg_conn = get_postgres_connection()
    bq_conn = get_bigquery_connection(TARGET_DATASET)
    sync_control = SyncControl(TARGET_TABLE, bq_conn)
    
    end_date = date.today() - timedelta(days=1)  # Yesterday
    
    # Find next unprocessed date (handles gaps automatically)
    start_date = sync_control.get_next_unprocessed_date(DEFAULT_START_DATE, end_date)
    
    if start_date is None:
        print("✅ All dates have been processed! No work to do.")
        return
    
    print(f"📅 Processing from {start_date} to {end_date}")
    
    current_date = start_date
    while current_date <= end_date:
        process_single_day(
            pg_conn=pg_conn,
            bq_conn=bq_conn,
            sync_control=sync_control,
            process_date=current_date
        )
        current_date += timedelta(days=1)


def process_single_day(pg_conn, bq_conn, sync_control, process_date):
    """Process a single day's data in chunks."""
    
    if sync_control.is_finished(process_date):
        print(f"⏭️ Skipping {process_date} - already processed")
        return
    
    print(f"📊 Processing {process_date}")
    next_date = process_date + timedelta(days=1)
    last_id = sync_control.get_last_id(process_date)
    
    while True:
        start_time = time.perf_counter()
        
        # Fetch chunk from Postgres
        sql = f"""
            SELECT * FROM activity_logs
            WHERE created_at >= '{process_date}'
              AND created_at < '{next_date}'
              AND id > {last_id}
            ORDER BY id
            LIMIT {CHUNK_SIZE};
        """
        
        rows = pg_conn.fetch(sql)
        query_time = time.perf_counter() - start_time
        print(f"   Query took {query_time:.2f}s, found {len(rows) if rows else 0} rows")
        
        if not rows:
            sync_control.mark_as_finished(process_date, last_id)
            print(f"   ✅ Finished {process_date}")
            break
        
        # Transform and insert to BigQuery
        rows_to_insert = [
            {
                "id": row.id,
                "created_at": row.created_at.isoformat(),
                "updated_at": row.updated_at.isoformat() if row.updated_at else None,
                "event_name": row.event_name,
                "event_type": row.event_type,
                "user_id": row.user_id,
                "entity_id": row.entity_id,
                "entity_type": row.entity_type,
                "properties": row.properties,
                "session_id": row.session_id,
            }
            for row in rows
        ]
        
        bq_conn.insert_bulk(
            rows=rows_to_insert,
            schema=TARGET_TABLE_SCHEMA,
            table=TARGET_TABLE
        )
        print(f"   📤 Inserted {len(rows)} rows to BigQuery")
        
        # Update progress
        last_id = max(row.id for row in rows)
        sync_control.mark_as_in_progress(process_date, last_id)


# DAG Definition
dag = DAG(
    dag_id="postgres_to_bigquery_activity_logs",
    description="Sync activity logs from Postgres to BigQuery with chunked processing",
    schedule="@daily",
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=["etl", "postgres", "bigquery", "production"],
    default_args={
        "retries": 3,
        "retry_delay": timedelta(minutes=5),
    },
)

PythonOperator(
    task_id="sync_activity_logs",
    python_callable=sync_activity_logs,
    dag=dag,
)
