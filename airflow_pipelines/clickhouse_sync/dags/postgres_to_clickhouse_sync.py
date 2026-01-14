"""
Postgres to ClickHouse Sync Pipeline
=====================================
This DAG demonstrates syncing data from PostgreSQL to ClickHouse,
a columnar analytics database optimized for OLAP workloads.

Pattern includes:
- Chunked processing with DataFrame batches
- ClickHouse-specific optimizations (ReplicatedMergeTree)
- SSH tunneling for secure database access
- JSON column handling for nested data

Use Case: Real-time analytics, log aggregation, time-series data
where ClickHouse's columnar storage provides 10-100x query speedup.
"""

from datetime import datetime, timedelta, date
import json
from airflow import DAG
from airflow.decorators import task
from includes.connections import (
    get_postgres_connection,
    get_clickhouse_connection,
)
from includes.sync_control import ClickHouseSyncControl

# Configuration
CHUNK_SIZE = 200000
START_DATE = date(2023, 1, 1)
CLICKHOUSE_DATABASE = "analytics"
TARGET_TABLE = "activity_logs"
CONTROL_TABLE = "sync_control"


with DAG(
    dag_id="postgres_to_clickhouse_sync",
    description="Sync activity logs from Postgres to ClickHouse for analytics",
    start_date=datetime(2023, 1, 1),
    schedule=None,  # Manual trigger or external scheduler
    catchup=False,
    tags=["etl", "clickhouse", "analytics"],
) as dag:
    
    @task()
    def sync_to_clickhouse():
        """
        Sync data from Postgres to ClickHouse with chunked processing.
        
        ClickHouse advantages for this use case:
        - 10-100x faster analytical queries
        - Efficient compression (5-10x storage reduction)
        - Real-time data availability
        """
        pg_conn = get_postgres_connection()
        ch_conn = get_clickhouse_connection()
        sync_control = ClickHouseSyncControl(
            table=f"{CLICKHOUSE_DATABASE}.{TARGET_TABLE}",
            connection=ch_conn,
            control_table=f"{CLICKHOUSE_DATABASE}.{CONTROL_TABLE}",
        )
        
        current_date = START_DATE
        end_date = datetime.now().date() - timedelta(days=1)
        
        print(f"🚀 Starting sync from {current_date} to {end_date}")
        
        while current_date <= end_date:
            next_date = current_date + timedelta(days=1)
            print(f"\n📅 Processing: {current_date}")
            
            # Get last processed ID for this date
            max_id = sync_control.get_last_id(current_date)
            print(f"   Starting from ID > {max_id}")
            
            rows_synced = 0
            
            while True:
                # Fetch chunk from Postgres as DataFrame
                df = pg_conn.fetch_dataframe(f"""
                    SELECT * FROM activity_logs
                    WHERE created_at >= '{current_date}'
                      AND created_at < '{next_date}'
                      AND id > {max_id}
                    ORDER BY id
                    LIMIT {CHUNK_SIZE}
                """)
                
                if df.empty:
                    print(f"   ✅ No more rows for {current_date}")
                    break
                
                # Handle JSON columns (ClickHouse expects string)
                if 'properties' in df.columns:
                    df['properties'] = df['properties'].apply(
                        lambda x: json.dumps(x) if isinstance(x, (dict, list)) else x
                    )
                
                # Insert to ClickHouse
                ch_conn.insert_dataframe(
                    table=f"{CLICKHOUSE_DATABASE}.{TARGET_TABLE}",
                    df=df,
                )
                
                new_max_id = df['id'].max()
                rows_synced += len(df)
                
                # Update sync control
                sync_control.update(current_date, new_max_id)
                
                print(f"   📤 Inserted {len(df)} rows (total: {rows_synced})")
                max_id = new_max_id
            
            if rows_synced > 0:
                print(f"   📊 Total for {current_date}: {rows_synced} rows")
            
            current_date = next_date
        
        print("\n🎉 Sync complete!")
    
    sync_to_clickhouse()
