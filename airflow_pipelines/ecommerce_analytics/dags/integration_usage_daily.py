"""
E-commerce Integration Usage Analytics Pipeline
================================================
This DAG demonstrates daily analytics aggregation for e-commerce integrations.

Pattern includes:
- Incremental processing from last processed date
- Gap-filling for days with zero activity
- TaskFlow API with typed parameters
- Optimized lookups with index-aware queries

Use Case: Tracking integration performance, e-commerce shop analytics,
daily metrics aggregation for reporting dashboards.
"""

from datetime import datetime, timedelta, date
from typing import List, NamedTuple, Optional
from collections import namedtuple
from airflow import DAG
from airflow.decorators import task
from includes.connections import get_postgres_connection, get_bigquery_connection

# Configuration
BIGQUERY_DATASET = "analytics"
TARGET_TABLE = "integration_usage_daily"
BATCH_SIZE = 100
BIGQUERY_INSERT_BATCH = 10000
START_DATE_DEFAULT = date(2023, 1, 1)


def fill_date_gaps(
    collection: List[NamedTuple],
    start_day: date,
    integration_created_at: date = None,
    initial_run: bool = False,
    end_day: date = None,
) -> List[NamedTuple]:
    """
    Fill missing dates with zero values.
    
    Ensures continuous time series even for days with no activity.
    Essential for accurate analytics and visualization.
    """
    if end_day is None:
        end_day = datetime.today().date() - timedelta(days=1)
    
    # Determine actual start date
    if initial_run and integration_created_at:
        start_day = integration_created_at.date() if hasattr(integration_created_at, 'date') else integration_created_at
    elif collection:
        min_date = min(row.day for row in collection)
        if start_day is None or start_day < min_date:
            start_day = min_date
    
    # Create Row type matching collection
    Row = namedtuple('Row', collection[0]._fields if collection else ['day', 'orders'])
    
    # Find existing days
    existing_days = {row.day for row in collection}
    
    # Fill gaps
    current_day = start_day
    while current_day <= end_day:
        if current_day not in existing_days:
            collection.append(Row(day=current_day, orders=0))
        current_day += timedelta(days=1)
    
    return sorted(collection, key=lambda row: row.day)


def create_empty_row(date_value: date) -> List[NamedTuple]:
    """Create a single row with zero orders for a given date."""
    Row = namedtuple('Row', ['day', 'orders'])
    return [Row(day=date_value, orders=0)]


def merge_shop_with_orders(shop, orders: List[NamedTuple]) -> List[NamedTuple]:
    """Combine shop metadata with daily order counts."""
    CombinedRow = namedtuple('CombinedRow', 
        ['id', 'account_id', 'integration_created_at', 'platform', 'day', 'orders'])
    
    return [
        CombinedRow(
            id=shop.id,
            account_id=shop.account_id,
            integration_created_at=shop.integration_created_at,
            platform=shop.platform,
            day=row.day,
            orders=row.orders,
        )
        for row in orders
    ]


with DAG(
    dag_id="ecommerce_integration_usage_daily",
    description="Daily e-commerce integration usage metrics aggregation",
    start_date=datetime(2023, 1, 1),
    schedule="0 4 * * *",  # 4 AM UTC daily
    catchup=False,
    tags=["analytics", "ecommerce", "daily"],
) as dag:
    
    @task()
    def fetch_last_processed_date() -> date:
        """
        Get the last processed date from BigQuery.
        Returns default start date if table is empty.
        """
        conn = get_bigquery_connection(dataset=BIGQUERY_DATASET)
        
        result = conn.fetch(f"""
            SELECT MAX(day) AS max_date 
            FROM {TARGET_TABLE}
            WHERE day < CURRENT_DATE()
        """, one_row=True)
        
        if result and result.max_date:
            # Continue from next day
            return result.max_date + timedelta(days=1)
        
        print(f"📅 No existing data, starting from {START_DATE_DEFAULT}")
        return START_DATE_DEFAULT
    
    @task()
    def fetch_order_min_id(start_date: date) -> int:
        """
        Find minimum order ID for the start date.
        Used to optimize queries with index range scans.
        """
        conn = get_postgres_connection()
        
        next_day = start_date + timedelta(days=1)
        result = conn.fetch(f"""
            SELECT id 
            FROM orders
            WHERE created_at >= '{start_date}'
              AND created_at < '{next_day}'
            ORDER BY id
            LIMIT 1
        """, one_row=True)
        
        return result.id if result else 0
    
    @task()
    def aggregate_integration_usage(start_date: date, order_min_id: int):
        """
        Main aggregation task.
        
        For each active e-commerce integration:
        1. Fetch daily order counts from start_date to yesterday
        2. Fill gaps for days with zero orders
        3. Insert results to BigQuery
        """
        print(f"📊 Starting aggregation from {start_date}")
        print(f"   Using order_min_id = {order_min_id}")
        
        # Skip if we're trying to process today's data
        if start_date >= datetime.today().date():
            print("⏭️ Only processing complete days. Skipping today.")
            return
        
        is_initial_run = start_date == START_DATE_DEFAULT
        if is_initial_run:
            print("🆕 Initial run - table is empty")
        
        pg_conn = get_postgres_connection()
        bq_conn = get_bigquery_connection(dataset=BIGQUERY_DATASET)
        
        shop_max_id = 0
        total_rows_inserted = 0
        
        while True:
            # Fetch batch of active shops
            shops = pg_conn.fetch(f"""
                SELECT
                    id,
                    account_id,
                    created_at AS integration_created_at,
                    platform
                FROM ecommerce_shops
                WHERE enabled = TRUE
                  AND deleted_at IS NULL
                  AND platform IS NOT NULL
                  AND id > {shop_max_id}
                ORDER BY id
                LIMIT {BATCH_SIZE}
            """)
            
            if not shops:
                break
            
            print(f"📦 Processing {len(shops)} shops (max_id > {shop_max_id})")
            
            rows_to_insert = []
            
            for shop in shops:
                shop_max_id = max(shop_max_id, shop.id)
                
                # Get daily order counts for this shop
                orders = pg_conn.fetch(f"""
                    SELECT 
                        DATE(created_at) AS day,
                        COUNT(*) AS orders
                    FROM orders
                    WHERE shop_id = {shop.id}
                      AND account_id = {shop.account_id}
                      AND created_at >= '{start_date}'
                      AND created_at < CURRENT_DATE
                      AND id > {order_min_id}
                    GROUP BY DATE(created_at)
                    ORDER BY day
                """)
                
                # Handle shops with no orders
                if not orders:
                    orders = create_empty_row(start_date)
                
                # Fill date gaps
                orders = fill_date_gaps(
                    collection=orders,
                    start_day=start_date,
                    integration_created_at=shop.integration_created_at,
                    initial_run=is_initial_run,
                )
                
                # Merge with shop metadata
                combined = merge_shop_with_orders(shop, orders)
                rows_to_insert.extend(combined)
                
                # Insert in batches for memory efficiency
                if len(rows_to_insert) >= BIGQUERY_INSERT_BATCH:
                    insert_to_bigquery(bq_conn, rows_to_insert)
                    total_rows_inserted += len(rows_to_insert)
                    rows_to_insert = []
            
            # Insert remaining rows
            if rows_to_insert:
                insert_to_bigquery(bq_conn, rows_to_insert)
                total_rows_inserted += len(rows_to_insert)
                rows_to_insert = []
        
        print(f"✅ Aggregation complete! Total rows: {total_rows_inserted}")


def insert_to_bigquery(conn, rows: List[NamedTuple]) -> None:
    """Insert rows to BigQuery target table."""
    formatted_rows = [
        {
            "shop_id": row.id,
            "account_id": row.account_id,
            "integration_created_at": row.integration_created_at.isoformat() 
                if hasattr(row.integration_created_at, 'isoformat') 
                else str(row.integration_created_at),
            "platform": row.platform,
            "day": row.day.isoformat() if hasattr(row.day, 'isoformat') else str(row.day),
            "orders_count": row.orders,
        }
        for row in rows
    ]
    
    conn.insert(table=TARGET_TABLE, rows=formatted_rows)
    print(f"   📤 Inserted {len(formatted_rows)} rows to {TARGET_TABLE}")


# Task dependencies using TaskFlow API
with dag:
    last_date = fetch_last_processed_date()
    min_order_id = fetch_order_min_id(last_date)
    aggregate_integration_usage(last_date, min_order_id)
