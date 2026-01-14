"""
Sync Control for ClickHouse
============================
Track sync progress in ClickHouse for resumable ETL.
"""

from datetime import date, datetime
from typing import Optional


class ClickHouseSyncControl:
    """
    Manages sync state in ClickHouse.
    
    Uses ReplicatedMergeTree for distributed environments.
    """
    
    def __init__(self, table: str, connection, control_table: str):
        self.table = table
        self.connection = connection
        self.control_table = control_table
    
    def get_last_id(self, sync_date: date) -> int:
        """Get last processed ID for a specific date."""
        result = self.connection.fetch(f"""
            SELECT last_max_id
            FROM {self.control_table}
            WHERE table_name = '{self.table}'
              AND sync_date = toDate('{sync_date.isoformat()}')
            ORDER BY synced_at DESC
            LIMIT 1
        """, one_row=True)
        
        return result.last_max_id if result else 0
    
    def update(self, sync_date: date, last_id: int) -> None:
        """Update sync progress."""
        self.connection.execute(f"""
            INSERT INTO {self.control_table} 
            (table_name, sync_date, last_max_id, synced_at)
            VALUES (
                %(table)s, 
                %(date)s, 
                %(max_id)s, 
                now()
            )
        """, {
            'table': self.table,
            'date': sync_date.isoformat(),
            'max_id': int(last_id),
        })


# ClickHouse DDL for sync_control table
SYNC_CONTROL_DDL = """
-- Local table (for each node in cluster)
CREATE TABLE IF NOT EXISTS analytics.sync_control_local ON CLUSTER '{cluster}'
(
    table_name   String,
    sync_date    Date,
    last_max_id  Int64,
    synced_at    DateTime
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/sync_control', '{replica}')
ORDER BY (table_name, sync_date, synced_at);

-- Distributed table (query across cluster)
CREATE TABLE IF NOT EXISTS analytics.sync_control ON CLUSTER '{cluster}'
AS analytics.sync_control_local
ENGINE = Distributed('{cluster}', 'analytics', 'sync_control_local', rand());
"""


# ClickHouse DDL for activity_logs table
ACTIVITY_LOGS_DDL = """
-- Local table (per node)
CREATE TABLE IF NOT EXISTS analytics.activity_logs_local ON CLUSTER '{cluster}'
(
    id           Int64,
    created_at   DateTime,
    updated_at   Nullable(DateTime),
    event_name   Nullable(String),
    description  String,
    user_id      Nullable(Int64),
    entity_id    Nullable(Int64),
    entity_type  Nullable(String),
    properties   Nullable(String),
    session_id   Nullable(String)
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/activity_logs', '{replica}')
PARTITION BY toYYYYMM(created_at)
ORDER BY (created_at, id);

-- Distributed table
CREATE TABLE IF NOT EXISTS analytics.activity_logs ON CLUSTER '{cluster}'
AS analytics.activity_logs_local
ENGINE = Distributed('{cluster}', 'analytics', 'activity_logs_local', rand());
"""
