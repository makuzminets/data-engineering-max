"""
Sync Control - Resumable ETL State Management
==============================================
Tracks sync progress in a control table to enable:
- Resumable syncs after failures
- Gap detection for backfills
- Idempotent re-runs

Schema for sync_control table in BigQuery:
CREATE TABLE sync_control (
    table_name STRING,
    sync_date DATE,
    last_max_id INT64,
    is_finished BOOL,
    synced_at TIMESTAMP
)
"""

from datetime import datetime, date
from typing import Optional
from dateutil.relativedelta import relativedelta


class SyncControl:
    """Manages sync state for resumable ETL pipelines."""
    
    def __init__(self, table: str, connection):
        """
        Initialize SyncControl.
        
        Args:
            table: Name of the source table being synced
            connection: BigQuery connection instance
        """
        self.table = table
        self.connection = connection
        self.control_table = "sync_control"
    
    def get_last_id(self, sync_date: date) -> int:
        """
        Get the last processed ID for a specific date.
        
        Returns 0 if no records found, allowing fresh start.
        """
        query = f"""
            SELECT last_max_id
            FROM {self.control_table}
            WHERE table_name = '{self.table}'
              AND sync_date = '{sync_date.isoformat()}'
            ORDER BY synced_at DESC
            LIMIT 1
        """
        result = self.connection.fetch(query, one_row=True)
        return result.last_max_id if result else 0
    
    def is_finished(self, sync_date: date) -> bool:
        """Check if sync is complete for a given date."""
        query = f"""
            SELECT is_finished
            FROM {self.control_table}
            WHERE table_name = '{self.table}'
              AND sync_date = '{sync_date.isoformat()}'
            ORDER BY synced_at DESC
            LIMIT 1
        """
        result = self.connection.fetch(query, one_row=True)
        return bool(result.is_finished) if result else False
    
    def _insert(self, sync_date: date, last_id: int, is_finished: bool) -> None:
        """Insert a sync control record."""
        self.connection.insert(
            table=self.control_table,
            rows=[{
                "table_name": self.table,
                "sync_date": sync_date.isoformat(),
                "last_max_id": last_id,
                "is_finished": is_finished,
                "synced_at": datetime.now().isoformat(),
            }]
        )
    
    def mark_as_finished(self, sync_date: date, last_id: int) -> None:
        """Mark a date as fully processed."""
        print(f"   Marking {sync_date} as finished with last_id={last_id}")
        self._insert(sync_date, last_id, is_finished=True)
    
    def mark_as_in_progress(self, sync_date: date, last_id: int) -> None:
        """Update progress for a date (for resumability)."""
        self._insert(sync_date, last_id, is_finished=False)
    
    def get_next_unprocessed_date(
        self, 
        start_date: date, 
        end_date: date
    ) -> Optional[date]:
        """
        Find the next date that needs processing.
        
        Algorithm:
        1. Check if any records exist - if not, return start_date
        2. Scan from min_date to max_date for gaps
        3. Then continue from max_date looking for unfinished dates
        """
        # Check for existing records
        query = f"""
            SELECT 
                MIN(sync_date) as min_date, 
                MAX(sync_date) as max_date
            FROM {self.control_table}
            WHERE table_name = '{self.table}'
        """
        result = self.connection.fetch(query, one_row=True)
        
        if not result or not result.min_date:
            print(f"   No sync records found, starting from {start_date}")
            return start_date
        
        min_date = result.min_date
        max_date = result.max_date
        
        # Scan for gaps in the date range
        current_date = min_date
        while current_date <= max_date:
            check_query = f"""
                SELECT sync_date 
                FROM {self.control_table}
                WHERE table_name = '{self.table}' 
                  AND sync_date = '{current_date.isoformat()}'
                LIMIT 1
            """
            exists = self.connection.fetch(check_query, one_row=True)
            
            if not exists:
                print(f"   Found gap at {current_date}")
                return current_date
            
            current_date += relativedelta(days=1)
        
        # No gaps, check for unfinished dates after max_date
        current_date = max_date
        while current_date <= end_date:
            if not self.is_finished(current_date):
                print(f"   Found unfinished date: {current_date}")
                return current_date
            current_date += relativedelta(days=1)
        
        return None
