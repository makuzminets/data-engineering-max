"""
Backfill Utilities
===================
Helper functions for managed backfill operations.
"""

from datetime import date, datetime, timedelta
from typing import Dict, Any, Optional, List


def validate_date_range(
    start_date: date,
    end_date: date,
    max_days: int = 30,
) -> Dict[str, Any]:
    """
    Validate backfill date range.
    
    Args:
        start_date: Start of backfill range
        end_date: End of backfill range
        max_days: Maximum allowed days in range
        
    Returns:
        Dict with 'valid' bool and 'error' message if invalid
    """
    # Check order
    if start_date > end_date:
        return {
            "valid": False,
            "error": f"Start date ({start_date}) must be before end date ({end_date})",
        }
    
    # Check not in future
    today = date.today()
    if end_date > today:
        return {
            "valid": False,
            "error": f"End date ({end_date}) cannot be in the future",
        }
    
    # Check range size
    days = (end_date - start_date).days + 1
    if days > max_days:
        return {
            "valid": False,
            "error": f"Date range ({days} days) exceeds maximum ({max_days} days). "
                    f"Split into smaller batches.",
        }
    
    return {"valid": True, "days": days}


class BackfillTracker:
    """
    Track backfill progress in a control table.
    
    Useful for:
    - Resuming failed backfills
    - Audit trail of what was backfilled
    - Preventing duplicate backfills
    """
    
    def __init__(self, connection, control_table: str = "backfill_log"):
        self.conn = connection
        self.control_table = control_table
    
    def start_backfill(
        self,
        backfill_id: str,
        table: str,
        start_date: date,
        end_date: date,
    ) -> None:
        """Record start of a backfill operation."""
        self.conn.execute(f"""
            INSERT INTO {self.control_table} 
            (backfill_id, table_name, start_date, end_date, status, started_at)
            VALUES (
                '{backfill_id}',
                '{table}',
                '{start_date.isoformat()}',
                '{end_date.isoformat()}',
                'in_progress',
                CURRENT_TIMESTAMP()
            )
        """)
    
    def complete_backfill(
        self,
        backfill_id: str,
        rows_affected: int,
    ) -> None:
        """Mark backfill as completed."""
        self.conn.execute(f"""
            UPDATE {self.control_table}
            SET status = 'completed',
                rows_affected = {rows_affected},
                completed_at = CURRENT_TIMESTAMP()
            WHERE backfill_id = '{backfill_id}'
        """)
    
    def fail_backfill(
        self,
        backfill_id: str,
        error_message: str,
    ) -> None:
        """Mark backfill as failed."""
        # Escape quotes in error message
        safe_error = error_message.replace("'", "''")[:1000]
        self.conn.execute(f"""
            UPDATE {self.control_table}
            SET status = 'failed',
                error_message = '{safe_error}',
                completed_at = CURRENT_TIMESTAMP()
            WHERE backfill_id = '{backfill_id}'
        """)
    
    def get_recent_backfills(self, table: str, limit: int = 10) -> List[Dict]:
        """Get recent backfill history for a table."""
        result = self.conn.fetch(f"""
            SELECT *
            FROM {self.control_table}
            WHERE table_name = '{table}'
            ORDER BY started_at DESC
            LIMIT {limit}
        """)
        return result


# DDL for backfill_log table
BACKFILL_LOG_DDL = """
CREATE TABLE IF NOT EXISTS backfill_log (
    backfill_id STRING,
    table_name STRING,
    start_date DATE,
    end_date DATE,
    status STRING,  -- 'in_progress', 'completed', 'failed'
    rows_affected INT64,
    error_message STRING,
    started_at TIMESTAMP,
    completed_at TIMESTAMP,
    triggered_by STRING
);
"""
