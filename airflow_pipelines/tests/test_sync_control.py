"""
Unit Tests for Sync Control
============================
Tests for the sync_control module used in chunked ETL pipelines.
"""

import pytest
from datetime import date, datetime
from unittest.mock import Mock, MagicMock


class TestSyncControl:
    """Tests for SyncControl class."""
    
    def test_get_last_id_returns_zero_when_no_records(self):
        """Should return 0 when no sync records exist."""
        mock_conn = Mock()
        mock_conn.fetch.return_value = None
        
        # Import here to avoid Airflow dependency in tests
        from postgres_bigquery_sync.includes.sync_control import SyncControl
        
        sync = SyncControl(table="test_table", connection=mock_conn)
        result = sync.get_last_id(date(2024, 1, 1))
        
        assert result == 0
    
    def test_get_last_id_returns_stored_value(self):
        """Should return last_max_id from database."""
        mock_conn = Mock()
        mock_result = Mock()
        mock_result.last_max_id = 12345
        mock_conn.fetch.return_value = mock_result
        
        from postgres_bigquery_sync.includes.sync_control import SyncControl
        
        sync = SyncControl(table="test_table", connection=mock_conn)
        result = sync.get_last_id(date(2024, 1, 1))
        
        assert result == 12345
    
    def test_is_finished_returns_false_when_no_records(self):
        """Should return False when no sync records exist."""
        mock_conn = Mock()
        mock_conn.fetch.return_value = None
        
        from postgres_bigquery_sync.includes.sync_control import SyncControl
        
        sync = SyncControl(table="test_table", connection=mock_conn)
        result = sync.is_finished(date(2024, 1, 1))
        
        assert result is False
    
    def test_is_finished_returns_true_when_marked_finished(self):
        """Should return True when is_finished is 1."""
        mock_conn = Mock()
        mock_result = Mock()
        mock_result.is_finished = 1
        mock_conn.fetch.return_value = mock_result
        
        from postgres_bigquery_sync.includes.sync_control import SyncControl
        
        sync = SyncControl(table="test_table", connection=mock_conn)
        result = sync.is_finished(date(2024, 1, 1))
        
        assert result is True
    
    def test_mark_as_finished_inserts_with_is_finished_true(self):
        """Should insert record with is_finished=True."""
        mock_conn = Mock()
        
        from postgres_bigquery_sync.includes.sync_control import SyncControl
        
        sync = SyncControl(table="test_table", connection=mock_conn)
        sync.mark_as_finished(date(2024, 1, 1), last_id=999)
        
        # Verify insert was called
        mock_conn.insert.assert_called_once()
        call_args = mock_conn.insert.call_args
        
        rows = call_args.kwargs.get("rows", call_args[1].get("rows"))
        assert rows[0]["is_finished"] is True
        assert rows[0]["last_max_id"] == 999


class TestBackfillUtils:
    """Tests for backfill utilities."""
    
    def test_validate_date_range_valid(self):
        """Should accept valid date range."""
        from backfill_manager.includes.backfill_utils import validate_date_range
        
        result = validate_date_range(
            start_date=date(2024, 1, 1),
            end_date=date(2024, 1, 15),
            max_days=30,
        )
        
        assert result["valid"] is True
        assert result["days"] == 15
    
    def test_validate_date_range_inverted_dates(self):
        """Should reject when start > end."""
        from backfill_manager.includes.backfill_utils import validate_date_range
        
        result = validate_date_range(
            start_date=date(2024, 1, 15),
            end_date=date(2024, 1, 1),
            max_days=30,
        )
        
        assert result["valid"] is False
        assert "before" in result["error"].lower()
    
    def test_validate_date_range_exceeds_max_days(self):
        """Should reject range exceeding max_days."""
        from backfill_manager.includes.backfill_utils import validate_date_range
        
        result = validate_date_range(
            start_date=date(2024, 1, 1),
            end_date=date(2024, 12, 31),
            max_days=30,
        )
        
        assert result["valid"] is False
        assert "exceeds" in result["error"].lower()


class TestLineageUtils:
    """Tests for lineage extraction."""
    
    def test_extract_sql_lineage_simple_select(self):
        """Should extract tables from simple SELECT."""
        from data_lineage.includes.lineage_utils import extract_sql_lineage
        
        sql = "SELECT * FROM users WHERE id = 1"
        result = extract_sql_lineage(sql)
        
        assert "users" in result["inputs"]
        assert len(result["outputs"]) == 0
    
    def test_extract_sql_lineage_with_join(self):
        """Should extract all tables from JOIN."""
        from data_lineage.includes.lineage_utils import extract_sql_lineage
        
        sql = """
            SELECT t.id, u.name
            FROM transactions t
            JOIN users u ON t.user_id = u.id
        """
        result = extract_sql_lineage(sql)
        
        assert "transactions" in result["inputs"] or "TRANSACTIONS" in result["inputs"]
        assert "users" in result["inputs"] or "USERS" in result["inputs"]
    
    def test_extract_sql_lineage_create_table(self):
        """Should extract output table from CREATE."""
        from data_lineage.includes.lineage_utils import extract_sql_lineage
        
        sql = """
            CREATE TABLE summary AS
            SELECT * FROM source_table
        """
        result = extract_sql_lineage(sql)
        
        assert "summary" in result["outputs"] or "SUMMARY" in result["outputs"]
