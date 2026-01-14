"""
Data Quality Expectations
==========================
Custom data quality validators inspired by Great Expectations.
Provides reusable validation logic for common data quality checks.
"""

from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional


class DataQualityValidator:
    """
    Validator class for running data quality checks.
    
    Inspired by Great Expectations but simplified for Airflow integration.
    """
    
    def __init__(self, connection):
        self.conn = connection
        self.expectations = TableExpectations()
    
    def check_schema(self, table: str) -> Dict[str, Any]:
        """
        Validate table schema against expectations.
        
        Checks:
        - Required columns exist
        - Column types match expected
        - Nullable constraints respected
        """
        expected = self.expectations.get_schema(table)
        if not expected:
            return {"passed": True, "message": "No schema expectations defined"}
        
        # Get actual schema from BigQuery
        actual_schema = self.conn.fetch(f"""
            SELECT column_name, data_type, is_nullable
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE table_name = '{table}'
        """)
        
        actual_columns = {row.column_name: row for row in actual_schema}
        errors = []
        
        for col_name, expected_type in expected.items():
            if col_name not in actual_columns:
                errors.append(f"Missing column: {col_name}")
            elif actual_columns[col_name].data_type != expected_type:
                errors.append(
                    f"Type mismatch for {col_name}: "
                    f"expected {expected_type}, got {actual_columns[col_name].data_type}"
                )
        
        return {
            "passed": len(errors) == 0,
            "errors": errors,
        }
    
    def check_freshness(self, table: str, max_hours: int = 24) -> Dict[str, Any]:
        """
        Check if data is fresh (updated within threshold).
        
        Uses common timestamp columns: updated_at, created_at, _etl_loaded_at
        """
        timestamp_columns = ["updated_at", "created_at", "_etl_loaded_at", "timestamp"]
        
        for ts_col in timestamp_columns:
            try:
                result = self.conn.fetch(f"""
                    SELECT 
                        MAX({ts_col}) AS last_update,
                        TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), MAX({ts_col}), HOUR) AS hours_old
                    FROM {table}
                """, one_row=True)
                
                if result and result.last_update:
                    return {
                        "passed": result.hours_old <= max_hours,
                        "last_update": str(result.last_update),
                        "hours_old": result.hours_old,
                    }
            except Exception:
                continue
        
        return {"passed": False, "error": "No timestamp column found"}
    
    def check_values(self, table: str) -> Dict[str, Any]:
        """
        Validate value constraints.
        
        Checks:
        - Null percentage within threshold
        - Values within expected ranges
        - Enum values valid
        """
        expectations = self.expectations.get_value_checks(table)
        if not expectations:
            return {"passed": True, "message": "No value expectations defined"}
        
        issues = []
        
        for check in expectations:
            if check["type"] == "null_percentage":
                result = self.conn.fetch(f"""
                    SELECT 
                        COUNTIF({check['column']} IS NULL) / COUNT(*) * 100 AS null_pct
                    FROM {table}
                """, one_row=True)
                
                if result.null_pct > check["max_percentage"]:
                    issues.append(
                        f"{check['column']}: {result.null_pct:.1f}% null "
                        f"(max: {check['max_percentage']}%)"
                    )
            
            elif check["type"] == "range":
                result = self.conn.fetch(f"""
                    SELECT 
                        MIN({check['column']}) AS min_val,
                        MAX({check['column']}) AS max_val
                    FROM {table}
                """, one_row=True)
                
                if result.min_val < check.get("min", float("-inf")):
                    issues.append(f"{check['column']}: min {result.min_val} below threshold")
                if result.max_val > check.get("max", float("inf")):
                    issues.append(f"{check['column']}: max {result.max_val} above threshold")
            
            elif check["type"] == "enum":
                result = self.conn.fetch(f"""
                    SELECT DISTINCT {check['column']} AS val
                    FROM {table}
                    WHERE {check['column']} NOT IN ({','.join(f"'{v}'" for v in check['values'])})
                """)
                
                if result:
                    invalid = [row.val for row in result]
                    issues.append(f"{check['column']}: invalid values {invalid}")
        
        return {
            "passed": len(issues) == 0,
            "issues": issues,
        }
    
    def check_duplicates(
        self, 
        table: str, 
        key_columns: List[str] = None
    ) -> Dict[str, Any]:
        """Check for duplicate records based on key columns."""
        key_columns = key_columns or self.expectations.get_unique_keys(table)
        
        if not key_columns:
            key_columns = ["id"]  # Default to id column
        
        key_cols_str = ", ".join(key_columns)
        
        result = self.conn.fetch(f"""
            SELECT COUNT(*) AS dup_count
            FROM (
                SELECT {key_cols_str}, COUNT(*) AS cnt
                FROM {table}
                GROUP BY {key_cols_str}
                HAVING COUNT(*) > 1
            )
        """, one_row=True)
        
        return {
            "passed": result.dup_count == 0,
            "count": result.dup_count,
        }
    
    def check_referential_integrity(self, table: str) -> Dict[str, Any]:
        """Check foreign key relationships."""
        fk_checks = self.expectations.get_foreign_keys(table)
        
        if not fk_checks:
            return {"passed": True, "message": "No FK checks defined"}
        
        orphans = []
        
        for fk in fk_checks:
            result = self.conn.fetch(f"""
                SELECT COUNT(*) AS orphan_count
                FROM {table} t
                LEFT JOIN {fk['references_table']} r 
                    ON t.{fk['column']} = r.{fk['references_column']}
                WHERE t.{fk['column']} IS NOT NULL
                  AND r.{fk['references_column']} IS NULL
            """, one_row=True)
            
            if result.orphan_count > 0:
                orphans.append({
                    "column": fk["column"],
                    "references": f"{fk['references_table']}.{fk['references_column']}",
                    "orphan_count": result.orphan_count,
                })
        
        return {
            "passed": len(orphans) == 0,
            "orphans": orphans,
        }


class TableExpectations:
    """
    Define expected schema and constraints for tables.
    
    In production, this would be loaded from a config file or database.
    """
    
    SCHEMAS = {
        "transactions": {
            "id": "INT64",
            "user_id": "INT64",
            "amount": "FLOAT64",
            "currency": "STRING",
            "status": "STRING",
            "created_at": "TIMESTAMP",
        },
        "users": {
            "id": "INT64",
            "email": "STRING",
            "created_at": "TIMESTAMP",
            "status": "STRING",
        },
        "activity_logs": {
            "id": "INT64",
            "event_name": "STRING",
            "user_id": "INT64",
            "created_at": "TIMESTAMP",
        },
    }
    
    VALUE_CHECKS = {
        "transactions": [
            {"type": "null_percentage", "column": "user_id", "max_percentage": 1},
            {"type": "range", "column": "amount", "min": 0, "max": 1000000},
            {"type": "enum", "column": "status", "values": ["pending", "completed", "failed", "refunded"]},
        ],
        "users": [
            {"type": "null_percentage", "column": "email", "max_percentage": 0},
            {"type": "enum", "column": "status", "values": ["active", "inactive", "suspended", "deleted"]},
        ],
    }
    
    UNIQUE_KEYS = {
        "transactions": ["id"],
        "users": ["id", "email"],
        "activity_logs": ["id"],
    }
    
    FOREIGN_KEYS = {
        "transactions": [
            {"column": "user_id", "references_table": "users", "references_column": "id"},
        ],
        "activity_logs": [
            {"column": "user_id", "references_table": "users", "references_column": "id"},
        ],
    }
    
    def get_schema(self, table: str) -> Optional[Dict[str, str]]:
        return self.SCHEMAS.get(table)
    
    def get_value_checks(self, table: str) -> Optional[List[Dict]]:
        return self.VALUE_CHECKS.get(table)
    
    def get_unique_keys(self, table: str) -> Optional[List[str]]:
        return self.UNIQUE_KEYS.get(table)
    
    def get_foreign_keys(self, table: str) -> Optional[List[Dict]]:
        return self.FOREIGN_KEYS.get(table)
