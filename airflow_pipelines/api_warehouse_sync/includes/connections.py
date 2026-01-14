"""
Database Connection Factories
=============================
Shared connection utilities for BigQuery.
"""

from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from google.cloud import bigquery


class BigQueryConnection:
    """Wrapper for BigQuery operations."""
    
    def __init__(self, dataset: str, project: str = None):
        self.hook = BigQueryHook()
        self.client = self.hook.get_client()
        self.dataset = dataset
        self.project = project or self.client.project
    
    def get_table_id(self, table: str) -> str:
        return f"{self.project}.{self.dataset}.{table}"
    
    def execute(self, sql: str):
        """Execute a SQL statement."""
        job = self.client.query(sql)
        return job.result()
    
    def insert(self, table: str, rows: list):
        """Insert rows to a table using streaming inserts."""
        table_ref = self.client.get_table(self.get_table_id(table))
        errors = self.client.insert_rows_json(table_ref, rows)
        if errors:
            raise Exception(f"Insert errors: {errors}")


def get_bigquery_connection(dataset: str, project: str = None) -> BigQueryConnection:
    """Factory function for BigQuery connections."""
    return BigQueryConnection(dataset, project)
