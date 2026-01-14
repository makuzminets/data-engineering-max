"""
Database Connection Factories
=============================
Abstraction layer for database connections with SSH tunneling support.
Follows factory pattern for consistent connection management.
"""

from typing import Optional
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from google.cloud import bigquery


class PostgresConnection:
    """Wrapper for PostgreSQL connections with convenient fetch methods."""
    
    def __init__(self, connection_id: str = "postgres_default"):
        self.hook = PostgresHook(postgres_conn_id=connection_id)
        self.conn = self.hook.get_conn()
        self.cursor = self.conn.cursor()
    
    def fetch(self, sql: str, one_row: bool = False):
        """
        Execute SQL and fetch results as named tuples.
        
        Args:
            sql: SQL query to execute
            one_row: If True, return single row instead of list
        """
        self.cursor.execute(sql)
        columns = [desc[0] for desc in self.cursor.description]
        
        from collections import namedtuple
        Row = namedtuple("Row", columns)
        
        if one_row:
            row = self.cursor.fetchone()
            return Row(*row) if row else None
        
        return [Row(*row) for row in self.cursor.fetchall()]


class BigQueryConnection:
    """Wrapper for BigQuery with insert_bulk and execute methods."""
    
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
    
    def fetch(self, sql: str, one_row: bool = False):
        """Execute query and fetch results."""
        job = self.client.query(sql)
        results = list(job.result())
        
        if not results:
            return None if one_row else []
        
        if one_row:
            return results[0]
        return results
    
    def insert(self, table: str, rows: list):
        """Insert rows to a table."""
        table_ref = self.client.get_table(self.get_table_id(table))
        errors = self.client.insert_rows_json(table_ref, rows)
        if errors:
            raise Exception(f"Insert errors: {errors}")
    
    def insert_bulk(
        self, 
        rows: list, 
        schema: list, 
        table: str,
        write_disposition: str = "WRITE_APPEND"
    ):
        """
        Bulk insert using BigQuery load job for better performance.
        
        Args:
            rows: List of dictionaries to insert
            schema: List of (name, type) tuples
            table: Target table name
            write_disposition: WRITE_APPEND or WRITE_TRUNCATE
        """
        from google.cloud.bigquery import LoadJobConfig, SchemaField
        
        bq_schema = [
            SchemaField(name, field_type) 
            for name, field_type in schema
        ]
        
        job_config = LoadJobConfig(
            schema=bq_schema,
            write_disposition=write_disposition,
        )
        
        table_id = self.get_table_id(table)
        job = self.client.load_table_from_json(rows, table_id, job_config=job_config)
        job.result()  # Wait for completion


def get_postgres_connection(connection_id: str = "postgres_default") -> PostgresConnection:
    """Factory function for Postgres connections."""
    return PostgresConnection(connection_id)


def get_bigquery_connection(dataset: str, project: str = None) -> BigQueryConnection:
    """Factory function for BigQuery connections."""
    return BigQueryConnection(dataset, project)
