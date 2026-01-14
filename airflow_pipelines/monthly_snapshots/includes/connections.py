"""
Database Connection Factories
=============================
Shared connection utilities for PostgreSQL and BigQuery.
"""

from typing import Optional
from collections import namedtuple
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook


class PostgresConnection:
    """Wrapper for PostgreSQL connections."""
    
    def __init__(self, connection_id: str = "postgres_default"):
        self.hook = PostgresHook(postgres_conn_id=connection_id)
        self.conn = self.hook.get_conn()
        self.cursor = self.conn.cursor()
    
    def fetch(self, sql: str, one_row: bool = False):
        """Execute SQL and return results as named tuples."""
        self.cursor.execute(sql)
        columns = [desc[0] for desc in self.cursor.description]
        Row = namedtuple("Row", columns)
        
        if one_row:
            row = self.cursor.fetchone()
            return Row(*row) if row else None
        
        return [Row(*row) for row in self.cursor.fetchall()]


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
    
    def insert_bulk(
        self, 
        rows: list, 
        schema: list, 
        table: str,
        write_disposition: str = "WRITE_APPEND",
    ):
        """Bulk insert using load job."""
        from google.cloud.bigquery import LoadJobConfig, SchemaField
        
        bq_schema = [SchemaField(name, field_type) for name, field_type in schema]
        job_config = LoadJobConfig(
            schema=bq_schema,
            write_disposition=write_disposition,
        )
        
        table_id = self.get_table_id(table)
        job = self.client.load_table_from_json(rows, table_id, job_config=job_config)
        job.result()


def get_postgres_connection(connection_id: str = "postgres_default") -> PostgresConnection:
    return PostgresConnection(connection_id)


def get_bigquery_connection(dataset: str, project: str = None) -> BigQueryConnection:
    return BigQueryConnection(dataset, project)
