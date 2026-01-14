"""
Database Connection Factories
=============================
Connection utilities for PostgreSQL and ClickHouse.
"""

from typing import Optional
from collections import namedtuple
import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook


class PostgresConnection:
    """Wrapper for PostgreSQL with DataFrame support."""
    
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
    
    def fetch_dataframe(self, sql: str) -> pd.DataFrame:
        """Execute SQL and return results as pandas DataFrame."""
        return pd.read_sql(sql, self.conn)


class ClickHouseConnection:
    """
    Wrapper for ClickHouse operations.
    
    ClickHouse is optimized for:
    - OLAP (analytical) queries
    - Time-series data
    - Real-time aggregations
    - High insert throughput
    """
    
    def __init__(self, connection_id: str = "clickhouse_default"):
        # Using clickhouse-driver or clickhouse-connect
        from clickhouse_driver import Client
        from airflow.hooks.base import BaseHook
        
        conn = BaseHook.get_connection(connection_id)
        self.client = Client(
            host=conn.host,
            port=conn.port or 9000,
            user=conn.login,
            password=conn.password,
            database=conn.schema or "default",
        )
    
    def execute(self, sql: str, params: dict = None):
        """Execute a SQL statement."""
        return self.client.execute(sql, params or {})
    
    def fetch(self, sql: str, one_row: bool = False):
        """Execute query and fetch results."""
        result = self.client.execute(sql, with_column_types=True)
        rows, columns = result
        column_names = [col[0] for col in columns]
        Row = namedtuple("Row", column_names)
        
        if not rows:
            return None if one_row else []
        
        if one_row:
            return Row(*rows[0])
        return [Row(*row) for row in rows]
    
    def insert_dataframe(self, table: str, df: pd.DataFrame):
        """
        Insert DataFrame to ClickHouse.
        
        Uses native protocol for high-performance inserts.
        """
        columns = df.columns.tolist()
        data = df.values.tolist()
        
        self.client.execute(
            f"INSERT INTO {table} ({', '.join(columns)}) VALUES",
            data,
        )


def get_postgres_connection(connection_id: str = "postgres_default") -> PostgresConnection:
    """Factory function for Postgres connections."""
    return PostgresConnection(connection_id)


def get_clickhouse_connection(connection_id: str = "clickhouse_default") -> ClickHouseConnection:
    """Factory function for ClickHouse connections."""
    return ClickHouseConnection(connection_id)
