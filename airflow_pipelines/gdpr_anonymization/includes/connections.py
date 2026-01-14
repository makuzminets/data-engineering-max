"""
Database Connection Factories
=============================
BigQuery connection wrapper for GDPR anonymization pipeline.
"""

from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook


class BigQueryConnection:
    """Wrapper for BigQuery operations with GDPR-focused methods."""
    
    def __init__(self, dataset: str, project: str = None):
        self.hook = BigQueryHook()
        self.client = self.hook.get_client()
        self.dataset = dataset
        self.project = project or self.client.project
    
    def get_table_id(self, table: str) -> str:
        return f"{self.project}.{self.dataset}.{table}"
    
    def execute(self, sql: str):
        """Execute a SQL statement (UPDATE, DROP, CREATE, etc.)."""
        job = self.client.query(sql)
        result = job.result()
        # Return number of affected rows for UPDATE statements
        return job.num_dml_affected_rows if hasattr(job, 'num_dml_affected_rows') else None
    
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
        """Insert rows using streaming inserts."""
        table_ref = self.client.get_table(self.get_table_id(table))
        errors = self.client.insert_rows_json(table_ref, rows)
        if errors:
            raise Exception(f"Insert errors: {errors}")


def get_bigquery_connection(dataset: str, project: str = None) -> BigQueryConnection:
    """Factory function for BigQuery connections."""
    return BigQueryConnection(dataset, project)
