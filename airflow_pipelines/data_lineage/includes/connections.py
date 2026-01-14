"""Database Connection Factories."""

from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook


class BigQueryConnection:
    def __init__(self, dataset: str, project: str = None):
        self.hook = BigQueryHook()
        self.client = self.hook.get_client()
        self.dataset = dataset
        self.project = project or self.client.project
    
    def execute(self, sql: str):
        job = self.client.query(sql)
        return job.result()
    
    def fetch(self, sql: str, one_row: bool = False):
        job = self.client.query(sql)
        results = list(job.result())
        if not results:
            return None if one_row else []
        return results[0] if one_row else results


def get_bigquery_connection(dataset: str, project: str = None) -> BigQueryConnection:
    return BigQueryConnection(dataset, project)
