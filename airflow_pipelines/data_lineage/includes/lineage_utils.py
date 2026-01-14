"""
Data Lineage Utilities
=======================
Helper functions for OpenLineage integration and custom lineage tracking.
"""

import re
from typing import Dict, List, Any, Optional
from datetime import datetime
import json


def extract_sql_lineage(sql: str) -> Dict[str, List[str]]:
    """
    Extract table-level lineage from SQL statement.
    
    Simple regex-based extraction. For production, consider:
    - sqlglot library for proper SQL parsing
    - OpenLineage SQL parser
    
    Args:
        sql: SQL statement to analyze
        
    Returns:
        Dict with 'inputs' and 'outputs' lists
    """
    # Normalize SQL
    sql_clean = sql.upper().replace('\n', ' ')
    
    inputs = []
    outputs = []
    
    # Find FROM and JOIN tables (inputs)
    from_pattern = r'FROM\s+([a-zA-Z_][a-zA-Z0-9_\.]*)'
    join_pattern = r'JOIN\s+([a-zA-Z_][a-zA-Z0-9_\.]*)'
    
    inputs.extend(re.findall(from_pattern, sql_clean, re.IGNORECASE))
    inputs.extend(re.findall(join_pattern, sql_clean, re.IGNORECASE))
    
    # Find INSERT/CREATE tables (outputs)
    insert_pattern = r'INSERT\s+INTO\s+([a-zA-Z_][a-zA-Z0-9_\.]*)'
    create_pattern = r'CREATE\s+(?:OR\s+REPLACE\s+)?TABLE\s+([a-zA-Z_][a-zA-Z0-9_\.]*)'
    
    outputs.extend(re.findall(insert_pattern, sql_clean, re.IGNORECASE))
    outputs.extend(re.findall(create_pattern, sql_clean, re.IGNORECASE))
    
    # Clean up and dedupe
    inputs = list(set(t.lower() for t in inputs if t.lower() not in ('select', 'from', 'where')))
    outputs = list(set(t.lower() for t in outputs))
    
    return {
        "inputs": inputs,
        "outputs": outputs,
    }


def emit_lineage_event(
    job_name: str,
    inputs: List[str],
    outputs: List[str],
    column_lineage: Optional[Dict[str, List[str]]] = None,
    metadata: Optional[Dict[str, Any]] = None,
) -> None:
    """
    Emit custom OpenLineage event.
    
    For full integration, this would send to OpenLineage API.
    This implementation logs for demonstration.
    
    Args:
        job_name: Name of the job/transformation
        inputs: List of input dataset names
        outputs: List of output dataset names
        column_lineage: Optional column-level lineage mapping
        metadata: Additional metadata to include
    """
    event = {
        "eventType": "COMPLETE",
        "eventTime": datetime.utcnow().isoformat() + "Z",
        "job": {
            "namespace": "airflow",
            "name": job_name,
        },
        "inputs": [
            {"namespace": "bigquery", "name": inp}
            for inp in inputs
        ],
        "outputs": [
            {"namespace": "bigquery", "name": out}
            for out in outputs
        ],
    }
    
    if column_lineage:
        event["outputs"][0]["columnLineage"] = {
            "fields": [
                {
                    "name": col,
                    "inputFields": [
                        {"namespace": "bigquery", "name": src.split('.')[0], "field": src.split('.')[-1]}
                        for src in sources
                    ]
                }
                for col, sources in column_lineage.items()
            ]
        }
    
    if metadata:
        event["metadata"] = metadata
    
    # In production, send to OpenLineage API:
    # requests.post(os.environ['OPENLINEAGE_URL'], json=event)
    
    print(f"📊 Lineage Event: {job_name}")
    print(f"   Inputs: {inputs}")
    print(f"   Outputs: {outputs}")
    if column_lineage:
        print(f"   Column Lineage: {list(column_lineage.keys())}")


class LineageContext:
    """
    Context manager for tracking lineage within a task.
    
    Usage:
        with LineageContext(job_name="my_transform") as ctx:
            ctx.add_input("source_table")
            # ... do work ...
            ctx.add_output("target_table")
            ctx.add_column_lineage("new_col", ["source_table.old_col"])
    """
    
    def __init__(self, job_name: str):
        self.job_name = job_name
        self.inputs = []
        self.outputs = []
        self.column_lineage = {}
        self.metadata = {}
        self.start_time = None
    
    def __enter__(self):
        self.start_time = datetime.utcnow()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        # Emit lineage on exit
        self.metadata["duration_seconds"] = (
            datetime.utcnow() - self.start_time
        ).total_seconds()
        self.metadata["status"] = "failed" if exc_type else "success"
        
        emit_lineage_event(
            job_name=self.job_name,
            inputs=self.inputs,
            outputs=self.outputs,
            column_lineage=self.column_lineage if self.column_lineage else None,
            metadata=self.metadata,
        )
        
        return False  # Don't suppress exceptions
    
    def add_input(self, dataset: str) -> None:
        if dataset not in self.inputs:
            self.inputs.append(dataset)
    
    def add_output(self, dataset: str) -> None:
        if dataset not in self.outputs:
            self.outputs.append(dataset)
    
    def add_column_lineage(self, output_col: str, source_cols: List[str]) -> None:
        self.column_lineage[output_col] = source_cols
    
    def add_metadata(self, key: str, value: Any) -> None:
        self.metadata[key] = value
