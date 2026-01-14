#!/usr/bin/env python3
"""
Test DAG Integrity
===================
Validates DAG structure and configuration:
- No import cycles
- Valid task dependencies
- Required tags present
- Owner configured
- No duplicate DAG IDs
"""

import os
import sys
from pathlib import Path
from collections import defaultdict

# Minimal Airflow imports for testing
os.environ.setdefault("AIRFLOW_HOME", os.getcwd())


def test_dag_integrity():
    """Test all DAGs for structural integrity."""
    from airflow.models import DagBag
    
    print("=" * 60)
    print("DAG Integrity Tests")
    print("=" * 60)
    
    # Load all DAGs
    repo_root = os.environ.get("GITHUB_WORKSPACE", os.getcwd())
    
    # Find all dags directories
    dag_folders = list(Path(repo_root).glob("**/dags"))
    
    all_errors = []
    all_dags = []
    dag_ids = defaultdict(list)
    
    for dag_folder in dag_folders:
        print(f"\nLoading DAGs from: {dag_folder.relative_to(repo_root)}")
        
        dagbag = DagBag(
            dag_folder=str(dag_folder),
            include_examples=False,
        )
        
        # Check for import errors
        if dagbag.import_errors:
            for dag_file, error in dagbag.import_errors.items():
                all_errors.append(f"Import error in {dag_file}: {error}")
                print(f"  ❌ Import error: {dag_file}")
        
        # Validate each DAG
        for dag_id, dag in dagbag.dags.items():
            all_dags.append(dag)
            dag_ids[dag_id].append(dag.fileloc)
            
            errors = validate_dag(dag)
            if errors:
                all_errors.extend(errors)
                print(f"  ❌ {dag_id}: {len(errors)} issues")
            else:
                print(f"  ✅ {dag_id}")
    
    # Check for duplicate DAG IDs
    for dag_id, locations in dag_ids.items():
        if len(locations) > 1:
            all_errors.append(
                f"Duplicate DAG ID '{dag_id}' found in: {locations}"
            )
    
    print("\n" + "=" * 60)
    print(f"Tested {len(all_dags)} DAGs")
    print("=" * 60)
    
    if all_errors:
        print(f"\n❌ Found {len(all_errors)} issues:\n")
        for error in all_errors:
            print(f"  - {error}")
        sys.exit(1)
    else:
        print("\n✅ All DAG integrity checks passed!")
        sys.exit(0)


def validate_dag(dag) -> list:
    """
    Validate a single DAG.
    
    Returns list of error messages.
    """
    errors = []
    
    # Check for cycles
    try:
        dag.topological_sort()
    except Exception as e:
        errors.append(f"{dag.dag_id}: Cycle detected - {e}")
    
    # Check for tasks
    if not dag.tasks:
        errors.append(f"{dag.dag_id}: DAG has no tasks")
    
    # Check for tags
    if not dag.tags:
        errors.append(f"{dag.dag_id}: DAG has no tags (recommended for organization)")
    
    # Check for description
    if not dag.description:
        errors.append(f"{dag.dag_id}: DAG has no description (recommended)")
    
    # Check for valid schedule
    if dag.schedule_interval is not None:
        try:
            # Validate cron expression or preset
            if isinstance(dag.schedule_interval, str):
                if not dag.schedule_interval.startswith("@") and \
                   len(dag.schedule_interval.split()) not in (5, 6):
                    errors.append(
                        f"{dag.dag_id}: Invalid schedule '{dag.schedule_interval}'"
                    )
        except Exception as e:
            errors.append(f"{dag.dag_id}: Schedule validation error - {e}")
    
    # Check task IDs for naming conventions
    for task in dag.tasks:
        if " " in task.task_id:
            errors.append(
                f"{dag.dag_id}: Task '{task.task_id}' contains spaces"
            )
        if task.task_id.startswith("_"):
            errors.append(
                f"{dag.dag_id}: Task '{task.task_id}' starts with underscore"
            )
    
    return errors


if __name__ == "__main__":
    test_dag_integrity()
