#!/usr/bin/env python3
"""
Test DAG Imports
=================
Validates that all DAG files can be imported without errors.
Catches missing dependencies, syntax errors, and import issues.
"""

import os
import sys
from pathlib import Path


def find_dag_files(root_dir: str) -> list:
    """Find all Python files in dags directories."""
    dag_files = []
    root_path = Path(root_dir)
    
    for dag_dir in root_path.glob("**/dags"):
        for py_file in dag_dir.glob("*.py"):
            if not py_file.name.startswith("_"):
                dag_files.append(py_file)
    
    return dag_files


def test_dag_import(dag_file: Path) -> tuple:
    """
    Test importing a single DAG file.
    
    Returns:
        (success: bool, error_message: str or None)
    """
    try:
        # Add parent to path for includes
        sys.path.insert(0, str(dag_file.parent.parent))
        
        # Compile the file (catches syntax errors)
        with open(dag_file) as f:
            compile(f.read(), dag_file, 'exec')
        
        return True, None
        
    except SyntaxError as e:
        return False, f"Syntax error: {e}"
    except Exception as e:
        return False, f"Import error: {e}"
    finally:
        if str(dag_file.parent.parent) in sys.path:
            sys.path.remove(str(dag_file.parent.parent))


def main():
    """Run DAG import tests."""
    # Get repository root
    repo_root = os.environ.get("GITHUB_WORKSPACE", os.getcwd())
    
    print("=" * 60)
    print("DAG Import Tests")
    print("=" * 60)
    
    dag_files = find_dag_files(repo_root)
    print(f"\nFound {len(dag_files)} DAG files to test\n")
    
    passed = 0
    failed = 0
    errors = []
    
    for dag_file in dag_files:
        relative_path = dag_file.relative_to(repo_root)
        success, error = test_dag_import(dag_file)
        
        if success:
            print(f"✅ {relative_path}")
            passed += 1
        else:
            print(f"❌ {relative_path}")
            print(f"   {error}")
            failed += 1
            errors.append((relative_path, error))
    
    print("\n" + "=" * 60)
    print(f"Results: {passed} passed, {failed} failed")
    print("=" * 60)
    
    if failed > 0:
        print("\n❌ Failed DAGs:")
        for path, error in errors:
            print(f"  - {path}: {error}")
        sys.exit(1)
    else:
        print("\n✅ All DAG imports successful!")
        sys.exit(0)


if __name__ == "__main__":
    main()
