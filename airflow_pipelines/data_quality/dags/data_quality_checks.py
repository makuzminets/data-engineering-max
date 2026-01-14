"""
Data Quality Checks Pipeline
=============================
This DAG demonstrates data quality validation using Great Expectations
integrated with Airflow for automated data validation.

Pattern includes:
- Schema validation (column types, nullability)
- Value validation (ranges, enums, uniqueness)
- Freshness checks (data recency)
- Cross-table referential integrity

Use Case: Production data quality monitoring, catching data issues
before they impact downstream analytics and ML models.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.decorators import task
from airflow.operators.python import BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from includes.expectations import DataQualityValidator
from includes.connections import get_bigquery_connection
from includes.alerting import send_quality_alert

# Configuration
DATASET = "analytics"
TABLES_TO_CHECK = [
    {
        "table": "transactions",
        "checks": ["schema", "freshness", "values", "duplicates"],
        "freshness_hours": 24,
    },
    {
        "table": "users",
        "checks": ["schema", "values", "referential"],
        "freshness_hours": 48,
    },
    {
        "table": "activity_logs",
        "checks": ["schema", "freshness"],
        "freshness_hours": 6,
    },
]


with DAG(
    dag_id="data_quality_checks",
    description="Automated data quality validation with Great Expectations",
    start_date=datetime(2024, 1, 1),
    schedule="0 6 * * *",  # Daily at 6 AM
    catchup=False,
    tags=["data-quality", "monitoring", "validation"],
    default_args={
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
) as dag:
    
    @task()
    def run_quality_checks() -> dict:
        """
        Execute all data quality checks and return results.
        
        Returns dict with:
        - passed: List of passed checks
        - failed: List of failed checks with details
        - warnings: List of warning-level issues
        """
        bq_conn = get_bigquery_connection(dataset=DATASET)
        validator = DataQualityValidator(bq_conn)
        
        results = {
            "passed": [],
            "failed": [],
            "warnings": [],
            "summary": {},
        }
        
        for table_config in TABLES_TO_CHECK:
            table = table_config["table"]
            checks = table_config["checks"]
            
            print(f"\n📊 Validating: {table}")
            
            # Schema Validation
            if "schema" in checks:
                schema_result = validator.check_schema(table)
                if schema_result["passed"]:
                    results["passed"].append(f"{table}: schema")
                    print(f"   ✅ Schema valid")
                else:
                    results["failed"].append({
                        "table": table,
                        "check": "schema",
                        "details": schema_result["errors"],
                    })
                    print(f"   ❌ Schema issues: {schema_result['errors']}")
            
            # Freshness Check
            if "freshness" in checks:
                max_hours = table_config.get("freshness_hours", 24)
                freshness_result = validator.check_freshness(table, max_hours)
                if freshness_result["passed"]:
                    results["passed"].append(f"{table}: freshness")
                    print(f"   ✅ Data fresh (last update: {freshness_result['last_update']})")
                else:
                    results["failed"].append({
                        "table": table,
                        "check": "freshness",
                        "details": f"Stale data: {freshness_result['hours_old']}h old",
                    })
                    print(f"   ❌ Stale data: {freshness_result['hours_old']}h old")
            
            # Value Validation
            if "values" in checks:
                value_result = validator.check_values(table)
                if value_result["passed"]:
                    results["passed"].append(f"{table}: values")
                    print(f"   ✅ Values valid")
                else:
                    results["warnings"].append({
                        "table": table,
                        "check": "values",
                        "details": value_result["issues"],
                    })
                    print(f"   ⚠️ Value issues: {value_result['issues']}")
            
            # Duplicate Check
            if "duplicates" in checks:
                dup_result = validator.check_duplicates(table)
                if dup_result["passed"]:
                    results["passed"].append(f"{table}: duplicates")
                    print(f"   ✅ No duplicates")
                else:
                    results["failed"].append({
                        "table": table,
                        "check": "duplicates",
                        "details": f"{dup_result['count']} duplicates found",
                    })
                    print(f"   ❌ {dup_result['count']} duplicates found")
            
            # Referential Integrity
            if "referential" in checks:
                ref_result = validator.check_referential_integrity(table)
                if ref_result["passed"]:
                    results["passed"].append(f"{table}: referential")
                    print(f"   ✅ Referential integrity OK")
                else:
                    results["failed"].append({
                        "table": table,
                        "check": "referential",
                        "details": ref_result["orphans"],
                    })
                    print(f"   ❌ Orphan records found")
        
        # Summary
        results["summary"] = {
            "total_checks": len(results["passed"]) + len(results["failed"]) + len(results["warnings"]),
            "passed": len(results["passed"]),
            "failed": len(results["failed"]),
            "warnings": len(results["warnings"]),
            "success_rate": round(
                len(results["passed"]) / 
                (len(results["passed"]) + len(results["failed"]) + 0.001) * 100, 
                2
            ),
        }
        
        print(f"\n📈 Summary: {results['summary']}")
        
        return results
    
    @task()
    def evaluate_results(results: dict) -> str:
        """Decide next action based on check results."""
        if results["failed"]:
            return "alert_failures"
        elif results["warnings"]:
            return "alert_warnings"
        else:
            return "all_passed"
    
    @task()
    def alert_failures(results: dict):
        """Send critical alert for failed checks."""
        send_quality_alert(
            level="critical",
            title="❌ Data Quality Checks FAILED",
            failed_checks=results["failed"],
            summary=results["summary"],
        )
    
    @task()
    def alert_warnings(results: dict):
        """Send warning alert for non-critical issues."""
        send_quality_alert(
            level="warning",
            title="⚠️ Data Quality Warnings",
            warnings=results["warnings"],
            summary=results["summary"],
        )
    
    @task()
    def log_success(results: dict):
        """Log successful validation."""
        print(f"✅ All {results['summary']['passed']} checks passed!")
    
    # DAG Flow
    results = run_quality_checks()
    decision = evaluate_results(results)
    
    # Branching based on results
    alert_failures(results)
    alert_warnings(results)
    log_success(results)
