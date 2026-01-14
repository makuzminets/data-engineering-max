"""
Managed Backfill Pipeline
===========================
This DAG demonstrates controlled backfill operations with
parameters, date range validation, and progress tracking.

Pattern includes:
- Parameterized date ranges via DAG params
- Dry-run mode for testing
- Progress tracking with XCom
- Chunked backfill for large date ranges
- Notification on completion

Use Case: Historical data reprocessing, data corrections,
migrating to new schema, filling gaps in data warehouse.
"""

from datetime import datetime, timedelta, date
from airflow import DAG
from airflow.decorators import task
from airflow.models.param import Param
from includes.connections import get_bigquery_connection
from includes.backfill_utils import BackfillTracker, validate_date_range

# Configuration
DATASET = "analytics"
MAX_DAYS_PER_RUN = 30  # Safety limit


with DAG(
    dag_id="managed_backfill",
    description="Controlled backfill with parameters and progress tracking",
    start_date=datetime(2024, 1, 1),
    schedule=None,  # Manual trigger only
    catchup=False,
    tags=["backfill", "utility", "manual"],
    params={
        "start_date": Param(
            default="2024-01-01",
            type="string",
            description="Start date (YYYY-MM-DD)",
        ),
        "end_date": Param(
            default="2024-01-31",
            type="string",
            description="End date (YYYY-MM-DD)",
        ),
        "target_table": Param(
            default="transactions",
            type="string",
            enum=["transactions", "users", "activity_logs"],
            description="Table to backfill",
        ),
        "dry_run": Param(
            default=True,
            type="boolean",
            description="If true, only simulate without writing",
        ),
        "chunk_size_days": Param(
            default=7,
            type="integer",
            description="Days to process per chunk",
        ),
    },
    render_template_as_native_obj=True,
) as dag:
    
    @task()
    def validate_params(**context) -> dict:
        """
        Validate backfill parameters.
        
        Checks:
        - Date format is valid
        - Start date < end date
        - Date range not too large
        - Table exists
        """
        params = context["params"]
        
        start_str = params["start_date"]
        end_str = params["end_date"]
        target_table = params["target_table"]
        dry_run = params["dry_run"]
        
        print(f"📋 Backfill Parameters:")
        print(f"   Start: {start_str}")
        print(f"   End: {end_str}")
        print(f"   Table: {target_table}")
        print(f"   Dry Run: {dry_run}")
        
        # Parse dates
        try:
            start_date = datetime.strptime(start_str, "%Y-%m-%d").date()
            end_date = datetime.strptime(end_str, "%Y-%m-%d").date()
        except ValueError as e:
            raise ValueError(f"Invalid date format: {e}")
        
        # Validate range
        validation = validate_date_range(start_date, end_date, MAX_DAYS_PER_RUN)
        if not validation["valid"]:
            raise ValueError(validation["error"])
        
        days_to_process = (end_date - start_date).days + 1
        
        return {
            "start_date": start_str,
            "end_date": end_str,
            "target_table": target_table,
            "dry_run": dry_run,
            "chunk_size_days": params["chunk_size_days"],
            "total_days": days_to_process,
        }
    
    @task()
    def generate_date_chunks(config: dict) -> list:
        """
        Split date range into manageable chunks.
        
        Returns list of (chunk_start, chunk_end) tuples.
        """
        start = datetime.strptime(config["start_date"], "%Y-%m-%d").date()
        end = datetime.strptime(config["end_date"], "%Y-%m-%d").date()
        chunk_size = config["chunk_size_days"]
        
        chunks = []
        current = start
        
        while current <= end:
            chunk_end = min(current + timedelta(days=chunk_size - 1), end)
            chunks.append({
                "start": current.isoformat(),
                "end": chunk_end.isoformat(),
                "index": len(chunks) + 1,
            })
            current = chunk_end + timedelta(days=1)
        
        print(f"📊 Generated {len(chunks)} chunks:")
        for chunk in chunks:
            print(f"   Chunk {chunk['index']}: {chunk['start']} to {chunk['end']}")
        
        return chunks
    
    @task()
    def process_backfill_chunk(chunk: dict, config: dict) -> dict:
        """
        Process a single backfill chunk.
        
        In production, this would:
        1. Delete existing data for date range
        2. Re-extract from source
        3. Transform and load
        """
        target_table = config["target_table"]
        dry_run = config["dry_run"]
        
        print(f"\n🔄 Processing Chunk {chunk['index']}: {chunk['start']} to {chunk['end']}")
        
        if dry_run:
            print(f"   🔍 DRY RUN - Would process {target_table} for dates {chunk['start']} to {chunk['end']}")
            # Simulate work
            rows_processed = 1000 * (chunk["index"])  # Fake number
        else:
            # Real backfill logic
            bq_conn = get_bigquery_connection(dataset=DATASET)
            
            # 1. Delete existing data
            delete_sql = f"""
                DELETE FROM {target_table}
                WHERE DATE(created_at) BETWEEN '{chunk['start']}' AND '{chunk['end']}'
            """
            print(f"   🗑️ Deleting existing data...")
            bq_conn.execute(delete_sql)
            
            # 2. Re-insert from source (simplified example)
            # In production, this would call the actual ETL logic
            insert_sql = f"""
                INSERT INTO {target_table}
                SELECT * FROM source_{target_table}
                WHERE DATE(created_at) BETWEEN '{chunk['start']}' AND '{chunk['end']}'
            """
            print(f"   📥 Re-loading data...")
            result = bq_conn.execute(insert_sql)
            rows_processed = result.num_dml_affected_rows if hasattr(result, 'num_dml_affected_rows') else 0
        
        return {
            "chunk_index": chunk["index"],
            "start": chunk["start"],
            "end": chunk["end"],
            "rows_processed": rows_processed,
            "status": "simulated" if dry_run else "completed",
        }
    
    @task()
    def summarize_backfill(chunk_results: list, config: dict) -> dict:
        """Generate backfill summary report."""
        total_rows = sum(r["rows_processed"] for r in chunk_results)
        
        summary = {
            "table": config["target_table"],
            "date_range": f"{config['start_date']} to {config['end_date']}",
            "total_days": config["total_days"],
            "chunks_processed": len(chunk_results),
            "total_rows": total_rows,
            "dry_run": config["dry_run"],
            "status": "simulated" if config["dry_run"] else "completed",
        }
        
        print(f"\n📊 Backfill Summary:")
        print(f"   Table: {summary['table']}")
        print(f"   Date Range: {summary['date_range']}")
        print(f"   Days: {summary['total_days']}")
        print(f"   Chunks: {summary['chunks_processed']}")
        print(f"   Rows: {summary['total_rows']:,}")
        print(f"   Status: {summary['status']}")
        
        if config["dry_run"]:
            print(f"\n⚠️ This was a DRY RUN. Set dry_run=false to execute.")
        else:
            print(f"\n✅ Backfill completed successfully!")
        
        return summary
    
    # DAG Flow
    config = validate_params()
    chunks = generate_date_chunks(config)
    
    # Process chunks sequentially (could be parallelized with expand)
    chunk_results = process_backfill_chunk.expand(chunk=chunks, config=[config] * 100)
    summarize_backfill(chunk_results, config)
