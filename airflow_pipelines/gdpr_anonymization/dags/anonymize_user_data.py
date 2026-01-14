"""
GDPR Data Anonymization Pipeline
=================================
This DAG demonstrates GDPR-compliant data anonymization for users
who have requested account deletion ("right to be forgotten").

Pattern includes:
- Identifying accounts marked for deletion
- Generating consistent fake data with Faker
- Batch updates across multiple datasets
- Audit trail preservation

Use Case: GDPR compliance, data privacy, user data deletion requests.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from faker import Faker
from includes.connections import get_bigquery_connection

# Configuration
MAIN_DATASET = "analytics"
ACCOUNTS_TABLE = "accounts"

# Datasets containing user PII that need anonymization
DATASETS_TO_ANONYMIZE = [
    {
        "dataset": "payments",
        "table": "customers",
        "display_name": "Payment Customers",
        "pii_fields": ["name", "email", "phone", "address"],
    },
    {
        "dataset": "crm",
        "table": "contacts",
        "display_name": "CRM Contacts",
        "pii_fields": ["name", "email", "phone"],
    },
    {
        "dataset": "support",
        "table": "tickets",
        "display_name": "Support Tickets",
        "pii_fields": ["customer_name", "customer_email"],
    },
]


def anonymize_user_data(**kwargs):
    """
    Main anonymization function.
    
    Algorithm:
    1. Find all accounts with status='forgotten' (deletion requested)
    2. Generate consistent fake identifiers using Faker
    3. Update PII fields across all configured datasets
    4. Use temp table + UPDATE JOIN for efficient batch updates
    """
    print("🔐 Starting GDPR anonymization process...")
    
    main_conn = get_bigquery_connection(MAIN_DATASET)
    faker = Faker()
    faker.seed_instance(42)  # Reproducible for testing
    
    # Find accounts pending deletion
    forgotten_query = f"""
        SELECT id AS account_id, email
        FROM {ACCOUNTS_TABLE}
        WHERE status = 'forgotten'
          AND anonymized_at IS NULL
    """
    
    print("🔍 Searching for accounts pending anonymization...")
    accounts = main_conn.fetch(forgotten_query)
    
    if not accounts:
        print("✅ No accounts require anonymization")
        return
    
    print(f"🎯 Found {len(accounts)} accounts to anonymize")
    
    # Generate fake data mapping
    # Using consistent seed per account_id for reproducibility
    fake_data = {}
    for account in accounts:
        Faker.seed(hash(str(account.account_id)) % (10**9))
        fake_data[account.account_id] = {
            "fake_name": f"ANON_{faker.lexify('????????').upper()}",
            "fake_email": f"deleted_{faker.uuid4()[:8]}@anonymized.local",
            "fake_phone": "000-000-0000",
            "fake_address": "REDACTED",
        }
    
    # Process each dataset
    for config in DATASETS_TO_ANONYMIZE:
        anonymize_dataset(
            config=config,
            account_ids=list(fake_data.keys()),
            fake_data=fake_data,
        )
    
    # Mark accounts as anonymized
    account_ids_str = ", ".join(str(aid) for aid in fake_data.keys())
    main_conn.execute(f"""
        UPDATE {ACCOUNTS_TABLE}
        SET anonymized_at = CURRENT_TIMESTAMP(),
            email = 'deleted@anonymized.local',
            name = 'DELETED USER'
        WHERE id IN ({account_ids_str})
    """)
    
    print(f"🎉 Anonymization complete! Processed {len(fake_data)} accounts")


def anonymize_dataset(config: dict, account_ids: list, fake_data: dict):
    """
    Anonymize PII in a specific dataset/table.
    
    Uses temp table + UPDATE JOIN pattern for efficient batch updates:
    1. Create temp table with account_id -> fake_value mapping
    2. UPDATE target table using JOIN
    3. Drop temp table
    """
    dataset = config["dataset"]
    table = config["table"]
    display_name = config["display_name"]
    pii_fields = config["pii_fields"]
    
    print(f"\n📊 Processing: {display_name}")
    
    conn = get_bigquery_connection(dataset)
    
    # Create temp table with fake data mappings
    temp_table = "temp_anonymization_mapping"
    
    conn.execute(f"DROP TABLE IF EXISTS {temp_table}")
    conn.execute(f"""
        CREATE TABLE {temp_table} (
            account_id INT64,
            fake_name STRING,
            fake_email STRING,
            fake_phone STRING,
            fake_address STRING
        )
    """)
    
    # Insert fake data mappings
    rows_to_insert = [
        {
            "account_id": account_id,
            **fake_data[account_id],
        }
        for account_id in account_ids
    ]
    
    conn.insert(table=temp_table, rows=rows_to_insert)
    print(f"   📝 Created mapping table with {len(rows_to_insert)} entries")
    
    # Build UPDATE statement based on PII fields
    set_clauses = []
    for field in pii_fields:
        if "name" in field.lower():
            set_clauses.append(f"t.{field} = m.fake_name")
        elif "email" in field.lower():
            set_clauses.append(f"t.{field} = m.fake_email")
        elif "phone" in field.lower():
            set_clauses.append(f"t.{field} = m.fake_phone")
        elif "address" in field.lower():
            set_clauses.append(f"t.{field} = m.fake_address")
    
    set_clause = ",\n            ".join(set_clauses)
    
    # Execute UPDATE with JOIN
    update_query = f"""
        UPDATE {table} AS t
        SET {set_clause}
        FROM {temp_table} AS m
        WHERE t.account_id = m.account_id
          AND t.{pii_fields[0]} NOT LIKE 'ANON_%'
          AND t.{pii_fields[0]} NOT LIKE 'deleted_%'
    """
    
    print(f"   🔄 Executing anonymization update...")
    conn.execute(update_query)
    
    # Cleanup
    conn.execute(f"DROP TABLE {temp_table}")
    print(f"   ✅ Anonymized PII in {display_name}")


# DAG Definition
dag = DAG(
    dag_id="gdpr_anonymize_user_data",
    description="Anonymize PII for users who requested account deletion (GDPR compliance)",
    schedule="30 3 * * *",  # Daily at 3:30 AM UTC
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=["gdpr", "privacy", "anonymization", "compliance"],
    default_args={
        "retries": 3,
        "retry_delay": timedelta(minutes=5),
    },
)

PythonOperator(
    task_id="anonymize_user_data",
    python_callable=anonymize_user_data,
    dag=dag,
)
