"""
API to Data Warehouse Sync Pipeline
====================================
This DAG demonstrates syncing data from an external REST API to BigQuery.
Pattern includes:
- Paginated API fetching with cursor/page handling
- Currency conversion for multi-currency data
- Atomic table swap (tmp table pattern)
- Data enrichment with calculated fields

Use Case: Syncing affiliate partners, CRM data, or any paginated API source.
"""

from datetime import datetime
import re
from airflow import DAG
from airflow.decorators import task
from includes.connections import get_bigquery_connection
from includes.api_client import AffiliateAPIClient
from includes.currency import CurrencyExchange

# Configuration
TARGET_DATASET = "affiliate"
TARGET_TABLE = "partners"
TARGET_TABLE_TMP = "partners_tmp"


def clean_number(value) -> int | None:
    """Clean number values by removing formatting (commas, currency symbols)."""
    if value is None:
        return None
    if isinstance(value, str):
        cleaned = re.sub(r'[^\d.]', '', value)
        try:
            return int(float(cleaned))
        except (ValueError, TypeError):
            return None
    elif isinstance(value, (int, float)):
        return int(value)
    return None


def convert_to_usd(amount_data: dict | int | float | None) -> float:
    """
    Convert any currency amount to USD.
    
    Handles formats:
    - Integer/float: Already in USD
    - Dict: {"EUR": 100.50} -> converts to USD
    """
    if amount_data is None or amount_data == []:
        return 0.0
    
    if isinstance(amount_data, (int, float)):
        return round(float(amount_data), 2)
    
    if isinstance(amount_data, dict):
        currency, value = next(iter(amount_data.items()))
        value = round(float(value), 2)
        
        if currency.upper() != 'USD':
            exchange = CurrencyExchange()
            value = exchange.convert(value, currency, 'USD')
        
        return value
    
    return 0.0


with DAG(
    dag_id="api_affiliate_partners_sync",
    description="Sync affiliate partners from external API to BigQuery",
    start_date=datetime(2024, 1, 1),
    schedule_interval='@weekly',
    catchup=False,
    tags=["api", "affiliate", "bigquery"],
) as dag:
    
    @task()
    def sync_partners_to_bigquery():
        """
        Main sync task: Fetch all partners from API and load to BigQuery.
        
        Uses atomic table swap pattern:
        1. Create/truncate temporary table
        2. Load all data to temp table
        3. Rename temp -> production in single transaction
        """
        bq_conn = get_bigquery_connection(dataset=TARGET_DATASET)
        api_client = AffiliateAPIClient()
        
        # Ensure tables exist
        create_table_sql = f"""
            CREATE TABLE IF NOT EXISTS {TARGET_TABLE} (
                extraction_date TIMESTAMP,
                partner_id STRING,
                name STRING,
                email STRING,
                status STRING,
                joining_date TIMESTAMP,
                country STRING,
                website STRING,
                signups INT64,
                customers INT64,
                revenue_usd FLOAT64,
                commissions_paid_usd FLOAT64,
                commissions_pending_usd FLOAT64,
                conversion_rate FLOAT64
            )
        """
        bq_conn.execute(create_table_sql)
        bq_conn.execute(create_table_sql.replace(TARGET_TABLE, TARGET_TABLE_TMP))
        bq_conn.execute(f"TRUNCATE TABLE {TARGET_TABLE_TMP}")
        
        print("📊 Starting partner sync from API...")
        
        processed_ids = set()  # Track duplicates across pages
        total_inserted = 0
        
        # Paginate through all partners
        while True:
            partners = api_client.get_partners()
            
            if not partners:
                print("✅ All partners fetched!")
                break
            
            print(f"📥 Fetched {len(partners)} partners (page {api_client.current_page})")
            
            rows_to_insert = []
            for partner in partners:
                partner_id = partner.get('id')
                
                # Skip duplicates (can occur with pagination)
                if partner_id in processed_ids:
                    continue
                processed_ids.add(partner_id)
                
                # Fetch detailed partner data
                details = api_client.get_partner_details(partner_id)
                
                # Transform and enrich
                signups = clean_number(details.get('referrals', {}).get('signups', 0))
                customers = clean_number(details.get('referrals', {}).get('customers', 0))
                revenue = convert_to_usd(details.get('referrals', {}).get('revenue'))
                
                # Calculate conversion rate
                conversion_rate = 0.0
                if signups and signups > 0:
                    conversion_rate = round((customers or 0) / signups * 100, 2)
                
                rows_to_insert.append({
                    'extraction_date': datetime.now().isoformat(),
                    'partner_id': str(partner_id),
                    'name': f"{partner.get('name', '')} {partner.get('surname', '')}".strip(),
                    'email': partner.get('email', ''),
                    'status': partner.get('status', ''),
                    'joining_date': partner.get('created_at', ''),
                    'country': partner.get('custom_fields', {}).get('country', ''),
                    'website': partner.get('custom_fields', {}).get('website', ''),
                    'signups': signups,
                    'customers': customers,
                    'revenue_usd': revenue,
                    'commissions_paid_usd': convert_to_usd(details.get('commissions', {}).get('paid')),
                    'commissions_pending_usd': convert_to_usd(details.get('commissions', {}).get('pending')),
                    'conversion_rate': conversion_rate,
                })
            
            # Insert batch to temp table
            if rows_to_insert:
                bq_conn.insert(table=TARGET_TABLE_TMP, rows=rows_to_insert)
                total_inserted += len(rows_to_insert)
                print(f"   📤 Inserted {len(rows_to_insert)} partners to temp table")
        
        # Atomic table swap
        print("🔄 Performing atomic table swap...")
        swap_sql = f"""
            BEGIN TRANSACTION;
            DROP TABLE IF EXISTS {TARGET_TABLE}_old;
            ALTER TABLE {TARGET_TABLE} RENAME TO {TARGET_TABLE.split('.')[-1]}_old;
            ALTER TABLE {TARGET_TABLE_TMP} RENAME TO {TARGET_TABLE.split('.')[-1]};
            DROP TABLE IF EXISTS {TARGET_TABLE}_old;
            COMMIT;
        """
        bq_conn.execute(swap_sql)
        
        print(f"✅ Sync complete! Total partners: {total_inserted}")
    
    sync_partners_to_bigquery()
