"""
Pipeline Alerting System
=========================
This DAG demonstrates a comprehensive alerting framework with
custom Slack and email notifications for pipeline monitoring.

Pattern includes:
- Custom success/failure callbacks
- Rich Slack messages with context
- Email fallback notifications
- DAG-level and task-level alerts
- Escalation for repeated failures

Use Case: Production monitoring, on-call alerts, SLA tracking.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.decorators import task
from airflow.operators.python import PythonOperator
from includes.slack_alerts import SlackAlertCallback
from includes.email_alerts import EmailAlertCallback


# Shared alert callbacks
slack_callback = SlackAlertCallback(
    channel="#data-alerts",
    mention_on_failure="@oncall-data",
)

email_callback = EmailAlertCallback(
    recipients=["data-team@company.com"],
    cc_on_failure=["oncall@company.com"],
)


def combined_success_callback(context):
    """Combined callback for successful runs."""
    slack_callback.on_success(context)


def combined_failure_callback(context):
    """Combined callback for failed runs with escalation."""
    slack_callback.on_failure(context)
    email_callback.on_failure(context)


with DAG(
    dag_id="pipeline_alerting_demo",
    description="Demonstrates custom alerting patterns",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["alerting", "monitoring", "demo"],
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
        "on_failure_callback": combined_failure_callback,
    },
    on_success_callback=combined_success_callback,
    on_failure_callback=combined_failure_callback,
) as dag:
    
    @task()
    def extract_data():
        """Simulate data extraction."""
        print("📥 Extracting data...")
        # Simulate work
        import time
        time.sleep(2)
        return {"rows_extracted": 10000}
    
    @task()
    def transform_data(extract_result: dict):
        """Simulate data transformation."""
        print(f"🔄 Transforming {extract_result['rows_extracted']} rows...")
        return {"rows_transformed": extract_result["rows_extracted"]}
    
    @task()
    def load_data(transform_result: dict):
        """Simulate data loading."""
        print(f"📤 Loading {transform_result['rows_transformed']} rows...")
        return {"rows_loaded": transform_result["rows_transformed"]}
    
    @task()
    def send_completion_summary(load_result: dict):
        """Send summary notification on completion."""
        from includes.slack_alerts import send_slack_message
        
        send_slack_message(
            channel="#data-alerts",
            blocks=[
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": f"✅ *Pipeline Complete*\n"
                                f"• Rows loaded: {load_result['rows_loaded']:,}\n"
                                f"• Completed at: {datetime.now().strftime('%Y-%m-%d %H:%M')}"
                    }
                }
            ]
        )
    
    # DAG Flow
    extracted = extract_data()
    transformed = transform_data(extracted)
    loaded = load_data(transformed)
    send_completion_summary(loaded)
