"""
Email Alerting Utilities
=========================
Email notifications for pipeline failures with rich HTML templates.
"""

from datetime import datetime
from typing import Optional, List, Dict, Any
from airflow.utils.email import send_email


class EmailAlertCallback:
    """
    Email alert callback for Airflow DAGs.
    
    Features:
    - HTML email templates
    - CC escalation for failures
    - Detailed error context
    """
    
    def __init__(
        self,
        recipients: List[str],
        cc_on_failure: Optional[List[str]] = None,
        subject_prefix: str = "[Airflow]",
    ):
        self.recipients = recipients
        self.cc_on_failure = cc_on_failure or []
        self.subject_prefix = subject_prefix
    
    def on_failure(self, context: Dict[str, Any]) -> None:
        """Send email alert on failure."""
        dag_id = context.get("dag").dag_id
        task_id = context.get("task_instance").task_id if context.get("task_instance") else "N/A"
        run_id = context.get("run_id")
        execution_date = context.get("execution_date")
        exception = context.get("exception")
        log_url = context.get("task_instance").log_url if context.get("task_instance") else "#"
        
        subject = f"{self.subject_prefix} ❌ DAG Failed: {dag_id}"
        
        html_content = f"""
        <html>
        <head>
            <style>
                body {{ font-family: Arial, sans-serif; margin: 20px; }}
                .header {{ background: #dc3545; color: white; padding: 15px; border-radius: 5px; }}
                .content {{ padding: 20px; background: #f8f9fa; border-radius: 5px; margin-top: 10px; }}
                .field {{ margin: 10px 0; }}
                .label {{ font-weight: bold; color: #495057; }}
                .error {{ background: #fff3cd; padding: 10px; border-radius: 3px; font-family: monospace; }}
                .button {{ display: inline-block; padding: 10px 20px; background: #007bff; color: white; 
                          text-decoration: none; border-radius: 5px; margin-top: 15px; }}
            </style>
        </head>
        <body>
            <div class="header">
                <h2>❌ Pipeline Failure Alert</h2>
            </div>
            <div class="content">
                <div class="field">
                    <span class="label">DAG:</span> {dag_id}
                </div>
                <div class="field">
                    <span class="label">Task:</span> {task_id}
                </div>
                <div class="field">
                    <span class="label">Run ID:</span> {run_id}
                </div>
                <div class="field">
                    <span class="label">Execution Date:</span> {execution_date}
                </div>
                <div class="field">
                    <span class="label">Error:</span>
                    <div class="error">{str(exception)[:1000] if exception else 'No exception details'}</div>
                </div>
                <a href="{log_url}" class="button">View Logs</a>
            </div>
            <p style="color: #6c757d; font-size: 12px; margin-top: 20px;">
                This is an automated alert from Apache Airflow.
            </p>
        </body>
        </html>
        """
        
        try:
            send_email(
                to=self.recipients,
                cc=self.cc_on_failure,
                subject=subject,
                html_content=html_content,
            )
            print(f"✅ Email alert sent to {self.recipients}")
        except Exception as e:
            print(f"⚠️ Failed to send email alert: {e}")
    
    def on_success(self, context: Dict[str, Any]) -> None:
        """Optional success notification (disabled by default for noise reduction)."""
        pass  # Override if needed
    
    def on_sla_miss(
        self,
        dag_id: str,
        task_id: str,
        expected_duration: str,
        actual_duration: str,
    ) -> None:
        """Send SLA miss notification."""
        subject = f"{self.subject_prefix} ⏰ SLA Miss: {dag_id}"
        
        html_content = f"""
        <html>
        <body style="font-family: Arial, sans-serif;">
            <h2 style="color: #ffc107;">⏰ SLA Miss Alert</h2>
            <p><strong>DAG:</strong> {dag_id}</p>
            <p><strong>Task:</strong> {task_id}</p>
            <p><strong>Expected Duration:</strong> {expected_duration}</p>
            <p><strong>Actual Duration:</strong> {actual_duration}</p>
        </body>
        </html>
        """
        
        try:
            send_email(
                to=self.recipients,
                subject=subject,
                html_content=html_content,
            )
        except Exception as e:
            print(f"⚠️ Failed to send SLA miss email: {e}")
