"""
Slack Alerting Utilities
=========================
Rich Slack notifications with context, formatting, and escalation.
"""

from datetime import datetime
from typing import Optional, List, Dict, Any
from airflow.providers.slack.hooks.slack_webhook import SlackWebhookHook
from airflow.models import Variable, DagRun
from airflow.utils.state import State


class SlackAlertCallback:
    """
    Reusable Slack alert callback for Airflow DAGs.
    
    Features:
    - Rich message formatting
    - DAG run context (duration, task info)
    - Failure escalation (mention on-call)
    - Success summaries
    """
    
    def __init__(
        self,
        channel: str = "#data-alerts",
        webhook_conn_id: str = "slack_webhook",
        mention_on_failure: Optional[str] = None,
        include_log_link: bool = True,
    ):
        self.channel = channel
        self.webhook_conn_id = webhook_conn_id
        self.mention_on_failure = mention_on_failure
        self.include_log_link = include_log_link
    
    def on_success(self, context: Dict[str, Any]) -> None:
        """Callback for successful DAG/task completion."""
        dag_id = context.get("dag").dag_id
        run_id = context.get("run_id")
        execution_date = context.get("execution_date")
        
        # Calculate duration
        dag_run = context.get("dag_run")
        duration = self._calculate_duration(dag_run)
        
        blocks = [
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"✅ *DAG Succeeded*: `{dag_id}`"
                }
            },
            {
                "type": "context",
                "elements": [
                    {"type": "mrkdwn", "text": f"*Run ID:* {run_id}"},
                    {"type": "mrkdwn", "text": f"*Duration:* {duration}"},
                    {"type": "mrkdwn", "text": f"*Date:* {execution_date.strftime('%Y-%m-%d %H:%M')}"},
                ]
            }
        ]
        
        self._send_message(blocks, color="#28a745")
    
    def on_failure(self, context: Dict[str, Any]) -> None:
        """Callback for failed DAG/task."""
        dag_id = context.get("dag").dag_id
        task_id = context.get("task_instance").task_id if context.get("task_instance") else "N/A"
        run_id = context.get("run_id")
        exception = context.get("exception")
        log_url = context.get("task_instance").log_url if context.get("task_instance") else None
        
        # Build alert message
        mention = f"{self.mention_on_failure} " if self.mention_on_failure else ""
        
        blocks = [
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"{mention}❌ *DAG Failed*: `{dag_id}`"
                }
            },
            {
                "type": "section",
                "fields": [
                    {"type": "mrkdwn", "text": f"*Task:*\n`{task_id}`"},
                    {"type": "mrkdwn", "text": f"*Run ID:*\n{run_id}"},
                ]
            },
        ]
        
        # Add exception info
        if exception:
            error_msg = str(exception)[:500]  # Truncate long errors
            blocks.append({
                "type": "section",
                "text": {"type": "mrkdwn", "text": f"*Error:*\n```{error_msg}```"}
            })
        
        # Add log link
        if self.include_log_link and log_url:
            blocks.append({
                "type": "actions",
                "elements": [
                    {
                        "type": "button",
                        "text": {"type": "plain_text", "text": "📋 View Logs"},
                        "url": log_url,
                    }
                ]
            })
        
        self._send_message(blocks, color="#dc3545")
    
    def on_retry(self, context: Dict[str, Any]) -> None:
        """Callback for task retry."""
        dag_id = context.get("dag").dag_id
        task_id = context.get("task_instance").task_id
        try_number = context.get("task_instance").try_number
        
        blocks = [
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"🔄 *Retry Attempt #{try_number}*: `{dag_id}.{task_id}`"
                }
            }
        ]
        
        self._send_message(blocks, color="#ffc107")
    
    def _calculate_duration(self, dag_run) -> str:
        """Calculate human-readable duration."""
        if not dag_run or not dag_run.start_date:
            return "N/A"
        
        end = dag_run.end_date or datetime.now(dag_run.start_date.tzinfo)
        duration = end - dag_run.start_date
        
        total_seconds = int(duration.total_seconds())
        hours, remainder = divmod(total_seconds, 3600)
        minutes, seconds = divmod(remainder, 60)
        
        if hours > 0:
            return f"{hours}h {minutes}m {seconds}s"
        elif minutes > 0:
            return f"{minutes}m {seconds}s"
        else:
            return f"{seconds}s"
    
    def _send_message(self, blocks: List[Dict], color: str = None) -> None:
        """Send message to Slack."""
        try:
            hook = SlackWebhookHook(slack_webhook_conn_id=self.webhook_conn_id)
            
            attachments = [{"color": color, "blocks": []}] if color else None
            
            hook.send(
                blocks=blocks,
                attachments=attachments,
            )
        except Exception as e:
            print(f"⚠️ Failed to send Slack alert: {e}")


def send_slack_message(
    channel: str,
    blocks: List[Dict],
    webhook_conn_id: str = "slack_webhook",
) -> None:
    """
    Send a custom Slack message.
    
    Args:
        channel: Slack channel (for logging, webhook determines actual channel)
        blocks: Slack Block Kit blocks
        webhook_conn_id: Airflow connection ID for webhook
    """
    try:
        hook = SlackWebhookHook(slack_webhook_conn_id=webhook_conn_id)
        hook.send(blocks=blocks)
        print(f"✅ Sent Slack message to {channel}")
    except Exception as e:
        print(f"⚠️ Failed to send Slack message: {e}")
