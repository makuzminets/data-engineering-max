"""Alerting utilities for data quality notifications."""

from typing import Dict, List, Any, Optional
from airflow.providers.slack.hooks.slack_webhook import SlackWebhookHook
from airflow.models import Variable


def send_quality_alert(
    level: str,
    title: str,
    summary: Dict[str, Any],
    failed_checks: Optional[List[Dict]] = None,
    warnings: Optional[List[Dict]] = None,
):
    """
    Send data quality alert to Slack.
    
    Args:
        level: 'critical', 'warning', or 'info'
        title: Alert title
        summary: Summary statistics
        failed_checks: List of failed check details
        warnings: List of warning details
    """
    color = {
        "critical": "#dc3545",  # Red
        "warning": "#ffc107",   # Yellow
        "info": "#17a2b8",      # Blue
    }.get(level, "#6c757d")
    
    # Build message blocks
    blocks = [
        {
            "type": "header",
            "text": {"type": "plain_text", "text": title}
        },
        {
            "type": "section",
            "fields": [
                {"type": "mrkdwn", "text": f"*Total Checks:* {summary.get('total_checks', 0)}"},
                {"type": "mrkdwn", "text": f"*Passed:* {summary.get('passed', 0)}"},
                {"type": "mrkdwn", "text": f"*Failed:* {summary.get('failed', 0)}"},
                {"type": "mrkdwn", "text": f"*Success Rate:* {summary.get('success_rate', 0)}%"},
            ]
        },
    ]
    
    # Add failed checks details
    if failed_checks:
        failed_text = "\n".join([
            f"• *{fc['table']}* - {fc['check']}: {fc['details']}"
            for fc in failed_checks[:5]  # Limit to 5
        ])
        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn", "text": f"*Failed Checks:*\n{failed_text}"}
        })
    
    # Add warnings
    if warnings:
        warn_text = "\n".join([
            f"• *{w['table']}* - {w['check']}: {w['details']}"
            for w in warnings[:5]
        ])
        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn", "text": f"*Warnings:*\n{warn_text}"}
        })
    
    # Send to Slack
    try:
        slack_hook = SlackWebhookHook(
            slack_webhook_conn_id="slack_data_quality"
        )
        slack_hook.send(
            blocks=blocks,
            attachments=[{"color": color, "blocks": []}]
        )
        print(f"✅ Alert sent to Slack: {title}")
    except Exception as e:
        print(f"⚠️ Failed to send Slack alert: {e}")
        # Fallback: just log
        print(f"Alert content: {title}")
        print(f"Summary: {summary}")
