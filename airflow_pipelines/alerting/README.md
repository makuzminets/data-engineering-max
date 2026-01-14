# Pipeline Alerting System

Comprehensive alerting framework for Airflow pipelines with Slack and email notifications.

## 🎯 Key Features

- **Slack Alerts**: Rich Block Kit messages with context
- **Email Fallback**: HTML templates for critical failures
- **Escalation**: Mention on-call for failures
- **SLA Tracking**: Miss notifications
- **Retry Alerts**: Know when tasks are struggling

## 🏗️ Architecture

```
┌─────────────────┐     ┌──────────────────┐     ┌─────────────────┐
│   DAG/Task      │────▶│  Alert Callbacks │────▶│     Slack       │
│   Events        │     │                  │     │     #alerts     │
│                 │     │  - on_success    │     └─────────────────┘
│  - Success      │     │  - on_failure    │
│  - Failure      │     │  - on_retry      │     ┌─────────────────┐
│  - Retry        │     │  - on_sla_miss   │────▶│     Email       │
│  - SLA Miss     │     │                  │     │  (escalation)   │
└─────────────────┘     └──────────────────┘     └─────────────────┘
```

## 📁 Project Structure

```
alerting/
├── dags/
│   └── pipeline_alerting.py      # Demo DAG with alerts
├── includes/
│   ├── __init__.py
│   ├── slack_alerts.py           # Slack callback class
│   └── email_alerts.py           # Email callback class
└── README.md
```

## 🔧 Setup

### 1. Configure Slack Webhook

```bash
airflow connections add 'slack_webhook' \
    --conn-type 'http' \
    --conn-host 'https://hooks.slack.com/services' \
    --conn-password '/T00000000/B00000000/XXXXXXXXXXXXXXXXXXXXXXXX'
```

### 2. Configure Email (SMTP)

```bash
# In airflow.cfg or environment variables
AIRFLOW__SMTP__SMTP_HOST=smtp.gmail.com
AIRFLOW__SMTP__SMTP_PORT=587
AIRFLOW__SMTP__SMTP_USER=alerts@company.com
AIRFLOW__SMTP__SMTP_PASSWORD=app-password
AIRFLOW__SMTP__SMTP_MAIL_FROM=alerts@company.com
```

## 🔔 Usage

### DAG-Level Alerts

```python
from includes.slack_alerts import SlackAlertCallback

slack = SlackAlertCallback(
    channel="#data-alerts",
    mention_on_failure="@oncall",
)

with DAG(
    "my_dag",
    on_success_callback=slack.on_success,
    on_failure_callback=slack.on_failure,
) as dag:
    ...
```

### Task-Level Alerts

```python
@task(on_failure_callback=slack.on_failure)
def critical_task():
    ...
```

### Combined Alerts (Slack + Email)

```python
from includes.slack_alerts import SlackAlertCallback
from includes.email_alerts import EmailAlertCallback

slack = SlackAlertCallback(channel="#alerts")
email = EmailAlertCallback(
    recipients=["team@company.com"],
    cc_on_failure=["oncall@company.com"],
)

def on_failure(context):
    slack.on_failure(context)
    email.on_failure(context)

with DAG("critical_dag", on_failure_callback=on_failure):
    ...
```

## 📊 Alert Examples

### Slack Success Message

```
✅ DAG Succeeded: daily_etl
Run ID: scheduled__2024-01-15
Duration: 5m 32s
Date: 2024-01-15 06:00
```

### Slack Failure Message

```
@oncall ❌ DAG Failed: daily_etl

Task: load_data
Run ID: scheduled__2024-01-15

Error:
BigQueryError: Table not found

[📋 View Logs]
```

### Email Failure

Rich HTML email with:
- Error details
- DAG/Task info
- Direct link to logs
- Timestamp

## 🎚️ Configuration Options

| Option | Description | Default |
|--------|-------------|---------|
| `channel` | Slack channel | `#data-alerts` |
| `mention_on_failure` | User/group to ping | `None` |
| `include_log_link` | Add logs button | `True` |
| `cc_on_failure` | Additional email recipients | `[]` |

## 🔗 Related Patterns

- [Data Quality](../data_quality/) - Quality check alerts
- [Backfill Manager](../backfill_manager/) - Backfill notifications
