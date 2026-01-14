# 💳 Chargeback Analytics Dashboard

Production-ready chargeback monitoring and analytics system. Built based on experience reducing chargebacks from **7% to 1%** at Semrush.

## 🎯 Overview

End-to-end solution for payment fraud analytics:

1. **Synthetic Data** — Realistic chargeback patterns with Visa/Mastercard reason codes
2. **dbt Models** — Staging → Marts pipeline for analytics-ready data
3. **Streamlit Dashboard** — Interactive monitoring with key metrics
4. **SQL Queries** — Ready for Looker/Metabase integration

## 📊 Dashboard Preview

```
┌─────────────────────────────────────────────────────────────────┐
│  💳 CHARGEBACK ANALYTICS DASHBOARD                              │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  Transactions    Revenue      Chargebacks    Rate      Net Loss │
│  ┌─────────┐   ┌─────────┐   ┌─────────┐   ┌──────┐   ┌───────┐│
│  │  50,000 │   │ $1.2M   │   │   850   │   │ 1.7% │   │ $25K  ││
│  └─────────┘   └─────────┘   └─────────┘   └──────┘   └───────┘│
│                                                                 │
│  [═══════════════ Chargeback Rate Trend ════════════════]      │
│                                                                 │
│  [By Category]          [By Country]      [By Payment]         │
│  ████ Fraud 45%         🇺🇸 US 35%         💳 Visa 45%        │
│  ███ Service 30%        🇳🇬 NG 8%          💳 MC 35%          │
│  ██ Auth 15%            🇧🇷 BR 7%          💳 Amex 12%        │
│  █ Processing 10%                                               │
└─────────────────────────────────────────────────────────────────┘
```

## 📂 Project Structure

```
chargeback_analytics/
├── README.md
├── requirements.txt
│
├── data/
│   ├── generate_chargebacks.py  # Synthetic data generator
│   ├── transactions.csv         # 50K transactions
│   └── chargebacks.csv          # Chargeback records
│
├── dbt/
│   ├── dbt_project.yml
│   └── models/
│       ├── sources.yml
│       ├── staging/
│       │   ├── stg_transactions.sql
│       │   └── stg_chargebacks.sql
│       └── marts/
│           ├── fct_chargebacks.sql
│           ├── chargeback_daily_summary.sql
│           └── chargeback_by_dimension.sql
│
└── dashboard/
    └── app.py                   # Streamlit dashboard
```

## 🚀 Quick Start

### 1. Generate Data

```bash
# Install dependencies
pip install -r requirements.txt

# Generate synthetic data
python data/generate_chargebacks.py
```

### 2. Run Dashboard

```bash
streamlit run dashboard/app.py
```

Open http://localhost:8501

### 3. dbt Models (Optional)

```bash
cd dbt
dbt run
```

## 📈 Key Metrics

| Metric | Description | Target |
|--------|-------------|--------|
| **Chargeback Rate** | Chargebacks / Transactions | < 1% |
| **Chargeback Amount Rate** | CB Amount / Revenue | < 0.5% |
| **Dispute Win Rate** | Won / Filed | > 50% |
| **Fraud %** | Fraud CBs / Total CBs | Monitor |
| **3DS Coverage** | 3DS Transactions / Total | > 70% |

## 🔍 Dashboard Features

### KPI Cards
- Total transactions & revenue
- Chargeback count & rate
- Net loss after disputes
- Dispute win rate

### Trend Analysis
- Daily chargeback rate with 7-day moving average
- 1% threshold alert line
- Volume trends

### Breakdown Charts
- By reason category (fraud, service, auth, processing)
- By country (with chargeback rate)
- By payment method
- By plan type

### Data Tables
- Recent chargebacks with details
- Filterable by date range

## 📊 dbt Models

### Staging Layer

| Model | Description |
|-------|-------------|
| `stg_transactions` | Cleaned transactions with risk flags |
| `stg_chargebacks` | Enriched chargeback records |

### Marts Layer

| Model | Description |
|-------|-------------|
| `fct_chargebacks` | Fact table with full context |
| `chargeback_daily_summary` | Daily aggregates for dashboards |
| `chargeback_by_dimension` | Pivot by country/plan/payment/etc. |

## 🎯 Reason Codes

### Visa Reason Codes
| Code | Category | Description |
|------|----------|-------------|
| 10.4 | Fraud | Card Absent Environment |
| 10.5 | Fraud | Card Present |
| 13.1 | Service | Not Received |
| 13.3 | Service | Not as Described |
| 11.1 | Auth | Card Recovery Bulletin |

### Mastercard Reason Codes
| Code | Category | Description |
|------|----------|-------------|
| 4837 | Fraud | No Cardholder Authorization |
| 4863 | Fraud | Cardholder Does Not Recognize |
| 4853 | Service | Cardholder Dispute |
| 4834 | Processing | Duplicate Processing |

## 🛡️ Fraud Prevention Insights

### Key Findings from Data

1. **3D Secure Impact**
   - Transactions with 3DS have ~50% lower fraud chargebacks
   - Liability shifts to issuer for 3DS transactions

2. **High-Risk Countries**
   - NG, PK, RU have 2-2.5x higher chargeback rates
   - Implement additional verification for these regions

3. **Plan Correlation**
   - Lower-tier plans have higher chargeback rates
   - Card testing often uses cheapest option

4. **Timing Patterns**
   - Chargebacks filed 15-45 days after transaction
   - Resolution takes 30-90 days

## 📚 References

Based on real-world experience:
- **Semrush**: Reduced chargebacks from 7% to 1%
- **MRC Conferences**: Amsterdam & Dublin presentations
- Payment card network rules (Visa, Mastercard)

## 🔗 Related Projects

- [Fraud ML Pipeline](../fraud_ml_pipeline) — ML-based fraud detection
- [dbt Fraud Analytics](../dbt_fraud_analytics) — Transaction risk models

## 📄 License

MIT License
