# 🛡️ Fraud Detection ML Pipeline

Production-grade signup fraud detection system using XGBoost and LightGBM. Built based on real-world experience reducing scam incidents by **70%** at Upwork and chargebacks from **7% to 1%** at Semrush.

## 🎯 Overview

This pipeline provides end-to-end fraud detection for user signups:

1. **Feature Engineering** — 40+ behavioral, device, and velocity signals
2. **ML Models** — XGBoost & LightGBM with imbalanced data handling
3. **Real-time API** — FastAPI service for instant fraud scoring
4. **Docker Deployment** — Ready for production

## 🏗️ Architecture

```
┌─────────────┐     ┌──────────────────┐     ┌─────────────┐
│   Signup    │────▶│ Feature Engine   │────▶│  ML Model   │
│   Request   │     │ (40+ features)   │     │ XGBoost/LGB │
└─────────────┘     └──────────────────┘     └──────┬──────┘
                                                    │
                                                    ▼
                    ┌──────────────────────────────────────────┐
                    │             Risk Response                 │
                    │ • fraud_probability: 0.85                │
                    │ • risk_level: high                       │
                    │ • decision: review                       │
                    │ • flags: [vpn, inhuman_typing_speed]    │
                    └──────────────────────────────────────────┘
```

## 📂 Project Structure

```
fraud_ml_pipeline/
├── README.md
├── requirements.txt
├── Dockerfile
├── docker-compose.yml
│
├── data/
│   ├── generate_dataset.py      # Synthetic data generator
│   ├── train_signups.csv        # Training data (generated)
│   └── test_signups.csv         # Test data (generated)
│
├── features/
│   └── feature_engineering.py   # 40+ fraud detection features
│
├── models/
│   ├── train_model.py           # XGBoost & LightGBM training
│   └── saved_models/            # Trained model artifacts
│
└── api/
    └── scoring_service.py       # FastAPI real-time scoring
```

## 🚀 Quick Start

### Option 1: Docker (Recommended)

```bash
# Build and run
docker-compose up -d

# API available at http://localhost:8000
# Swagger docs at http://localhost:8000/docs
```

### Option 2: Local Setup

```bash
# Install dependencies
pip install -r requirements.txt

# Generate training data
python data/generate_dataset.py

# Train models
python models/train_model.py

# Start API
uvicorn api.scoring_service:app --reload
```

## 📊 Feature Engineering

### 40+ Fraud Detection Features

| Category | Features | Description |
|----------|----------|-------------|
| **Email** | entropy, disposable, digit_ratio | Pattern detection in email addresses |
| **Behavioral** | session_duration, form_speed, typing_speed | User interaction patterns |
| **Device** | is_vpn, user_agent, screen_resolution | Device fingerprinting |
| **Velocity** | ip_signup_count, device_signup_count | Abuse rate signals |
| **Geographic** | country_mismatch, high_risk_country | Location anomalies |
| **Profile** | completeness, has_photo, bio_length | Account quality |

### Feature Importance (Example)

```
Top 10 Features:
1. total_risk_score:     0.142
2. velocity_score:       0.098
3. form_fill_time:       0.087
4. typing_speed_cpm:     0.076
5. email_entropy:        0.065
6. session_duration:     0.058
7. is_vpn:               0.052
8. ip_signup_count_24h:  0.048
9. country_mismatch:     0.045
10. profile_completeness: 0.041
```

## 🤖 ML Models

### XGBoost & LightGBM

Both models are trained with:
- **Class imbalance handling** via `scale_pos_weight`
- **Early stopping** to prevent overfitting
- **Cross-validation** for robust evaluation

### Model Metrics (Example)

| Metric | XGBoost | LightGBM |
|--------|---------|----------|
| ROC-AUC | 0.9850 | 0.9842 |
| Precision | 0.91 | 0.90 |
| Recall | 0.88 | 0.87 |
| F1 Score | 0.89 | 0.88 |

## 🔌 API Reference

### Score Signup

```bash
POST /score
Content-Type: application/json

{
  "user_id": "user_12345",
  "email": "john.smith42@gmail.com",
  "ip_address": "192.168.1.1",
  "session_duration_seconds": 180,
  "form_fill_time_seconds": 90,
  "typing_speed_cpm": 250,
  "is_vpn": false,
  "claimed_country": "US",
  "ip_country": "US",
  "ip_signup_count_24h": 1
}
```

### Response

```json
{
  "user_id": "user_12345",
  "fraud_probability": 0.12,
  "risk_level": "low",
  "risk_score": 12.0,
  "decision": "approve",
  "email_risk": 0.0,
  "behavior_risk": 0.0,
  "device_risk": 0.0,
  "geo_risk": 0.0,
  "velocity_risk": 0.2,
  "risk_flags": [],
  "model_version": "xgboost_20240115_120000",
  "scored_at": "2024-01-15T12:00:00Z"
}
```

### Risk Levels & Decisions

| Probability | Risk Level | Decision |
|-------------|------------|----------|
| 0.0 - 0.2 | low | approve |
| 0.2 - 0.5 | medium | approve (monitor) |
| 0.5 - 0.8 | high | review |
| 0.8 - 1.0 | critical | block |

### Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/health` | GET | Health check |
| `/score` | POST | Score single signup |
| `/batch_score` | POST | Score multiple signups |
| `/docs` | GET | Swagger documentation |

## 🧪 Testing

```bash
# Test data generation
python data/generate_dataset.py

# Test feature engineering
python features/feature_engineering.py

# Train and evaluate models
python models/train_model.py

# Test API
curl -X POST http://localhost:8000/score \
  -H "Content-Type: application/json" \
  -d '{"user_id": "test", "email": "test@gmail.com", "ip_address": "1.2.3.4"}'
```

## 📈 Production Considerations

### Scaling

- **Horizontal scaling**: Run multiple API containers behind load balancer
- **Batch scoring**: Use `/batch_score` for bulk operations
- **Caching**: Model is loaded once at startup

### Monitoring

- Track fraud_probability distribution
- Alert on high false positive rates
- Monitor model drift over time

### Model Updates

1. Collect labeled data from manual reviews
2. Retrain models periodically (weekly/monthly)
3. A/B test new models before full deployment
4. Version all models with timestamps

## 🔗 Related Projects

- [Airflow Pipelines](../airflow_pipelines) — ETL for fraud data ingestion
- [dbt Fraud Analytics](../dbt_fraud_analytics) — Fraud metrics & reporting

## 📚 References

Based on real-world experience:
- **Upwork**: Signup fraud detection, 70% scam reduction
- **Semrush**: Chargeback reduction from 7% to 1%
- **MRC Conferences**: Amsterdam & Dublin presentations

## 📄 License

MIT License
