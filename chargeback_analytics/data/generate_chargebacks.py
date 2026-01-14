"""
Generate synthetic chargeback dataset for analytics.
Based on real-world patterns from SaaS payment fraud prevention.
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random
from pathlib import Path


# Chargeback reason codes (Visa/Mastercard)
REASON_CODES = {
    # Fraud-related
    "10.4": {"category": "fraud", "description": "Card Absent Environment", "dispute_rate": 0.7},
    "10.5": {"category": "fraud", "description": "Fraud - Card Present", "dispute_rate": 0.6},
    "4837": {"category": "fraud", "description": "No Cardholder Authorization", "dispute_rate": 0.65},
    "4863": {"category": "fraud", "description": "Cardholder Does Not Recognize", "dispute_rate": 0.5},
    
    # Service-related
    "13.1": {"category": "service", "description": "Merchandise/Services Not Received", "dispute_rate": 0.4},
    "13.3": {"category": "service", "description": "Not as Described", "dispute_rate": 0.35},
    "4853": {"category": "service", "description": "Cardholder Dispute", "dispute_rate": 0.45},
    
    # Authorization-related
    "11.1": {"category": "authorization", "description": "Card Recovery Bulletin", "dispute_rate": 0.8},
    "11.2": {"category": "authorization", "description": "Declined Authorization", "dispute_rate": 0.75},
    
    # Processing errors
    "12.1": {"category": "processing", "description": "Late Presentment", "dispute_rate": 0.3},
    "12.5": {"category": "processing", "description": "Incorrect Amount", "dispute_rate": 0.25},
    "4834": {"category": "processing", "description": "Duplicate Processing", "dispute_rate": 0.2},
}

# Subscription plans
PLANS = {
    "starter": {"price": 9.99, "chargeback_rate": 0.02},
    "pro": {"price": 29.99, "chargeback_rate": 0.015},
    "business": {"price": 99.99, "chargeback_rate": 0.01},
    "enterprise": {"price": 299.99, "chargeback_rate": 0.005},
}

# Countries with fraud risk levels
COUNTRIES = {
    "US": {"risk": "low", "chargeback_multiplier": 1.0},
    "UK": {"risk": "low", "chargeback_multiplier": 1.0},
    "DE": {"risk": "low", "chargeback_multiplier": 0.9},
    "CA": {"risk": "low", "chargeback_multiplier": 1.0},
    "FR": {"risk": "low", "chargeback_multiplier": 0.95},
    "AU": {"risk": "low", "chargeback_multiplier": 1.0},
    "BR": {"risk": "medium", "chargeback_multiplier": 1.5},
    "IN": {"risk": "medium", "chargeback_multiplier": 1.3},
    "RU": {"risk": "high", "chargeback_multiplier": 2.0},
    "NG": {"risk": "high", "chargeback_multiplier": 2.5},
    "PK": {"risk": "high", "chargeback_multiplier": 2.2},
}

# Payment methods
PAYMENT_METHODS = {
    "visa": {"share": 0.45, "chargeback_rate": 0.012},
    "mastercard": {"share": 0.35, "chargeback_rate": 0.011},
    "amex": {"share": 0.12, "chargeback_rate": 0.015},
    "paypal": {"share": 0.08, "chargeback_rate": 0.008},
}


def generate_transactions(n_transactions: int = 50000, 
                          start_date: datetime = None,
                          end_date: datetime = None) -> pd.DataFrame:
    """Generate synthetic transaction data."""
    
    np.random.seed(42)
    random.seed(42)
    
    if start_date is None:
        start_date = datetime(2024, 1, 1)
    if end_date is None:
        end_date = datetime(2024, 12, 31)
    
    transactions = []
    
    for i in range(n_transactions):
        # Random timestamp
        days_range = (end_date - start_date).days
        tx_date = start_date + timedelta(
            days=random.randint(0, days_range),
            hours=random.randint(0, 23),
            minutes=random.randint(0, 59)
        )
        
        # Select plan
        plan = random.choices(
            list(PLANS.keys()),
            weights=[0.4, 0.35, 0.2, 0.05]
        )[0]
        
        # Select country
        country = random.choices(
            list(COUNTRIES.keys()),
            weights=[0.35, 0.15, 0.1, 0.08, 0.07, 0.05, 0.08, 0.05, 0.03, 0.02, 0.02]
        )[0]
        
        # Select payment method
        payment_method = random.choices(
            list(PAYMENT_METHODS.keys()),
            weights=[p["share"] for p in PAYMENT_METHODS.values()]
        )[0]
        
        # Calculate base chargeback probability
        base_rate = PLANS[plan]["chargeback_rate"]
        country_mult = COUNTRIES[country]["chargeback_multiplier"]
        payment_rate = PAYMENT_METHODS[payment_method]["chargeback_rate"]
        
        chargeback_prob = base_rate * country_mult * (payment_rate / 0.01)
        
        # Add some randomness
        chargeback_prob = min(0.15, chargeback_prob * random.uniform(0.5, 1.5))
        
        # Determine if this transaction will be a chargeback
        is_chargeback = random.random() < chargeback_prob
        
        # Generate transaction
        amount = PLANS[plan]["price"]
        if random.random() < 0.1:  # 10% have additional charges
            amount += random.choice([4.99, 9.99, 19.99])
        
        tx = {
            "transaction_id": f"tx_{i:08d}",
            "customer_id": f"cust_{random.randint(1, n_transactions // 5):06d}",
            "transaction_date": tx_date,
            "amount": round(amount, 2),
            "currency": "USD",
            "plan": plan,
            "payment_method": payment_method,
            "country": country,
            "is_3ds": random.random() < 0.6,  # 60% use 3D Secure
            "is_recurring": random.random() < 0.7,  # 70% are recurring
            "device_type": random.choice(["desktop", "mobile", "tablet"]),
            "is_chargeback": is_chargeback,
        }
        
        transactions.append(tx)
    
    return pd.DataFrame(transactions)


def generate_chargebacks(transactions_df: pd.DataFrame) -> pd.DataFrame:
    """Generate chargeback records from transactions."""
    
    chargeback_txs = transactions_df[transactions_df["is_chargeback"]].copy()
    
    chargebacks = []
    
    for _, tx in chargeback_txs.iterrows():
        # Chargeback typically filed 15-45 days after transaction
        days_to_chargeback = random.randint(15, 45)
        chargeback_date = tx["transaction_date"] + timedelta(days=days_to_chargeback)
        
        # Select reason code based on patterns
        if tx["is_3ds"]:
            # 3DS reduces fraud chargebacks
            reason_weights = {
                "10.4": 0.1, "10.5": 0.05, "4837": 0.1, "4863": 0.15,
                "13.1": 0.2, "13.3": 0.15, "4853": 0.1,
                "11.1": 0.02, "11.2": 0.03,
                "12.1": 0.05, "12.5": 0.03, "4834": 0.02
            }
        else:
            # No 3DS = more fraud chargebacks
            reason_weights = {
                "10.4": 0.25, "10.5": 0.15, "4837": 0.2, "4863": 0.1,
                "13.1": 0.1, "13.3": 0.08, "4853": 0.05,
                "11.1": 0.02, "11.2": 0.02,
                "12.1": 0.01, "12.5": 0.01, "4834": 0.01
            }
        
        reason_code = random.choices(
            list(reason_weights.keys()),
            weights=list(reason_weights.values())
        )[0]
        
        reason_info = REASON_CODES[reason_code]
        
        # Dispute outcome
        dispute_won = random.random() > reason_info["dispute_rate"]
        
        # Resolution time (30-90 days)
        resolution_days = random.randint(30, 90)
        resolution_date = chargeback_date + timedelta(days=resolution_days)
        
        chargeback = {
            "chargeback_id": f"cb_{len(chargebacks):06d}",
            "transaction_id": tx["transaction_id"],
            "customer_id": tx["customer_id"],
            "transaction_date": tx["transaction_date"],
            "chargeback_date": chargeback_date,
            "amount": tx["amount"],
            "currency": tx["currency"],
            "reason_code": reason_code,
            "reason_category": reason_info["category"],
            "reason_description": reason_info["description"],
            "payment_method": tx["payment_method"],
            "country": tx["country"],
            "plan": tx["plan"],
            "is_3ds": tx["is_3ds"],
            "is_recurring": tx["is_recurring"],
            "dispute_filed": random.random() < 0.7,  # 70% disputed
            "dispute_won": dispute_won if random.random() < 0.7 else None,
            "resolution_date": resolution_date,
            "status": random.choice(["resolved", "pending", "escalated"]) if resolution_date <= datetime.now() else "open",
        }
        
        chargebacks.append(chargeback)
    
    return pd.DataFrame(chargebacks)


def generate_daily_metrics(transactions_df: pd.DataFrame, 
                           chargebacks_df: pd.DataFrame) -> pd.DataFrame:
    """Generate daily aggregated metrics."""
    
    # Daily transaction metrics
    tx_daily = transactions_df.groupby(transactions_df["transaction_date"].dt.date).agg({
        "transaction_id": "count",
        "amount": "sum",
        "is_3ds": "mean",
    }).reset_index()
    tx_daily.columns = ["date", "total_transactions", "total_revenue", "pct_3ds"]
    
    # Daily chargeback metrics
    cb_daily = chargebacks_df.groupby(chargebacks_df["chargeback_date"].dt.date).agg({
        "chargeback_id": "count",
        "amount": "sum",
    }).reset_index()
    cb_daily.columns = ["date", "total_chargebacks", "chargeback_amount"]
    
    # Merge
    metrics = tx_daily.merge(cb_daily, on="date", how="left").fillna(0)
    
    # Calculate rates
    metrics["chargeback_rate"] = metrics["total_chargebacks"] / metrics["total_transactions"]
    metrics["chargeback_amount_rate"] = metrics["chargeback_amount"] / metrics["total_revenue"]
    
    # 7-day rolling averages
    metrics["chargeback_rate_7d"] = metrics["chargeback_rate"].rolling(7, min_periods=1).mean()
    metrics["revenue_7d"] = metrics["total_revenue"].rolling(7, min_periods=1).sum()
    
    return metrics


def main():
    """Generate and save all datasets."""
    output_dir = Path(__file__).parent
    output_dir.mkdir(parents=True, exist_ok=True)
    
    print("Generating transactions...")
    transactions_df = generate_transactions(n_transactions=50000)
    transactions_df.to_csv(output_dir / "transactions.csv", index=False)
    print(f"  Saved {len(transactions_df)} transactions")
    print(f"  Chargeback rate: {transactions_df['is_chargeback'].mean():.2%}")
    
    print("\nGenerating chargebacks...")
    chargebacks_df = generate_chargebacks(transactions_df)
    chargebacks_df.to_csv(output_dir / "chargebacks.csv", index=False)
    print(f"  Saved {len(chargebacks_df)} chargebacks")
    
    print("\nGenerating daily metrics...")
    metrics_df = generate_daily_metrics(transactions_df, chargebacks_df)
    metrics_df.to_csv(output_dir / "daily_metrics.csv", index=False)
    print(f"  Saved {len(metrics_df)} daily records")
    
    # Summary statistics
    print("\n" + "="*50)
    print("DATASET SUMMARY")
    print("="*50)
    print(f"Total transactions: {len(transactions_df):,}")
    print(f"Total revenue: ${transactions_df['amount'].sum():,.2f}")
    print(f"Total chargebacks: {len(chargebacks_df):,}")
    print(f"Chargeback amount: ${chargebacks_df['amount'].sum():,.2f}")
    print(f"Overall chargeback rate: {len(chargebacks_df)/len(transactions_df):.2%}")
    
    print("\nChargebacks by category:")
    for cat, count in chargebacks_df["reason_category"].value_counts().items():
        print(f"  {cat}: {count} ({count/len(chargebacks_df):.1%})")
    
    print("\nChargebacks by country (top 5):")
    for country, count in chargebacks_df["country"].value_counts().head().items():
        rate = count / len(transactions_df[transactions_df["country"] == country])
        print(f"  {country}: {count} ({rate:.2%} rate)")


if __name__ == "__main__":
    main()
