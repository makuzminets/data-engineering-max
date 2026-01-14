"""
Generate synthetic signup fraud dataset for model training.
Based on real-world patterns from marketplace fraud detection.
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random
import hashlib
import json
from pathlib import Path


def generate_ip_address(is_vpn: bool = False) -> str:
    """Generate realistic IP addresses."""
    if is_vpn:
        # Common VPN/proxy ranges
        vpn_ranges = [
            (104, 238),  # DigitalOcean
            (45, 33),    # Linode
            (139, 59),   # DigitalOcean
            (185, 199),  # Various VPNs
        ]
        first, second = random.choice(vpn_ranges)
        return f"{first}.{second}.{random.randint(0, 255)}.{random.randint(0, 255)}"
    else:
        # Regular residential IPs
        return f"{random.randint(1, 223)}.{random.randint(0, 255)}.{random.randint(0, 255)}.{random.randint(1, 254)}"


def generate_email(is_fraud: bool = False) -> str:
    """Generate email with fraud patterns."""
    legit_domains = ["gmail.com", "yahoo.com", "outlook.com", "hotmail.com", "icloud.com"]
    fraud_domains = ["tempmail.com", "guerrillamail.com", "10minutemail.com", "mailinator.com", "yopmail.com"]
    
    if is_fraud and random.random() < 0.4:
        # Disposable email
        domain = random.choice(fraud_domains)
        name = ''.join(random.choices('abcdefghijklmnopqrstuvwxyz0123456789', k=random.randint(8, 15)))
    else:
        domain = random.choice(legit_domains)
        if is_fraud and random.random() < 0.3:
            # Random string email
            name = ''.join(random.choices('abcdefghijklmnopqrstuvwxyz0123456789', k=random.randint(10, 20)))
        else:
            # Normal looking email
            first_names = ["john", "jane", "mike", "sarah", "david", "emma", "alex", "lisa", "chris", "anna"]
            last_names = ["smith", "johnson", "williams", "brown", "jones", "garcia", "miller", "davis"]
            name = f"{random.choice(first_names)}.{random.choice(last_names)}{random.randint(1, 999)}"
    
    return f"{name}@{domain}"


def generate_user_agent(is_fraud: bool = False) -> str:
    """Generate user agent strings."""
    legit_agents = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0",
        "Mozilla/5.0 (iPhone; CPU iPhone OS 17_2_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Mobile/15E148 Safari/604.1",
        "Mozilla/5.0 (iPad; CPU OS 17_2_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Mobile/15E148 Safari/604.1",
    ]
    
    fraud_agents = [
        "python-requests/2.28.0",
        "curl/7.68.0",
        "Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)",
        "Selenium/4.0",
        "",  # Empty user agent
    ]
    
    if is_fraud and random.random() < 0.2:
        return random.choice(fraud_agents)
    return random.choice(legit_agents)


def generate_signup_data(n_samples: int = 10000, fraud_ratio: float = 0.05) -> pd.DataFrame:
    """
    Generate synthetic signup dataset.
    
    Args:
        n_samples: Number of signups to generate
        fraud_ratio: Proportion of fraudulent signups (default 5%)
    
    Returns:
        DataFrame with signup features and labels
    """
    np.random.seed(42)
    random.seed(42)
    
    data = []
    n_fraud = int(n_samples * fraud_ratio)
    n_legit = n_samples - n_fraud
    
    # Generate legitimate signups
    for i in range(n_legit):
        signup = generate_single_signup(is_fraud=False, user_id=i)
        data.append(signup)
    
    # Generate fraudulent signups
    for i in range(n_fraud):
        signup = generate_single_signup(is_fraud=True, user_id=n_legit + i)
        data.append(signup)
    
    df = pd.DataFrame(data)
    
    # Shuffle the data
    df = df.sample(frac=1, random_state=42).reset_index(drop=True)
    
    return df


def generate_single_signup(is_fraud: bool, user_id: int) -> dict:
    """Generate a single signup record."""
    
    # Timestamp
    base_date = datetime(2024, 1, 1)
    signup_timestamp = base_date + timedelta(
        days=random.randint(0, 365),
        hours=random.randint(0, 23),
        minutes=random.randint(0, 59)
    )
    
    # IP and location
    is_vpn = is_fraud and random.random() < 0.6
    ip_address = generate_ip_address(is_vpn)
    
    # Email
    email = generate_email(is_fraud)
    
    # Session behavior
    if is_fraud:
        session_duration_seconds = random.randint(5, 60)  # Very fast
        pages_viewed = random.randint(1, 3)
        form_fill_time_seconds = random.randint(2, 15)  # Suspiciously fast
        mouse_movements = random.randint(0, 50)
        typing_speed_cpm = random.randint(800, 2000)  # Inhuman speed (copy-paste)
    else:
        session_duration_seconds = random.randint(60, 600)
        pages_viewed = random.randint(2, 15)
        form_fill_time_seconds = random.randint(30, 300)
        mouse_movements = random.randint(100, 1000)
        typing_speed_cpm = random.randint(150, 400)  # Normal human speed
    
    # Device fingerprint
    user_agent = generate_user_agent(is_fraud)
    screen_resolution = random.choice(["1920x1080", "1366x768", "1440x900", "2560x1440", "1536x864"])
    timezone_offset = random.choice([-8, -7, -6, -5, -4, 0, 1, 2, 3, 5, 8, 9])
    
    # Referral source
    if is_fraud:
        referral_source = random.choice(["direct", "unknown", "suspicious_affiliate", "direct", "direct"])
    else:
        referral_source = random.choice(["google", "facebook", "linkedin", "twitter", "direct", "referral", "email"])
    
    # Account details
    if is_fraud:
        profile_completeness = random.uniform(0.1, 0.4)  # Minimal profile
        has_profile_photo = random.random() < 0.1
        bio_length = random.randint(0, 50)
    else:
        profile_completeness = random.uniform(0.5, 1.0)
        has_profile_photo = random.random() < 0.7
        bio_length = random.randint(50, 500)
    
    # Email patterns
    email_has_numbers = any(c.isdigit() for c in email.split('@')[0])
    email_length = len(email.split('@')[0])
    
    # Historical signals (from IP/device)
    if is_fraud:
        ip_signup_count_24h = random.randint(3, 20)  # Multiple signups from same IP
        device_signup_count_24h = random.randint(2, 10)
    else:
        ip_signup_count_24h = random.randint(0, 2)
        device_signup_count_24h = random.randint(0, 1)
    
    # Geographic mismatch
    claimed_country = random.choice(["US", "UK", "CA", "DE", "FR", "IN", "PH", "NG", "PK", "BD"])
    if is_fraud and random.random() < 0.5:
        ip_country = random.choice(["NG", "PK", "BD", "VN", "ID"])  # Different from claimed
    else:
        ip_country = claimed_country
    
    country_mismatch = claimed_country != ip_country
    
    return {
        "user_id": user_id,
        "signup_timestamp": signup_timestamp,
        "email": email,
        "ip_address": ip_address,
        "user_agent": user_agent,
        
        # Behavioral features
        "session_duration_seconds": session_duration_seconds,
        "pages_viewed": pages_viewed,
        "form_fill_time_seconds": form_fill_time_seconds,
        "mouse_movements": mouse_movements,
        "typing_speed_cpm": typing_speed_cpm,
        
        # Device features
        "screen_resolution": screen_resolution,
        "timezone_offset": timezone_offset,
        "is_vpn": is_vpn,
        
        # Referral
        "referral_source": referral_source,
        
        # Profile features
        "profile_completeness": profile_completeness,
        "has_profile_photo": has_profile_photo,
        "bio_length": bio_length,
        
        # Email features
        "email_has_numbers": email_has_numbers,
        "email_length": email_length,
        
        # Historical features
        "ip_signup_count_24h": ip_signup_count_24h,
        "device_signup_count_24h": device_signup_count_24h,
        
        # Geographic
        "claimed_country": claimed_country,
        "ip_country": ip_country,
        "country_mismatch": country_mismatch,
        
        # Label
        "is_fraud": is_fraud
    }


def main():
    """Generate and save datasets."""
    output_dir = Path(__file__).parent
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Generate training dataset
    print("Generating training dataset (10,000 samples, 5% fraud)...")
    train_df = generate_signup_data(n_samples=10000, fraud_ratio=0.05)
    train_df.to_csv(output_dir / "train_signups.csv", index=False)
    print(f"  Saved to {output_dir / 'train_signups.csv'}")
    print(f"  Total: {len(train_df)}, Fraud: {train_df['is_fraud'].sum()}")
    
    # Generate test dataset
    print("\nGenerating test dataset (2,000 samples, 5% fraud)...")
    test_df = generate_signup_data(n_samples=2000, fraud_ratio=0.05)
    test_df.to_csv(output_dir / "test_signups.csv", index=False)
    print(f"  Saved to {output_dir / 'test_signups.csv'}")
    print(f"  Total: {len(test_df)}, Fraud: {test_df['is_fraud'].sum()}")
    
    # Show sample
    print("\nSample fraud record:")
    print(train_df[train_df['is_fraud'] == True].iloc[0].to_dict())
    
    print("\nSample legitimate record:")
    print(train_df[train_df['is_fraud'] == False].iloc[0].to_dict())


if __name__ == "__main__":
    main()
