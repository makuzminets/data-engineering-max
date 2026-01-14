"""
Feature engineering for signup fraud detection.
Transforms raw signup data into ML-ready features.
"""

import pandas as pd
import numpy as np
from typing import Tuple, List
import re
from datetime import datetime


# Known disposable email domains
DISPOSABLE_DOMAINS = {
    "tempmail.com", "guerrillamail.com", "10minutemail.com", 
    "mailinator.com", "yopmail.com", "throwaway.email",
    "fakeinbox.com", "trashmail.com", "temp-mail.org"
}

# High-risk countries (for fraud, not discrimination - based on fraud patterns)
HIGH_RISK_COUNTRIES = {"NG", "PK", "BD", "VN", "ID", "GH", "KE"}

# Suspicious referral sources
SUSPICIOUS_REFERRALS = {"unknown", "suspicious_affiliate", ""}


class FeatureEngineer:
    """Transform raw signup data into ML features."""
    
    def __init__(self):
        self.feature_columns = []
        self.categorical_columns = []
        
    def fit_transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """Fit and transform the dataset."""
        return self.transform(df)
    
    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """Transform raw data into features."""
        features = df.copy()
        
        # Email features
        features = self._extract_email_features(features)
        
        # Behavioral features
        features = self._extract_behavioral_features(features)
        
        # Device features
        features = self._extract_device_features(features)
        
        # Temporal features
        features = self._extract_temporal_features(features)
        
        # Geographic features
        features = self._extract_geographic_features(features)
        
        # Velocity features (already in raw data)
        features = self._extract_velocity_features(features)
        
        # Risk scores (composite features)
        features = self._calculate_risk_scores(features)
        
        # Select final features
        features = self._select_final_features(features)
        
        return features
    
    def _extract_email_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """Extract features from email address."""
        df = df.copy()
        
        # Extract email parts
        df['email_username'] = df['email'].apply(lambda x: x.split('@')[0])
        df['email_domain'] = df['email'].apply(lambda x: x.split('@')[1])
        
        # Disposable email check
        df['is_disposable_email'] = df['email_domain'].isin(DISPOSABLE_DOMAINS).astype(int)
        
        # Email entropy (randomness indicator)
        df['email_entropy'] = df['email_username'].apply(self._calculate_entropy)
        
        # Digit ratio in email
        df['email_digit_ratio'] = df['email_username'].apply(
            lambda x: sum(c.isdigit() for c in x) / len(x) if len(x) > 0 else 0
        )
        
        # Special characters in email
        df['email_special_char_count'] = df['email_username'].apply(
            lambda x: sum(not c.isalnum() for c in x)
        )
        
        # Numeric suffix (like john123@gmail.com)
        df['has_numeric_suffix'] = df['email_username'].apply(
            lambda x: bool(re.search(r'\d+$', x))
        ).astype(int)
        
        # Common vs rare email provider
        common_providers = {'gmail.com', 'yahoo.com', 'outlook.com', 'hotmail.com', 'icloud.com'}
        df['is_common_email_provider'] = df['email_domain'].isin(common_providers).astype(int)
        
        return df
    
    def _extract_behavioral_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """Extract behavioral features from session data."""
        df = df.copy()
        
        # Normalized session metrics
        df['pages_per_minute'] = df['pages_viewed'] / (df['session_duration_seconds'] / 60 + 0.01)
        
        # Form fill speed (suspicious if too fast)
        df['form_fill_speed'] = df['form_fill_time_seconds'].apply(
            lambda x: 1 if x < 10 else 0  # Less than 10 seconds is suspicious
        )
        
        # Mouse movement density
        df['mouse_density'] = df['mouse_movements'] / (df['session_duration_seconds'] + 0.01)
        
        # Typing speed categories
        df['typing_speed_category'] = pd.cut(
            df['typing_speed_cpm'],
            bins=[0, 200, 400, 600, float('inf')],
            labels=[0, 1, 2, 3]  # slow, normal, fast, inhuman
        ).astype(int)
        
        # Inhuman typing speed flag
        df['inhuman_typing_speed'] = (df['typing_speed_cpm'] > 600).astype(int)
        
        # Low engagement flag
        df['low_engagement'] = (
            (df['session_duration_seconds'] < 30) | 
            (df['pages_viewed'] < 2) |
            (df['mouse_movements'] < 50)
        ).astype(int)
        
        return df
    
    def _extract_device_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """Extract device-related features."""
        df = df.copy()
        
        # VPN flag (already exists, just ensure int type)
        df['is_vpn'] = df['is_vpn'].astype(int)
        
        # User agent analysis
        df['is_bot_user_agent'] = df['user_agent'].apply(self._is_bot_user_agent).astype(int)
        df['is_mobile'] = df['user_agent'].apply(
            lambda x: 1 if isinstance(x, str) and any(m in x.lower() for m in ['iphone', 'android', 'mobile']) else 0
        )
        df['has_empty_user_agent'] = (df['user_agent'] == '').astype(int)
        
        # Screen resolution risk
        common_resolutions = {'1920x1080', '1366x768', '1440x900', '2560x1440', '1536x864'}
        df['uncommon_resolution'] = (~df['screen_resolution'].isin(common_resolutions)).astype(int)
        
        return df
    
    def _extract_temporal_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """Extract time-based features."""
        df = df.copy()
        
        # Convert to datetime if needed
        if not pd.api.types.is_datetime64_any_dtype(df['signup_timestamp']):
            df['signup_timestamp'] = pd.to_datetime(df['signup_timestamp'])
        
        # Hour of day
        df['signup_hour'] = df['signup_timestamp'].dt.hour
        
        # Day of week (0=Monday, 6=Sunday)
        df['signup_day_of_week'] = df['signup_timestamp'].dt.dayofweek
        
        # Weekend flag
        df['is_weekend'] = (df['signup_day_of_week'] >= 5).astype(int)
        
        # Night time flag (suspicious hours: 1am-5am local)
        df['is_night_signup'] = df['signup_hour'].between(1, 5).astype(int)
        
        return df
    
    def _extract_geographic_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """Extract geographic features."""
        df = df.copy()
        
        # Country mismatch (already exists)
        df['country_mismatch'] = df['country_mismatch'].astype(int)
        
        # High risk country
        df['is_high_risk_country'] = df['ip_country'].isin(HIGH_RISK_COUNTRIES).astype(int)
        
        # Timezone mismatch indicator
        # This would need more sophisticated logic with actual timezone data
        
        return df
    
    def _extract_velocity_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """Extract velocity-based features."""
        df = df.copy()
        
        # High velocity flags
        df['high_ip_velocity'] = (df['ip_signup_count_24h'] > 3).astype(int)
        df['high_device_velocity'] = (df['device_signup_count_24h'] > 2).astype(int)
        
        # Combined velocity score
        df['velocity_score'] = df['ip_signup_count_24h'] + df['device_signup_count_24h'] * 2
        
        return df
    
    def _calculate_risk_scores(self, df: pd.DataFrame) -> pd.DataFrame:
        """Calculate composite risk scores."""
        df = df.copy()
        
        # Email risk score
        df['email_risk_score'] = (
            df['is_disposable_email'] * 3 +
            df['email_entropy'] * 0.5 +
            df['email_digit_ratio'] * 2 +
            (1 - df['is_common_email_provider']) * 1
        )
        
        # Behavioral risk score
        df['behavior_risk_score'] = (
            df['form_fill_speed'] * 2 +
            df['inhuman_typing_speed'] * 3 +
            df['low_engagement'] * 2
        )
        
        # Device risk score
        df['device_risk_score'] = (
            df['is_vpn'] * 2 +
            df['is_bot_user_agent'] * 3 +
            df['has_empty_user_agent'] * 2
        )
        
        # Geographic risk score
        df['geo_risk_score'] = (
            df['country_mismatch'] * 2 +
            df['is_high_risk_country'] * 1
        )
        
        # Total risk score
        df['total_risk_score'] = (
            df['email_risk_score'] +
            df['behavior_risk_score'] +
            df['device_risk_score'] +
            df['geo_risk_score'] +
            df['velocity_score']
        )
        
        return df
    
    def _select_final_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """Select final feature columns for modeling."""
        
        feature_columns = [
            # Email features
            'email_length',
            'is_disposable_email',
            'email_entropy',
            'email_digit_ratio',
            'email_special_char_count',
            'has_numeric_suffix',
            'is_common_email_provider',
            
            # Behavioral features
            'session_duration_seconds',
            'pages_viewed',
            'form_fill_time_seconds',
            'mouse_movements',
            'typing_speed_cpm',
            'pages_per_minute',
            'form_fill_speed',
            'mouse_density',
            'typing_speed_category',
            'inhuman_typing_speed',
            'low_engagement',
            
            # Device features
            'is_vpn',
            'is_bot_user_agent',
            'is_mobile',
            'has_empty_user_agent',
            
            # Profile features
            'profile_completeness',
            'has_profile_photo',
            'bio_length',
            
            # Temporal features
            'signup_hour',
            'signup_day_of_week',
            'is_weekend',
            'is_night_signup',
            
            # Geographic features
            'country_mismatch',
            'is_high_risk_country',
            
            # Velocity features
            'ip_signup_count_24h',
            'device_signup_count_24h',
            'high_ip_velocity',
            'high_device_velocity',
            'velocity_score',
            
            # Composite risk scores
            'email_risk_score',
            'behavior_risk_score',
            'device_risk_score',
            'geo_risk_score',
            'total_risk_score',
        ]
        
        self.feature_columns = feature_columns
        
        # Keep user_id and label if present
        output_columns = ['user_id'] + feature_columns
        if 'is_fraud' in df.columns:
            output_columns.append('is_fraud')
        
        return df[output_columns]
    
    @staticmethod
    def _calculate_entropy(text: str) -> float:
        """Calculate Shannon entropy of a string."""
        if not text:
            return 0.0
        
        prob = [float(text.count(c)) / len(text) for c in set(text)]
        entropy = -sum(p * np.log2(p) for p in prob if p > 0)
        return entropy
    
    @staticmethod
    def _is_bot_user_agent(user_agent) -> bool:
        """Check if user agent indicates a bot."""
        if not user_agent or not isinstance(user_agent, str):
            return False
        bot_indicators = [
            'bot', 'crawler', 'spider', 'scraper',
            'python-requests', 'curl', 'wget', 'selenium',
            'headless', 'phantom'
        ]
        ua_lower = user_agent.lower()
        return any(bot in ua_lower for bot in bot_indicators)


def main():
    """Test feature engineering on sample data."""
    from pathlib import Path
    
    data_dir = Path(__file__).parent.parent / "data"
    
    # Load training data
    print("Loading training data...")
    train_df = pd.read_csv(data_dir / "train_signups.csv")
    print(f"  Loaded {len(train_df)} records")
    
    # Apply feature engineering
    print("\nApplying feature engineering...")
    fe = FeatureEngineer()
    features_df = fe.fit_transform(train_df)
    
    print(f"  Generated {len(fe.feature_columns)} features")
    print(f"  Feature columns: {fe.feature_columns}")
    
    # Save features
    features_df.to_csv(data_dir / "train_features.csv", index=False)
    print(f"\nSaved features to {data_dir / 'train_features.csv'}")
    
    # Show correlation with fraud
    print("\nTop features correlated with fraud:")
    correlations = features_df[fe.feature_columns + ['is_fraud']].corr()['is_fraud'].drop('is_fraud')
    top_corr = correlations.abs().sort_values(ascending=False).head(15)
    for feat, corr in top_corr.items():
        print(f"  {feat}: {correlations[feat]:.3f}")


if __name__ == "__main__":
    main()
