"""
Email Funnel Analytics - Synthetic Data Generator

Generates realistic email marketing funnel data including:
- Users with signup information
- Email events (sent, opened, clicked)
- Conversions (trial starts, paid conversions)

The data simulates a SaaS email marketing funnel with realistic drop-off rates.
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random

# Set random seed for reproducibility
np.random.seed(42)
random.seed(42)

# Configuration
NUM_USERS = 10000
START_DATE = datetime(2025, 1, 1)
END_DATE = datetime(2025, 12, 31)

# Traffic sources with different quality
SOURCES = {
    'organic': {'weight': 0.25, 'quality': 1.2},
    'google_ads': {'weight': 0.30, 'quality': 1.0},
    'facebook_ads': {'weight': 0.20, 'quality': 0.8},
    'referral': {'weight': 0.10, 'quality': 1.4},
    'email_campaign': {'weight': 0.10, 'quality': 1.1},
    'direct': {'weight': 0.05, 'quality': 1.0},
}

# Device types
DEVICES = {
    'desktop': {'weight': 0.45, 'engagement': 1.1},
    'mobile': {'weight': 0.45, 'engagement': 0.85},
    'tablet': {'weight': 0.10, 'engagement': 0.95},
}

# Countries with different engagement levels
COUNTRIES = {
    'US': {'weight': 0.35, 'engagement': 1.0},
    'UK': {'weight': 0.15, 'engagement': 1.05},
    'DE': {'weight': 0.10, 'engagement': 1.1},
    'FR': {'weight': 0.08, 'engagement': 0.95},
    'CA': {'weight': 0.07, 'engagement': 1.0},
    'AU': {'weight': 0.05, 'engagement': 1.0},
    'BR': {'weight': 0.05, 'engagement': 0.8},
    'IN': {'weight': 0.05, 'engagement': 0.75},
    'Other': {'weight': 0.10, 'engagement': 0.85},
}

# Base conversion rates (will be modified by source/device quality)
BASE_RATES = {
    'welcome_email_sent': 0.98,      # Almost all users get welcome email
    'welcome_email_opened': 0.45,    # Open rate ~45%
    'email_clicked': 0.12,           # Click rate ~12%
    'trial_started': 0.35,           # 35% of clickers start trial
    'paid_conversion': 0.25,         # 25% of trial users convert
}

# A/B test variants
AB_VARIANTS = {
    'control': {'weight': 0.5, 'open_lift': 1.0, 'click_lift': 1.0},
    'variant_a': {'weight': 0.25, 'open_lift': 1.15, 'click_lift': 1.20},  # Better subject line
    'variant_b': {'weight': 0.25, 'open_lift': 0.95, 'click_lift': 1.30},  # Better CTA
}


def weighted_choice(options_dict):
    """Select an option based on weights."""
    options = list(options_dict.keys())
    weights = [options_dict[k]['weight'] for k in options]
    return np.random.choice(options, p=weights)


def generate_users():
    """Generate user data with signup information."""
    users = []
    
    for user_id in range(1, NUM_USERS + 1):
        # Random signup date
        days_range = (END_DATE - START_DATE).days
        signup_date = START_DATE + timedelta(days=random.randint(0, days_range))
        
        # Assign attributes
        source = weighted_choice(SOURCES)
        device = weighted_choice(DEVICES)
        country = weighted_choice(COUNTRIES)
        ab_variant = weighted_choice(AB_VARIANTS)
        
        # Calculate engagement multiplier
        engagement = (
            SOURCES[source]['quality'] * 
            DEVICES[device]['engagement'] * 
            COUNTRIES[country]['engagement']
        )
        
        users.append({
            'user_id': user_id,
            'signup_date': signup_date.strftime('%Y-%m-%d'),
            'signup_month': signup_date.strftime('%Y-%m'),
            'source': source,
            'device': device,
            'country': country,
            'ab_variant': ab_variant,
            'engagement_score': round(engagement, 3),
        })
    
    return pd.DataFrame(users)


def generate_email_events(users_df):
    """Generate email funnel events for each user."""
    events = []
    
    for _, user in users_df.iterrows():
        user_id = user['user_id']
        signup_date = datetime.strptime(user['signup_date'], '%Y-%m-%d')
        engagement = user['engagement_score']
        ab_variant = user['ab_variant']
        
        # Get A/B test lifts
        open_lift = AB_VARIANTS[ab_variant]['open_lift']
        click_lift = AB_VARIANTS[ab_variant]['click_lift']
        
        # Welcome email sent (almost always)
        if random.random() < BASE_RATES['welcome_email_sent']:
            sent_time = signup_date + timedelta(minutes=random.randint(1, 30))
            events.append({
                'user_id': user_id,
                'event_type': 'welcome_email_sent',
                'event_time': sent_time.strftime('%Y-%m-%d %H:%M:%S'),
                'email_type': 'welcome',
            })
            
            # Email opened
            open_rate = min(BASE_RATES['welcome_email_opened'] * engagement * open_lift, 0.95)
            if random.random() < open_rate:
                open_time = sent_time + timedelta(hours=random.randint(1, 48))
                events.append({
                    'user_id': user_id,
                    'event_type': 'email_opened',
                    'event_time': open_time.strftime('%Y-%m-%d %H:%M:%S'),
                    'email_type': 'welcome',
                })
                
                # Email clicked
                click_rate = min(BASE_RATES['email_clicked'] * engagement * click_lift, 0.5)
                if random.random() < click_rate:
                    click_time = open_time + timedelta(minutes=random.randint(1, 30))
                    events.append({
                        'user_id': user_id,
                        'event_type': 'email_clicked',
                        'event_time': click_time.strftime('%Y-%m-%d %H:%M:%S'),
                        'email_type': 'welcome',
                    })
        
        # Follow-up emails (for users who didn't convert yet)
        for i, email_type in enumerate(['followup_1', 'followup_2', 'promo'], start=1):
            if random.random() < 0.9:  # 90% get follow-ups
                followup_time = signup_date + timedelta(days=3*i + random.randint(0, 2))
                if followup_time <= END_DATE:
                    events.append({
                        'user_id': user_id,
                        'event_type': 'email_sent',
                        'event_time': followup_time.strftime('%Y-%m-%d %H:%M:%S'),
                        'email_type': email_type,
                    })
                    
                    # Decreasing open rates for follow-ups
                    followup_open_rate = (BASE_RATES['welcome_email_opened'] * 0.7 ** i) * engagement
                    if random.random() < followup_open_rate:
                        open_time = followup_time + timedelta(hours=random.randint(1, 72))
                        events.append({
                            'user_id': user_id,
                            'event_type': 'email_opened',
                            'event_time': open_time.strftime('%Y-%m-%d %H:%M:%S'),
                            'email_type': email_type,
                        })
    
    return pd.DataFrame(events)


def generate_conversions(users_df, events_df):
    """Generate conversion events (trial starts and paid conversions)."""
    conversions = []
    
    # Get users who clicked welcome email
    clickers = events_df[
        (events_df['event_type'] == 'email_clicked') & 
        (events_df['email_type'] == 'welcome')
    ]['user_id'].unique()
    
    for user_id in clickers:
        user = users_df[users_df['user_id'] == user_id].iloc[0]
        engagement = user['engagement_score']
        
        # Get click time
        click_event = events_df[
            (events_df['user_id'] == user_id) & 
            (events_df['event_type'] == 'email_clicked')
        ].iloc[0]
        click_time = datetime.strptime(click_event['event_time'], '%Y-%m-%d %H:%M:%S')
        
        # Trial started
        trial_rate = min(BASE_RATES['trial_started'] * engagement, 0.8)
        if random.random() < trial_rate:
            trial_time = click_time + timedelta(minutes=random.randint(5, 120))
            conversions.append({
                'user_id': user_id,
                'event_type': 'trial_started',
                'event_time': trial_time.strftime('%Y-%m-%d %H:%M:%S'),
                'plan': random.choice(['basic', 'pro', 'enterprise']),
            })
            
            # Paid conversion (after trial period)
            conversion_rate = min(BASE_RATES['paid_conversion'] * engagement, 0.6)
            if random.random() < conversion_rate:
                # Trial is 14 days
                conversion_time = trial_time + timedelta(days=14 + random.randint(0, 7))
                if conversion_time <= END_DATE + timedelta(days=30):
                    plan_weights = {'basic': 0.4, 'pro': 0.45, 'enterprise': 0.15}
                    plan = random.choices(
                        list(plan_weights.keys()), 
                        weights=list(plan_weights.values())
                    )[0]
                    
                    plan_prices = {'basic': 29, 'pro': 79, 'enterprise': 199}
                    
                    conversions.append({
                        'user_id': user_id,
                        'event_type': 'paid_conversion',
                        'event_time': conversion_time.strftime('%Y-%m-%d %H:%M:%S'),
                        'plan': plan,
                        'revenue': plan_prices[plan],
                    })
    
    return pd.DataFrame(conversions)


def main():
    """Generate all datasets and save to CSV."""
    print("Generating synthetic email funnel data...")
    
    # Generate users
    print("  - Generating users...")
    users_df = generate_users()
    
    # Generate email events
    print("  - Generating email events...")
    events_df = generate_email_events(users_df)
    
    # Generate conversions
    print("  - Generating conversions...")
    conversions_df = generate_conversions(users_df, events_df)
    
    # Save to CSV
    users_df.to_csv('users.csv', index=False)
    events_df.to_csv('email_events.csv', index=False)
    conversions_df.to_csv('conversions.csv', index=False)
    
    # Print summary
    print("\nData generation complete!")
    print(f"  - Users: {len(users_df):,}")
    print(f"  - Email events: {len(events_df):,}")
    print(f"  - Conversions: {len(conversions_df):,}")
    
    # Funnel summary
    total_users = len(users_df)
    emails_sent = events_df[events_df['event_type'] == 'welcome_email_sent']['user_id'].nunique()
    emails_opened = events_df[(events_df['event_type'] == 'email_opened') & (events_df['email_type'] == 'welcome')]['user_id'].nunique()
    emails_clicked = events_df[(events_df['event_type'] == 'email_clicked') & (events_df['email_type'] == 'welcome')]['user_id'].nunique()
    trials = conversions_df[conversions_df['event_type'] == 'trial_started']['user_id'].nunique()
    paid = conversions_df[conversions_df['event_type'] == 'paid_conversion']['user_id'].nunique()
    
    print("\nFunnel Summary:")
    print(f"  - Signups:        {total_users:,} (100%)")
    print(f"  - Emails sent:    {emails_sent:,} ({emails_sent/total_users*100:.1f}%)")
    print(f"  - Emails opened:  {emails_opened:,} ({emails_opened/total_users*100:.1f}%)")
    print(f"  - Emails clicked: {emails_clicked:,} ({emails_clicked/total_users*100:.1f}%)")
    print(f"  - Trials started: {trials:,} ({trials/total_users*100:.1f}%)")
    print(f"  - Paid users:     {paid:,} ({paid/total_users*100:.1f}%)")


if __name__ == '__main__':
    main()
