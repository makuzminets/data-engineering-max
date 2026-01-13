-- Dimension table: Users with risk profile
-- User attributes and aggregated risk signals

with user_risk as (
    select * from {{ ref('int_user_risk_signals') }}
)

select
    -- User identifiers
    user_id,
    
    -- Account info
    account_status,
    is_verified,
    verification_level,
    signup_source,
    signup_country,
    
    -- Timestamps
    registered_at,
    last_login_at,
    account_age_days,
    
    -- Transaction behavior
    total_transactions,
    successful_transactions,
    total_amount,
    avg_amount,
    unique_countries,
    unique_devices,
    
    -- Fraud history
    fraud_count,
    confirmed_fraud_count,
    last_fraud_at,
    has_fraud_history,
    
    -- Device risk
    device_count,
    emulator_count,
    rooted_count,
    uses_emulator,
    
    -- Risk flags
    multi_country_user,
    multi_device_user,
    is_new_account,
    
    -- Risk score
    risk_score,
    
    -- Risk tier
    case
        when risk_score >= 50 then 'high'
        when risk_score >= 25 then 'medium'
        else 'low'
    end as risk_tier,
    
    -- User segment
    case
        when confirmed_fraud_count > 0 then 'fraudster'
        when risk_score >= 50 then 'high_risk'
        when is_new_account and risk_score >= 25 then 'suspicious_new'
        when total_transactions > 100 and fraud_count = 0 then 'trusted'
        else 'standard'
    end as user_segment

from user_risk
