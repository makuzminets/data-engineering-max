-- Intermediate model: User risk signals
-- Aggregates user behavior patterns for risk assessment

with users as (
    select * from {{ ref('stg_users') }}
),

transactions as (
    select * from {{ ref('stg_transactions') }}
),

devices as (
    select * from {{ ref('stg_devices') }}
),

fraud_labels as (
    select * from {{ ref('stg_fraud_labels') }}
),

-- User transaction stats
user_txn_stats as (
    select
        user_id,
        count(*) as total_transactions,
        count(case when is_successful then 1 end) as successful_transactions,
        sum(amount) as total_amount,
        avg(amount) as avg_amount,
        max(amount) as max_amount,
        min(created_at) as first_transaction_at,
        max(created_at) as last_transaction_at,
        count(distinct country_code) as unique_countries,
        count(distinct device_id) as unique_devices
    from transactions
    group by user_id
),

-- User fraud history
user_fraud_stats as (
    select
        user_id,
        count(*) as fraud_count,
        count(case when is_confirmed then 1 end) as confirmed_fraud_count,
        max(detected_at) as last_fraud_at
    from fraud_labels
    group by user_id
),

-- Device risk signals
user_device_risk as (
    select
        user_id,
        count(*) as device_count,
        count(case when is_emulator then 1 end) as emulator_count,
        count(case when is_rooted then 1 end) as rooted_count
    from devices
    group by user_id
)

select
    u.user_id,
    u.account_status,
    u.is_verified,
    u.verification_level,
    u.signup_source,
    u.signup_country,
    u.registered_at,
    u.last_login_at,
    
    -- Account age
    date_diff(current_date(), date(u.registered_at), day) as account_age_days,
    
    -- Transaction behavior
    coalesce(ts.total_transactions, 0) as total_transactions,
    coalesce(ts.successful_transactions, 0) as successful_transactions,
    coalesce(ts.total_amount, 0) as total_amount,
    coalesce(ts.avg_amount, 0) as avg_amount,
    coalesce(ts.unique_countries, 0) as unique_countries,
    coalesce(ts.unique_devices, 0) as unique_devices,
    
    -- Fraud history
    coalesce(fs.fraud_count, 0) as fraud_count,
    coalesce(fs.confirmed_fraud_count, 0) as confirmed_fraud_count,
    fs.last_fraud_at,
    
    -- Device risk
    coalesce(dr.device_count, 0) as device_count,
    coalesce(dr.emulator_count, 0) as emulator_count,
    coalesce(dr.rooted_count, 0) as rooted_count,
    
    -- Risk flags
    case when fs.confirmed_fraud_count > 0 then true else false end as has_fraud_history,
    case when dr.emulator_count > 0 then true else false end as uses_emulator,
    case when ts.unique_countries > 5 then true else false end as multi_country_user,
    case when ts.unique_devices > 10 then true else false end as multi_device_user,
    case 
        when date_diff(current_date(), date(u.registered_at), day) < 7 
        then true else false 
    end as is_new_account,
    
    -- Risk score (simple rule-based)
    (
        case when fs.confirmed_fraud_count > 0 then 30 else 0 end +
        case when dr.emulator_count > 0 then 20 else 0 end +
        case when dr.rooted_count > 0 then 15 else 0 end +
        case when ts.unique_countries > 5 then 10 else 0 end +
        case when ts.unique_devices > 10 then 10 else 0 end +
        case when date_diff(current_date(), date(u.registered_at), day) < 7 then 15 else 0 end
    ) as risk_score

from users u
left join user_txn_stats ts on u.user_id = ts.user_id
left join user_fraud_stats fs on u.user_id = fs.user_id
left join user_device_risk dr on u.user_id = dr.user_id
