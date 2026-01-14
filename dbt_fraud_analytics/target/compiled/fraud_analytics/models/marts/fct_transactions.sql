-- Fact table: Transactions with fraud features and risk scores
-- Final table for analytics and BI

with  __dbt__cte__int_transaction_features as (
-- Intermediate model: Transaction features for fraud detection
-- Calculates velocity, patterns, and risk signals per transaction

with transactions as (
    select * from "memory"."main_staging"."stg_transactions"
),

-- Transaction velocity: count transactions in time windows
velocity as (
    select
        transaction_id,
        user_id,
        created_at,
        
        -- Transactions in last 1 hour
        count(*) over (
            partition by user_id 
            order by created_at 
            range between interval 1 hour preceding and current row
        ) as txn_count_1h,
        
        -- Transactions in last 24 hours
        count(*) over (
            partition by user_id 
            order by created_at 
            range between interval 24 hour preceding and current row
        ) as txn_count_24h,
        
        -- Sum amount in last 24 hours
        sum(amount) over (
            partition by user_id 
            order by created_at 
            range between interval 24 hour preceding and current row
        ) as amount_sum_24h,
        
        -- Average amount for user (all time)
        avg(amount) over (partition by user_id) as user_avg_amount
        
    from transactions
),

-- Time-based features
time_features as (
    select
        transaction_id,
        extract(hour from created_at) as hour_of_day,
        extract(dayofweek from created_at) as day_of_week,
        
        -- Is night transaction (high risk hours)
        case 
            when extract(hour from created_at) between 0 and 5 then true
            else false
        end as is_night_transaction,
        
        -- Is weekend
        case 
            when extract(dayofweek from created_at) in (1, 7) then true
            else false
        end as is_weekend
        
    from transactions
),

-- Amount anomaly features
amount_features as (
    select
        t.transaction_id,
        t.amount,
        
        -- Is round amount (suspicious pattern)
        case 
            when mod(t.amount, 100) = 0 then true
            else false
        end as is_round_amount,
        
        -- Amount deviation from user average
        case 
            when v.user_avg_amount > 0 
            then (t.amount - v.user_avg_amount) / v.user_avg_amount
            else 0
        end as amount_deviation_ratio
        
    from transactions t
    left join velocity v on t.transaction_id = v.transaction_id
)

select
    t.transaction_id,
    t.user_id,
    t.device_id,
    t.amount,
    t.currency,
    t.payment_method,
    t.country_code,
    t.created_at,
    
    -- Velocity features
    v.txn_count_1h,
    v.txn_count_24h,
    v.amount_sum_24h,
    v.user_avg_amount,
    
    -- Time features
    tf.hour_of_day,
    tf.day_of_week,
    tf.is_night_transaction,
    tf.is_weekend,
    
    -- Amount features
    af.is_round_amount,
    af.amount_deviation_ratio,
    
    -- Risk signals
    case when v.txn_count_1h > 5 then true else false end as high_velocity_1h,
    case when v.txn_count_24h > 20 then true else false end as high_velocity_24h,
    case when af.amount_deviation_ratio > 3 then true else false end as unusual_amount

from transactions t
left join velocity v on t.transaction_id = v.transaction_id
left join time_features tf on t.transaction_id = tf.transaction_id
left join amount_features af on t.transaction_id = af.transaction_id
),  __dbt__cte__int_user_risk_signals as (
-- Intermediate model: User risk signals
-- Aggregates user behavior patterns for risk assessment

with users as (
    select * from "memory"."main_staging"."stg_users"
),

transactions as (
    select * from "memory"."main_staging"."stg_transactions"
),

devices as (
    select * from "memory"."main_staging"."stg_devices"
),

fraud_labels as (
    select * from "memory"."main_staging"."stg_fraud_labels"
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
), transaction_features as (
    select * from __dbt__cte__int_transaction_features
),

user_risk as (
    select * from __dbt__cte__int_user_risk_signals
),

fraud_labels as (
    select * from "memory"."main_staging"."stg_fraud_labels"
)

select
    -- Transaction identifiers
    tf.transaction_id,
    tf.user_id,
    tf.device_id,
    
    -- Transaction details
    tf.amount,
    tf.currency,
    tf.payment_method,
    tf.country_code,
    tf.created_at,
    
    -- Transaction features
    tf.txn_count_1h,
    tf.txn_count_24h,
    tf.amount_sum_24h,
    tf.hour_of_day,
    tf.is_night_transaction,
    tf.is_weekend,
    tf.is_round_amount,
    tf.amount_deviation_ratio,
    
    -- Transaction risk flags
    tf.high_velocity_1h,
    tf.high_velocity_24h,
    tf.unusual_amount,
    
    -- User risk context
    ur.account_age_days,
    ur.total_transactions as user_total_transactions,
    ur.fraud_count as user_fraud_count,
    ur.risk_score as user_risk_score,
    ur.has_fraud_history,
    ur.is_new_account,
    
    -- Fraud outcome (if known)
    case when fl.fraud_event_id is not null then true else false end as is_fraud,
    fl.fraud_type,
    fl.fraud_source,
    fl.confidence_score as fraud_confidence,
    
    -- Combined risk score
    (
        ur.risk_score +
        case when tf.high_velocity_1h then 15 else 0 end +
        case when tf.high_velocity_24h then 10 else 0 end +
        case when tf.unusual_amount then 20 else 0 end +
        case when tf.is_night_transaction then 5 else 0 end
    ) as transaction_risk_score,
    
    -- Risk tier
    case
        when (ur.risk_score + 
              case when tf.high_velocity_1h then 15 else 0 end +
              case when tf.unusual_amount then 20 else 0 end) >= 50 then 'high'
        when (ur.risk_score + 
              case when tf.high_velocity_1h then 15 else 0 end +
              case when tf.unusual_amount then 20 else 0 end) >= 25 then 'medium'
        else 'low'
    end as risk_tier

from transaction_features tf
left join user_risk ur on tf.user_id = ur.user_id
left join fraud_labels fl on tf.transaction_id = fl.transaction_id