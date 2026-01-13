-- Fact table: Transactions with fraud features and risk scores
-- Final table for analytics and BI

with transaction_features as (
    select * from {{ ref('int_transaction_features') }}
),

user_risk as (
    select * from {{ ref('int_user_risk_signals') }}
),

fraud_labels as (
    select * from {{ ref('stg_fraud_labels') }}
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
