-- Intermediate model: Transaction features for fraud detection
-- Calculates velocity, patterns, and risk signals per transaction

with transactions as (
    select * from {{ ref('stg_transactions') }}
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
