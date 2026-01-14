-- Dimension table: Devices with trust scores
-- Device fingerprints and risk attributes

with devices as (
    select * from "memory"."main_staging"."stg_devices"
),

transactions as (
    select * from "memory"."main_staging"."stg_transactions"
),

fraud_labels as (
    select * from "memory"."main_staging"."stg_fraud_labels"
),

-- Device transaction stats
device_txn_stats as (
    select
        device_id,
        count(*) as transaction_count,
        count(distinct user_id) as unique_users,
        sum(amount) as total_amount,
        min(created_at) as first_transaction_at,
        max(created_at) as last_transaction_at
    from transactions
    group by device_id
),

-- Device fraud stats
device_fraud_stats as (
    select
        t.device_id,
        count(fl.fraud_event_id) as fraud_count
    from transactions t
    inner join fraud_labels fl on t.transaction_id = fl.transaction_id
    where fl.is_confirmed = true
    group by t.device_id
)

select
    -- Device identifiers
    d.device_id,
    d.device_fingerprint,
    
    -- Device info
    d.device_type,
    d.os_name,
    d.os_version,
    d.browser_name,
    d.browser_version,
    
    -- Risk attributes
    d.is_emulator,
    d.is_rooted,
    d.timezone,
    d.language,
    
    -- Timestamps
    d.first_seen_at,
    d.last_seen_at,
    
    -- Usage stats
    coalesce(ts.transaction_count, 0) as transaction_count,
    coalesce(ts.unique_users, 0) as unique_users,
    coalesce(ts.total_amount, 0) as total_amount,
    ts.first_transaction_at,
    ts.last_transaction_at,
    
    -- Fraud stats
    coalesce(fs.fraud_count, 0) as fraud_count,
    
    -- Risk flags
    case when ts.unique_users > 3 then true else false end as shared_device,
    case when fs.fraud_count > 0 then true else false end as has_fraud_history,
    
    -- Device trust score (0-100, higher = more trusted)
    greatest(0, least(100,
        100 -
        case when d.is_emulator then 40 else 0 end -
        case when d.is_rooted then 30 else 0 end -
        case when ts.unique_users > 3 then 20 else 0 end -
        coalesce(fs.fraud_count, 0) * 10
    )) as trust_score,
    
    -- Trust tier
    case
        when d.is_emulator or fs.fraud_count > 0 then 'untrusted'
        when d.is_rooted or ts.unique_users > 3 then 'suspicious'
        when ts.transaction_count > 10 and coalesce(fs.fraud_count, 0) = 0 then 'trusted'
        else 'unknown'
    end as trust_tier

from devices d
left join device_txn_stats ts on d.device_id = ts.device_id
left join device_fraud_stats fs on d.device_id = fs.device_id