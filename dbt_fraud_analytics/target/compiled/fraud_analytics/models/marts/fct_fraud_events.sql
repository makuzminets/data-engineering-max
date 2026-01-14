-- Fact table: Fraud events for analysis
-- Confirmed and detected fraud cases

with fraud_labels as (
    select * from "memory"."main_staging"."stg_fraud_labels"
),

transactions as (
    select * from "memory"."main_staging"."stg_transactions"
),

users as (
    select * from "memory"."main_staging"."stg_users"
)

select
    -- Fraud event identifiers
    fl.fraud_event_id,
    fl.transaction_id,
    fl.user_id,
    
    -- Fraud details
    fl.fraud_type,
    fl.fraud_source,
    fl.confidence_score,
    fl.is_confirmed,
    fl.reviewed_by,
    fl.notes,
    
    -- Timestamps
    fl.detected_at,
    fl.confirmed_at,
    
    -- Detection latency
    timestamp_diff(fl.detected_at, t.created_at, hour) as detection_latency_hours,
    
    -- Transaction context
    t.amount as transaction_amount,
    t.currency,
    t.payment_method,
    t.country_code,
    t.created_at as transaction_at,
    
    -- User context
    u.account_status,
    u.is_verified,
    u.signup_source,
    u.registered_at as user_registered_at,
    date_diff(date(t.created_at), date(u.registered_at), day) as account_age_at_fraud,
    
    -- Time dimensions
    date(fl.detected_at) as detected_date,
    extract(month from fl.detected_at) as detected_month,
    extract(year from fl.detected_at) as detected_year

from fraud_labels fl
left join transactions t on fl.transaction_id = t.transaction_id
left join users u on fl.user_id = u.user_id