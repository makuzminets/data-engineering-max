-- Fact table for chargebacks
-- Combines transaction and chargeback data for analysis

WITH chargebacks AS (
    SELECT * FROM {{ ref('stg_chargebacks') }}
),

transactions AS (
    SELECT * FROM {{ ref('stg_transactions') }}
),

enriched AS (
    SELECT
        c.chargeback_id,
        c.transaction_id,
        c.customer_id,
        
        -- Dates
        c.transaction_date,
        c.chargeback_date,
        c.chargeback_date_day,
        c.resolution_date,
        
        -- Time dimensions
        EXTRACT(YEAR FROM c.chargeback_date) AS chargeback_year,
        EXTRACT(MONTH FROM c.chargeback_date) AS chargeback_month,
        EXTRACT(WEEK FROM c.chargeback_date) AS chargeback_week,
        FORMAT_DATE('%Y-%m', c.chargeback_date) AS chargeback_year_month,
        
        -- Financials
        c.chargeback_amount,
        c.currency,
        
        -- Reason
        c.reason_code,
        c.reason_category,
        c.reason_description,
        c.is_fraud_chargeback,
        
        -- Context
        c.payment_method,
        c.country_code,
        c.plan_name,
        c.was_3ds_enabled,
        c.was_recurring,
        t.country_risk_level,
        t.device_type,
        
        -- Dispute
        c.dispute_filed,
        c.dispute_won,
        c.dispute_outcome,
        c.chargeback_status,
        
        -- Timing metrics
        c.days_to_chargeback,
        c.days_to_resolution,
        
        -- Financial impact
        CASE 
            WHEN c.dispute_won = TRUE THEN 0
            ELSE c.chargeback_amount
        END AS net_chargeback_loss,
        
        -- Recovery
        CASE 
            WHEN c.dispute_won = TRUE THEN c.chargeback_amount
            ELSE 0
        END AS recovered_amount
        
    FROM chargebacks c
    LEFT JOIN transactions t ON c.transaction_id = t.transaction_id
)

SELECT * FROM enriched
