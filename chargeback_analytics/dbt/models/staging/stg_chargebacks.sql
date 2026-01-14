-- Staging model for chargebacks
-- Cleans and enriches chargeback data

WITH source AS (
    SELECT * FROM {{ source('raw', 'chargebacks') }}
),

cleaned AS (
    SELECT
        chargeback_id,
        transaction_id,
        customer_id,
        
        -- Dates
        CAST(transaction_date AS TIMESTAMP) AS transaction_date,
        CAST(chargeback_date AS TIMESTAMP) AS chargeback_date,
        DATE(chargeback_date) AS chargeback_date_day,
        CAST(resolution_date AS TIMESTAMP) AS resolution_date,
        
        -- Financials
        CAST(amount AS NUMERIC) AS chargeback_amount,
        UPPER(currency) AS currency,
        
        -- Reason details
        reason_code,
        LOWER(reason_category) AS reason_category,
        reason_description,
        
        -- Transaction context
        LOWER(payment_method) AS payment_method,
        UPPER(country) AS country_code,
        LOWER(plan) AS plan_name,
        COALESCE(is_3ds, FALSE) AS was_3ds_enabled,
        COALESCE(is_recurring, FALSE) AS was_recurring,
        
        -- Dispute info
        COALESCE(dispute_filed, FALSE) AS dispute_filed,
        dispute_won,
        LOWER(status) AS chargeback_status,
        
        -- Derived metrics
        DATE_DIFF(chargeback_date, transaction_date, DAY) AS days_to_chargeback,
        DATE_DIFF(resolution_date, chargeback_date, DAY) AS days_to_resolution,
        
        -- Categorization
        CASE 
            WHEN reason_category = 'fraud' THEN 1
            ELSE 0
        END AS is_fraud_chargeback,
        
        CASE 
            WHEN dispute_won = TRUE THEN 'won'
            WHEN dispute_won = FALSE THEN 'lost'
            WHEN dispute_filed = TRUE THEN 'pending'
            ELSE 'not_disputed'
        END AS dispute_outcome
        
    FROM source
)

SELECT * FROM cleaned
