-- Staging model for transactions
-- Cleans and standardizes raw transaction data

WITH source AS (
    SELECT * FROM {{ source('raw', 'transactions') }}
),

cleaned AS (
    SELECT
        transaction_id,
        customer_id,
        CAST(transaction_date AS TIMESTAMP) AS transaction_date,
        DATE(transaction_date) AS transaction_date_day,
        CAST(amount AS NUMERIC) AS amount,
        UPPER(currency) AS currency,
        LOWER(plan) AS plan_name,
        LOWER(payment_method) AS payment_method,
        UPPER(country) AS country_code,
        COALESCE(is_3ds, FALSE) AS is_3ds_enabled,
        COALESCE(is_recurring, FALSE) AS is_recurring,
        LOWER(device_type) AS device_type,
        COALESCE(is_chargeback, FALSE) AS has_chargeback,
        
        -- Derived fields
        EXTRACT(HOUR FROM transaction_date) AS transaction_hour,
        EXTRACT(DAYOFWEEK FROM transaction_date) AS transaction_day_of_week,
        EXTRACT(MONTH FROM transaction_date) AS transaction_month,
        
        -- Risk indicators
        CASE 
            WHEN country IN ('NG', 'PK', 'RU') THEN 'high'
            WHEN country IN ('BR', 'IN') THEN 'medium'
            ELSE 'low'
        END AS country_risk_level
        
    FROM source
)

SELECT * FROM cleaned
