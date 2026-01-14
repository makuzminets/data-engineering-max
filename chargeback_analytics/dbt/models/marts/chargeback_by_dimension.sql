-- Chargeback analysis by various dimensions
-- For drill-down analysis in dashboards

WITH chargebacks AS (
    SELECT * FROM {{ ref('fct_chargebacks') }}
),

transactions AS (
    SELECT * FROM {{ ref('stg_transactions') }}
),

-- By Country
by_country AS (
    SELECT
        'country' AS dimension,
        country_code AS dimension_value,
        COUNT(*) AS total_chargebacks,
        SUM(chargeback_amount) AS total_amount,
        SUM(net_chargeback_loss) AS net_loss,
        AVG(CASE WHEN is_fraud_chargeback = 1 THEN 1.0 ELSE 0.0 END) AS fraud_pct,
        AVG(CASE WHEN dispute_won = TRUE THEN 1.0 ELSE 0.0 END) AS dispute_win_rate
    FROM chargebacks
    GROUP BY 1, 2
),

-- By Payment Method
by_payment AS (
    SELECT
        'payment_method' AS dimension,
        payment_method AS dimension_value,
        COUNT(*) AS total_chargebacks,
        SUM(chargeback_amount) AS total_amount,
        SUM(net_chargeback_loss) AS net_loss,
        AVG(CASE WHEN is_fraud_chargeback = 1 THEN 1.0 ELSE 0.0 END) AS fraud_pct,
        AVG(CASE WHEN dispute_won = TRUE THEN 1.0 ELSE 0.0 END) AS dispute_win_rate
    FROM chargebacks
    GROUP BY 1, 2
),

-- By Plan
by_plan AS (
    SELECT
        'plan' AS dimension,
        plan_name AS dimension_value,
        COUNT(*) AS total_chargebacks,
        SUM(chargeback_amount) AS total_amount,
        SUM(net_chargeback_loss) AS net_loss,
        AVG(CASE WHEN is_fraud_chargeback = 1 THEN 1.0 ELSE 0.0 END) AS fraud_pct,
        AVG(CASE WHEN dispute_won = TRUE THEN 1.0 ELSE 0.0 END) AS dispute_win_rate
    FROM chargebacks
    GROUP BY 1, 2
),

-- By Reason Category
by_reason AS (
    SELECT
        'reason_category' AS dimension,
        reason_category AS dimension_value,
        COUNT(*) AS total_chargebacks,
        SUM(chargeback_amount) AS total_amount,
        SUM(net_chargeback_loss) AS net_loss,
        AVG(CASE WHEN is_fraud_chargeback = 1 THEN 1.0 ELSE 0.0 END) AS fraud_pct,
        AVG(CASE WHEN dispute_won = TRUE THEN 1.0 ELSE 0.0 END) AS dispute_win_rate
    FROM chargebacks
    GROUP BY 1, 2
),

-- By 3DS Status
by_3ds AS (
    SELECT
        '3ds_enabled' AS dimension,
        CAST(was_3ds_enabled AS STRING) AS dimension_value,
        COUNT(*) AS total_chargebacks,
        SUM(chargeback_amount) AS total_amount,
        SUM(net_chargeback_loss) AS net_loss,
        AVG(CASE WHEN is_fraud_chargeback = 1 THEN 1.0 ELSE 0.0 END) AS fraud_pct,
        AVG(CASE WHEN dispute_won = TRUE THEN 1.0 ELSE 0.0 END) AS dispute_win_rate
    FROM chargebacks
    GROUP BY 1, 2
),

-- By Risk Level
by_risk AS (
    SELECT
        'risk_level' AS dimension,
        country_risk_level AS dimension_value,
        COUNT(*) AS total_chargebacks,
        SUM(chargeback_amount) AS total_amount,
        SUM(net_chargeback_loss) AS net_loss,
        AVG(CASE WHEN is_fraud_chargeback = 1 THEN 1.0 ELSE 0.0 END) AS fraud_pct,
        AVG(CASE WHEN dispute_won = TRUE THEN 1.0 ELSE 0.0 END) AS dispute_win_rate
    FROM chargebacks
    GROUP BY 1, 2
),

combined AS (
    SELECT * FROM by_country
    UNION ALL
    SELECT * FROM by_payment
    UNION ALL
    SELECT * FROM by_plan
    UNION ALL
    SELECT * FROM by_reason
    UNION ALL
    SELECT * FROM by_3ds
    UNION ALL
    SELECT * FROM by_risk
)

SELECT 
    dimension,
    dimension_value,
    total_chargebacks,
    total_amount,
    net_loss,
    ROUND(fraud_pct * 100, 1) AS fraud_pct,
    ROUND(dispute_win_rate * 100, 1) AS dispute_win_rate_pct
FROM combined
ORDER BY dimension, total_chargebacks DESC
