-- Daily chargeback summary for dashboard
-- Aggregates key metrics by day

WITH chargebacks AS (
    SELECT * FROM {{ ref('fct_chargebacks') }}
),

transactions AS (
    SELECT * FROM {{ ref('stg_transactions') }}
),

daily_transactions AS (
    SELECT
        transaction_date_day AS date,
        COUNT(*) AS total_transactions,
        SUM(amount) AS total_revenue,
        AVG(CASE WHEN is_3ds_enabled THEN 1.0 ELSE 0.0 END) AS pct_3ds,
        COUNT(DISTINCT customer_id) AS unique_customers
    FROM transactions
    GROUP BY 1
),

daily_chargebacks AS (
    SELECT
        chargeback_date_day AS date,
        COUNT(*) AS total_chargebacks,
        SUM(chargeback_amount) AS chargeback_amount,
        SUM(net_chargeback_loss) AS net_loss,
        SUM(recovered_amount) AS recovered,
        
        -- By category
        SUM(CASE WHEN reason_category = 'fraud' THEN 1 ELSE 0 END) AS fraud_chargebacks,
        SUM(CASE WHEN reason_category = 'service' THEN 1 ELSE 0 END) AS service_chargebacks,
        SUM(CASE WHEN reason_category = 'authorization' THEN 1 ELSE 0 END) AS auth_chargebacks,
        SUM(CASE WHEN reason_category = 'processing' THEN 1 ELSE 0 END) AS processing_chargebacks,
        
        -- Disputes
        SUM(CASE WHEN dispute_filed THEN 1 ELSE 0 END) AS disputes_filed,
        SUM(CASE WHEN dispute_won = TRUE THEN 1 ELSE 0 END) AS disputes_won,
        SUM(CASE WHEN dispute_won = FALSE THEN 1 ELSE 0 END) AS disputes_lost,
        
        -- Averages
        AVG(days_to_chargeback) AS avg_days_to_chargeback,
        AVG(chargeback_amount) AS avg_chargeback_amount
        
    FROM chargebacks
    GROUP BY 1
),

combined AS (
    SELECT
        t.date,
        t.total_transactions,
        t.total_revenue,
        t.pct_3ds,
        t.unique_customers,
        
        COALESCE(c.total_chargebacks, 0) AS total_chargebacks,
        COALESCE(c.chargeback_amount, 0) AS chargeback_amount,
        COALESCE(c.net_loss, 0) AS net_loss,
        COALESCE(c.recovered, 0) AS recovered,
        
        COALESCE(c.fraud_chargebacks, 0) AS fraud_chargebacks,
        COALESCE(c.service_chargebacks, 0) AS service_chargebacks,
        COALESCE(c.auth_chargebacks, 0) AS auth_chargebacks,
        COALESCE(c.processing_chargebacks, 0) AS processing_chargebacks,
        
        COALESCE(c.disputes_filed, 0) AS disputes_filed,
        COALESCE(c.disputes_won, 0) AS disputes_won,
        COALESCE(c.disputes_lost, 0) AS disputes_lost,
        
        c.avg_days_to_chargeback,
        c.avg_chargeback_amount,
        
        -- Rates
        SAFE_DIVIDE(COALESCE(c.total_chargebacks, 0), t.total_transactions) AS chargeback_rate,
        SAFE_DIVIDE(COALESCE(c.chargeback_amount, 0), t.total_revenue) AS chargeback_amount_rate,
        SAFE_DIVIDE(COALESCE(c.disputes_won, 0), NULLIF(c.disputes_filed, 0)) AS dispute_win_rate,
        SAFE_DIVIDE(COALESCE(c.fraud_chargebacks, 0), NULLIF(c.total_chargebacks, 0)) AS fraud_chargeback_pct
        
    FROM daily_transactions t
    LEFT JOIN daily_chargebacks c ON t.date = c.date
)

SELECT 
    *,
    -- Rolling averages
    AVG(chargeback_rate) OVER (ORDER BY date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS chargeback_rate_7d_avg,
    AVG(chargeback_rate) OVER (ORDER BY date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) AS chargeback_rate_30d_avg,
    SUM(total_chargebacks) OVER (ORDER BY date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS chargebacks_7d,
    SUM(net_loss) OVER (ORDER BY date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) AS net_loss_30d
    
FROM combined
ORDER BY date
