-- Staging model for fraud labels
-- Ground truth data for fraud detection

with source as (
    select * from "analytics"."raw"."fraud_labels"
),

renamed as (
    select
        -- IDs
        fraud_event_id,
        transaction_id,
        user_id,
        
        -- Fraud details
        fraud_type,
        fraud_source,  -- 'chargeback', 'manual_review', 'ml_model', 'rule'
        confidence_score,
        
        -- Resolution
        is_confirmed,
        reviewed_by,
        
        -- Timestamps
        detected_at,
        confirmed_at,
        
        -- Notes
        notes
        
    from source
)

select * from renamed