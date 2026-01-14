-- Staging model for raw transactions
-- Cleans and standardizes transaction data

with source as (
    select * from "analytics"."raw"."transactions"
),

renamed as (
    select
        -- IDs
        transaction_id,
        user_id,
        device_id,
        
        -- Transaction details
        amount,
        currency,
        transaction_type,
        payment_method,
        
        -- Status
        status,
        is_successful,
        
        -- Location
        ip_address,
        country_code,
        city,
        
        -- Timestamps
        created_at,
        processed_at,
        
        -- Metadata
        _loaded_at
        
    from source
    where transaction_id is not null
)

select * from renamed