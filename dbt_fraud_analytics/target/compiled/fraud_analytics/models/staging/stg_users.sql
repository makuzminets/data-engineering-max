-- Staging model for raw users
-- Cleans and standardizes user data

with source as (
    select * from "analytics"."raw"."users"
),

renamed as (
    select
        -- IDs
        user_id,
        
        -- User info
        email,
        phone_hash,
        
        -- Account details
        account_status,
        is_verified,
        verification_level,
        
        -- Registration
        signup_source,
        signup_country,
        signup_device_type,
        
        -- Timestamps
        created_at as registered_at,
        last_login_at,
        
        -- Calculated fields
        current_timestamp() as _refreshed_at
        
    from source
    where user_id is not null
)

select * from renamed