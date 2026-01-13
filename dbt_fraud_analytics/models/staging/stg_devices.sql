-- Staging model for raw devices
-- Device fingerprints and attributes

with source as (
    select * from {{ source('raw', 'devices') }}
),

renamed as (
    select
        -- IDs
        device_id,
        user_id,
        
        -- Device info
        device_type,
        os_name,
        os_version,
        browser_name,
        browser_version,
        
        -- Fingerprint
        device_fingerprint,
        is_emulator,
        is_rooted,
        
        -- Location
        timezone,
        language,
        
        -- Timestamps
        first_seen_at,
        last_seen_at
        
    from source
    where device_id is not null
)

select * from renamed
