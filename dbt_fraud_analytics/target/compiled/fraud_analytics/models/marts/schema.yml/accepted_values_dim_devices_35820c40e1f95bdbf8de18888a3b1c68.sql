
    
    

with all_values as (

    select
        trust_tier as value_field,
        count(*) as n_records

    from "memory"."main_marts"."dim_devices"
    group by trust_tier

)

select *
from all_values
where value_field not in (
    'trusted','suspicious','untrusted','unknown'
)


