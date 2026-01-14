
    
    

with all_values as (

    select
        risk_tier as value_field,
        count(*) as n_records

    from "memory"."main_marts"."dim_users"
    group by risk_tier

)

select *
from all_values
where value_field not in (
    'low','medium','high'
)


