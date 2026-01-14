
    
    

with all_values as (

    select
        user_segment as value_field,
        count(*) as n_records

    from "memory"."main_marts"."dim_users"
    group by user_segment

)

select *
from all_values
where value_field not in (
    'fraudster','high_risk','suspicious_new','trusted','standard'
)


