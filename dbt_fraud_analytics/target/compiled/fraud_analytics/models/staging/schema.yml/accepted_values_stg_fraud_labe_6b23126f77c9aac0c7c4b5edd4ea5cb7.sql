
    
    

with all_values as (

    select
        fraud_type as value_field,
        count(*) as n_records

    from "memory"."main_staging"."stg_fraud_labels"
    group by fraud_type

)

select *
from all_values
where value_field not in (
    'chargeback','account_takeover','synthetic_identity','friendly_fraud','card_testing'
)


