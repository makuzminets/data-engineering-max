
    
    

select
    fraud_event_id as unique_field,
    count(*) as n_records

from "memory"."main_staging"."stg_fraud_labels"
where fraud_event_id is not null
group by fraud_event_id
having count(*) > 1


