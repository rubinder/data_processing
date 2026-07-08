
    
    

with all_values as (

    select
        event_type as value_field,
        count(*) as n_records

    from "data_processing"."raw"."impressions"
    group by event_type

)

select *
from all_values
where value_field not in (
    'a','b','c','d','e','f'
)


