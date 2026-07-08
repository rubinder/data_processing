
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with all_values as (

    select
        page_type as value_field,
        count(*) as n_records

    from "data_processing"."raw"."impressions"
    group by page_type

)

select *
from all_values
where value_field not in (
    '1','2','3'
)



  
  
      
    ) dbt_internal_test