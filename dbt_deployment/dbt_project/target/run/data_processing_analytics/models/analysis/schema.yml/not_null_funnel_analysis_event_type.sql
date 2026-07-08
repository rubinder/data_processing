
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select event_type
from "data_processing"."public_analytics"."funnel_analysis"
where event_type is null



  
  
      
    ) dbt_internal_test