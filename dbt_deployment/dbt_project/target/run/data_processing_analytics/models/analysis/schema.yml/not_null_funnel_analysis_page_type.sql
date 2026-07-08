
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select page_type
from "data_processing"."public_analytics"."funnel_analysis"
where page_type is null



  
  
      
    ) dbt_internal_test