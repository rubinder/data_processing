
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select hour
from "data_processing"."raw"."impressions"
where hour is null



  
  
      
    ) dbt_internal_test