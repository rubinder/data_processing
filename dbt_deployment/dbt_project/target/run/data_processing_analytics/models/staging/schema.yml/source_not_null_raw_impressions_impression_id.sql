
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select impression_id
from "data_processing"."raw"."impressions"
where impression_id is null



  
  
      
    ) dbt_internal_test