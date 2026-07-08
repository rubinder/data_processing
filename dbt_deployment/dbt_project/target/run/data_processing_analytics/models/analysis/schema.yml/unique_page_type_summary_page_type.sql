
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

select
    page_type as unique_field,
    count(*) as n_records

from "data_processing"."public_analytics"."page_type_summary"
where page_type is not null
group by page_type
having count(*) > 1



  
  
      
    ) dbt_internal_test