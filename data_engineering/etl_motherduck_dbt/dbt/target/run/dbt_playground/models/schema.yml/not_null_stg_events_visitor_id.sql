select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select visitor_id
from "my_db"."main"."stg_events"
where visitor_id is null



      
    ) dbt_internal_test