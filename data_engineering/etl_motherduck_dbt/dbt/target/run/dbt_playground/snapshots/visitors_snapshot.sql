
      
  
    
    

    create  table
      "analytics"."dbt"."visitors_snapshot"
  
    as (
      

    select *,
        md5(coalesce(cast(visitor_id as varchar ), '')
         || '|' || coalesce(cast(now()::timestamp as varchar ), '')
        ) as dbt_scd_id,
        now()::timestamp as dbt_updated_at,
        now()::timestamp as dbt_valid_from,
        nullif(now()::timestamp, now()::timestamp) as dbt_valid_to
    from (
        



select * from "my_db"."main"."visitors" 

    ) sbq



    );
  
  
  