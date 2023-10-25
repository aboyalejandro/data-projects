select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    

with all_values as (

    select
        visitor_returning as value_field,
        count(*) as n_records

    from "my_db"."main"."stg_analytics"
    group by visitor_returning

)

select *
from all_values
where value_field not in (
    'Returning'
)



      
    ) dbt_internal_test