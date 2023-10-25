select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    

with all_values as (

    select
        referrer_type as value_field,
        count(*) as n_records

    from "my_db"."main"."stg_analytics"
    group by referrer_type

)

select *
from all_values
where value_field not in (
    'Social','Direct entry','Website','Search engine','Campaign'
)



      
    ) dbt_internal_test