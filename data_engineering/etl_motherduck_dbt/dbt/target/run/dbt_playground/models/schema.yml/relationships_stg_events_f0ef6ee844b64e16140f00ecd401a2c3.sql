select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    

with child as (
    select session_id as from_field
    from "my_db"."main"."stg_events"
    where session_id is not null
),

parent as (
    select session_id as to_field
    from "my_db"."main"."stg_sessions"
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null



      
    ) dbt_internal_test