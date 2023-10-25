
    
    

with child as (
    select visitor_id as from_field
    from "my_db"."main"."stg_events"
    where visitor_id is not null
),

parent as (
    select visitor_id as to_field
    from "my_db"."main"."stg_sessions"
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null


