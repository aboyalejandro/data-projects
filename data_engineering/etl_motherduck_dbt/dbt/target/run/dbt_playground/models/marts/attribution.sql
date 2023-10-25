
  
    
    

    create  table
      "my_db"."main"."attribution__dbt_tmp"
  
    as (
      select md5(cast(coalesce(cast(sessions.visitor_id as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(sessions.session_id as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(events.event_id as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(events.timestamp as TEXT), '_dbt_utils_surrogate_key_null_') as TEXT)) as s_id,
       sessions.visitor_id,
       sessions.session_id,
       sessions.visitor_session_number,
       case when sessions.visitor_session_number = 1 then true else false end as is_first_session,
       events.event_id,
       events.page_view_index,
       events.timestamp,
       events.event_url,
       sessions.is_returning,
       sessions.country,
       sessions.source_medium,
       sessions.utm_campaign,
       sessions.session_total_page_views,
       sessions.session_total_events,
       current_timestamp as date_transformed
from "my_db"."main"."int_sessions" as sessions
left join "my_db"."main"."int_events" as events
on sessions.session_id = events.session_id
    );
  
  