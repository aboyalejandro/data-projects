select session_id::int as session_id,
       event_id::int as event_id,
       visitor_id,
       timestamp::timestamp as timestamp,
       event_index::int as event_index,
       page_view_index::int as page_view_index,
       custom_event_category,
       custom_event_action,
       custom_event_name,
       event_url,
       source_medium,
       campaign_name as utm_campaign,
       goal_id::int as goal_id,
       current_timestamp as date_transformed
from {{ref('stg_events')}}