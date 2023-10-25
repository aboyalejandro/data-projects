select session_id,
       visitor_id,
       timestamp::timestamp as timestamp,
       visitor_session_number::int as visitor_session_number,
       
    case when visitor_returning = 'Returning' then true else false end
 as is_returning,
       
    case 
        when location_country_name = 'United Stated' then location_country_name
        when location_country_name = 'Netherlands' then location_country_name
        when location_country_name = 'Ireland' then location_country_name
        when location_country_name = 'Canada' then location_country_name
        when location_country_name = 'Belgium' then location_country_name
        when location_country_name = 'China' then location_country_name
        else 'Other'
    end
 as country,
       source_medium,
       campaign_name as utm_campaign,
       session_total_page_views::int as session_total_page_views,
       session_total_events::int as session_total_events,
       visitor_days_since_last_session::int as visitor_days_since_last_session,
       visitor_days_since_first_session::int as visitor_days_since_first_session,
       current_timestamp as date_transformed
from "my_db"."main"."stg_sessions"