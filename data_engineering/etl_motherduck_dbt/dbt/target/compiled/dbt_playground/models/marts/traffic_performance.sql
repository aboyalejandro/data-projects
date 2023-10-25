select timestamp__to_date::date as date,
       referrer_type as channel,
       source_medium,
       campaign_name as utm_campaign,
       campaign_content as utm_content,
       session_entry_url as landing_page,
       
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
       operating_system,
       device_type,
       events::int as events,
       visitors::int as users,
       sessions::int as sessions,
       page_views::int as page_views,
       ecommerce_conversions::int as purchases,
       cart_additions::int as add_to_cart,
       ecommerce_abandoned_carts::int as abandoned_carts,
       case when consents_none = 0 then false else true end as is_full_consent,
       current_timestamp as date_transformed
from "my_db"."main"."stg_analytics"