-- Pending to add Airflow DAG source

{{
    config
    (
        materialized='incremental',
        unique_key = ['id'],
        on_schema_change = 'sync_all_columns'
    )

}} 

select
    "timestamp__to_date" as date , 
    "session_entry_url" as landing_page, 
    "location_country_name" as country,
    "device_type" as device_category,
    "source_medium" as source_medium, 
    "campaign_name" as campaign, 
    "referrer_type" as channel_grouping,
    "visitors"::int as users,
    "sessions"::int as sessions,
    "goal_conversions__b6b9173b-47a1-446a-8ea6-1bb96249eb37"::int as leads,
     {{ dbt_utils.generate_surrogate_key(['date', 'country','channel_grouping','device_category','landing_page','source_medium','campaign']) }} as id,
    current_timestamp as date_inserted
from  {{ source('piwik_pro', 'analytics') }}
{% if is_incremental() %}
  where date > (select max(date) from {{ this }})
{% endif %}
