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
    "location_country_name__dimension_value_grouping_1f54fe131b2446f284810167718d8bfa" as country,
    "device_type" as device_category,
    "source_medium" as source_medium, 
    "campaign_name" as campaign, 
    "custom_channel_grouping__67d495d6900145f5a2bddf501f2c30d7" as channel_grouping,
    "visitors"::int as users,
    "sessions"::int as sessions,
    "goal_conversions__b6b9173b-47a1-446a-8ea6-1bb96249eb37"::int as leads,
     {{ dbt_utils.generate_surrogate_key(['date', 'country','channel_grouping','device_category','landing_page','source_medium','campaign']) }} as id,
    current_timestamp as date_inserted
from  {{ source('raw_mkt_piwik', 'landing_page_conversion') }}
{% if is_incremental() %}
  where date > (select max(date) from {{ this }})
{% endif %}
