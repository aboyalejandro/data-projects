{{
    config
    (
        materialized='incremental',
        unique_key = ['id'],
        on_schema_change = 'sync_all_columns'
    )

}} 

select date, 
       country, 
       channel_grouping, 
       device_category,
       oncat('https://',hostname,landing_page_path) as landing_page,
       source_medium, 
       campaign, 
       users::int as users, 
       sessions::int as sessions,
       {{ dbt_utils.generate_surrogate_key(['date', 'country','channel_grouping','device_category','landing_page','source_medium','campaign']) }} as id,
       current_timestamp as date_inserted
from  {{ source('google_analytics', 'landing_page_conversion') }}
where  date < '2023-07-01' 
{% if is_incremental() %}
  and date > (select max(date) from {{ this }})
{% endif %}