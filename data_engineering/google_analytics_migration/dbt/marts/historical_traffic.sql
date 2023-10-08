{{
    config
    (
        materialized='incremental',
        unique_key = ['id'],
        on_schema_change = 'sync_all_columns'
    )

}} 

SELECT 'Google Analytics' as source, 
       *exclude(id), -- avoid duplicated surrogate key
       {{ dbt_utils.generate_surrogate_key(['date', 'country','channel_grouping','device_category','landing_page','source_medium','campaign']) }} as id
FROM {{ref('google_analytics_raw')}} 
where date < '2023-07-01' -- google analytics deprecation
{% if is_incremental() %}
  and date > (select max(date) from {{ this }})
{% endif %}
union all
SELECT 'Piwik Pro' as source, 
        *exclude(id),  -- avoid duplicated surrogate key
        {{ dbt_utils.generate_surrogate_key(['date', 'country','channel_grouping','device_category','landing_page','source_medium','campaign']) }} as id
FROM {{ref('piwikpro_raw')}}  
where  date > '2023-06-30' -- follow google analytics deprecation
{% if is_incremental() %}
  and date > (select max(date) from {{ this }})
{% endif %}
