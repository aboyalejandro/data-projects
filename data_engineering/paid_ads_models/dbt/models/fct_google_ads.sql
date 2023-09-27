{{
    config
    (
        materialized='incremental',
        unique_key = ['id'],
        on_schema_change = 'sync_all_columns'
    )

}} 

SELECT dim_keyword.date,
       dim_campaign.campaign_id, 
       dim_adgroup.id as adgroup_id,
       dim_keyword.keyword_id,
       dim_campaign.campaign as campaign_name, 
       dim_adgroup.adgroup as adgroup_name,
       {{get_country_from_campaign('dim_campaign.campaign')}} as country,
       dim_keyword.keyword_text,
       dim_keyword.quality_score,
       sum(dim_keyword.clicks) as clicks,
       sum(dim_keyword.impressions) as impressions,
       sum(dim_keyword.conversions) as conversions,
       sum(dim_keyword.cost) as cost,
       {{ dbt_utils.generate_surrogate_key(['dim_keyword.date',
                                           'dim_campaign.campaign_id',
                                           'dim_adgroup.id',
                                           'dim_keyword.keyword_id'])}} as id,
                                           current_timestamp as date_inserter
from {{ref('dim_campaign')}} 
left join  {{ref('dim_adgroup')}} 
    on dim_campaign.campaign_id = dim_adgroup.campaign_id 
    and dim_campaign.date = dim_adgroup.date 
left join {{ref('dim_keyword')}} 
    on dim_adgroup.id = dim_keyword.ad_group_id 
    and dim_adgroup.date = dim_keyword.date   
{% if is_incremental() %}
    where dim_keyword.date > (select max(date) from {{ this }})
{% endif %}
group by all