{{
    config
    (
        materialized='incremental',
        unique_key = ['id'],
        on_schema_change = 'sync_all_columns'
    )

}} 

select dim_ad.date,
       dim_campaign.campaign_id, 
       dim_adgroup.adgroup_id,
       dim_ad.ad_id,
       {{get_country_from_campaign('dim_campaign.campaign_name')}} as country,
       dim_campaign.campaign_name,
       dim_adgroup.adgroup,
       dim_ad.ad_name,
       dim_ad.ad_type,
       dim_ad.asset_url, 
       dim_ad.ad_text,
       sum(dim_ad.spend) as cost,
       sum(dim_ad.impressions)as impressions,
       zeroifnull(sum(dim_ad.clicks)) as clicks,
       zeroifnull(sum(dim_ad.conversions)) as conversions,
       {{ dbt_utils.generate_surrogate_key(['dim_keyword.date',
                                           'dim_campaign.campaign_id',
                                           'dim_adgroup.id',
                                           'dim_keyword.keyword_id'])}} as id,
        current_timestamp as date_inserted
from {{ref('dim_campaign')}}
left join {{ref('dim_adgroup')}}
    on dim_campaign.campaign_id = dim_adgroup.campaign_id
    and dim_campaign.date = dim_adgroup.date
left join {{ref('dim_ad')}}
    on dim_adgroup.adgroup_id = dim_ad.adgroup_id
    and dim_adgroup.date = dim_ad.date 
{% if is_incremental() %}
    where dim_ad.date > (select max(date) from {{ this }})
{% endif %}
group by all