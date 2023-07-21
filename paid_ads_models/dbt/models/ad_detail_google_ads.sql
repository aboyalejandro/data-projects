SELECT dim_keyword_google.date,
       gads_query.campaign_id, 
       gads_adgroups_query.id as adgroup_id,
       dim_keyword_google.keyword_id,
       gads_query.account, 
       gads_query.campaign as campaign_name, 
       gads_query.country, 
       gads_query.original_currency as currency,
       gads_adgroups_query.adgroup as adgroup_name,
       gads_adgroups_query.language,
       dim_keyword_google.keyword_text,
       gads_adgroups_query.ad_strength,
       dim_keyword_google.HISTORICAL_CREATIVE_QUALITY_SCORE,
       dim_keyword_google.HISTORICAL_LANDING_PAGE_QUALITY_SCORE,
       dim_keyword_google.HISTORICAL_QUALITY_SCORE,
       sum(dim_keyword_google.clicks) as clicks,
       sum(dim_keyword_google.impressions) as impressions,
       sum(dim_keyword_google.conversions) as conversions,
       sum(dim_keyword_google.cost) as cost
from {{ref('gads_query')}} 
left join  {{ref('gads_adgroups_query')}} 
    on gads_query.campaign_id = gads_adgroups_query.campaign_id 
    and gads_query.date = gads_adgroups_query.date 
left join {{ref('dim_keyword_google')}} 
    on gads_adgroups_query.id = dim_keyword_google.ad_group_id 
    and gads_adgroups_query.date = dim_keyword_google.date   
group by all