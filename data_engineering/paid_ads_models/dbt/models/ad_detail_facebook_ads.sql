select facebook_ads_adgroups_query.date,
       facebook_ads_query.campaign_id, 
       facebook_ads_adgroups_query.adgroup_id,
       fct_ad_facebook.ad_id,
       country,
       currency,
       campaign,
       facebook_ads_adgroups_query.adgroup,
       fct_ad_facebook.ad_name,
       fct_ad_facebook.ad_type,
       fct_ad_facebook.asset_url, 
       fct_ad_facebook.ad_text,
       sum(fct_ad_facebook.spend) as cost,
       sum(fct_ad_facebook.impressions)as impressions,
       zeroifnull(sum(fct_ad_facebook.clicks)) as clicks,
       zeroifnull(sum(fct_ad_facebook.conversions)) as conversions
from {{ref('facebook_ads_query')}}
left join {{ref('facebook_ads_adgroups_query')}}
    on facebook_ads_query.campaign_id = facebook_ads_adgroups_query.campaign_id
    and facebook_ads_query.date = facebook_ads_adgroups_query.date
left join {{ref('fct_ad_facebook')}}
    on facebook_ads_adgroups_query.adgroup_id = fct_ad_facebook.adgroup_id
    and facebook_ads_adgroups_query.date = fct_ad_facebook.date 
group by all
having sum(fct_ad_facebook.spend) > 1
order by date desc