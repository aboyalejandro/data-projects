select date,
       'Facebook' as source,
       account,
       campaign,
       campaign_id,
       currency,
       country,
       cost,
       impressions,
       clicks,
       conversions
from {{ ref('facebook_ads_query') }}
union all
select date,
       'Linkedin'  as source,
       account,
       campaign,
       campaign_id,
       currency,
       country,
       cost,
       impressions,
       clicks,
       conversions
from {{ ref('linkedin_ads_query') }}
union all 
select date,
       'Google Search'                      as source, 
       account,
       campaign,
       campaign_id,
       currency,
       country,
       cost,
       impressions,
       clicks,
       conversions
from {{ ref('google_ads_query') }} 
union all
select date,
       'Tiktok'  as source,
       account,
       campaign,
       campaign_id,
       currency,
       country,
       cost,
       impressions,
       clicks,
       conversions
from {{ref('tiktok_ads_query')}} et
union all
select date,
       'Bing'  as source,
       account,
       campaign,
       campaign_id,
       currency,
       country,
       cost,
       impressions,
       clicks,
       conversions
from {{ ref('bing_ads_query') }}