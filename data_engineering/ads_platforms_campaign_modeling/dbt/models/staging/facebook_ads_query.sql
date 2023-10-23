select date,
	   account, 
	   campaign_name as campaign,
	   campaign_id,
       {{get_country_from_campaign('campaign')}} as country,
	   currency, 
	   sum(impressions) as impressions,
	   sum(clicks) as clicks,
	   sum(spend) as cost,	
	   sum(conversions) as conversions
from {{ source('raw_mkt_historical_fb', 'country_per_facebook_campaign') }}
group by all