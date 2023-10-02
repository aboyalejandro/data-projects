
select date, 
	   account,
	   campaign_name as campaign, 
	   account_id as id, 
	   campaign_id, 
       {{get_country_from_campaign('campaign')}} as country,
       currency_code as currency,
	   sum(impressions) as impressions, 
	   sum(clicks) as clicks,
       sum(spend) as cost,
	   sum(conversions) as conversions
from {{ source('raw_mkt_historical_bing_ads', 'campaign_performance_daily_report') }} 
group by all