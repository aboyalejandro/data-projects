with campaign_history as (select campaign_id, 
								 campaign_name,
								 advertiser_id
                          from raw_mkt_historical_tik.campaign_history 
                          qualify row_number() over (partition by campaign_id order by updated_at desc) = 1)


select  campaign_stats.stat_time_day::date as date,
		account.name as account, 
	    campaign.campaign_name as campaign,
	    campaign.campaign_id as campaign_id, 
		{{get_country_from_campaign('campaign')}} as country,
		original_currency as currency,
	    sum(campaign_stats.spend) as cost_original_currency, 
	    sum(campaign_stats.impressions) as impressions, 
	    sum(campaign_stats.clicks) as clicks,
	    sum(campaign_stats.spend) as cost,
		sum(campaign_stats.conversions) as conversions,
from {{ source('raw_mkt_historical_tik', 'advertiser') }} as account 
left join campaign_history as campaign 
	on account.id = campaign.advertiser_id 
left join {{ source('raw_mkt_historical_tik', 'campaign_report_daily') }} as campaign_stats 
	on campaign.campaign_id = campaign_stats.campaign_id 
group by all