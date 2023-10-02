with campaign_stats as (select 	date,
		                      customer_id, 
		                      id as campaign_id,
							  name,
		                      sum(cost_micros*0.000001) as     cost, -- micros conversions
							  sum(impressions) as impressions,
		                      sum(clicks) as  clicks,
							  sum(conversions) as conversions
		               from {{ source('raw_mkt_historical_gads', 'campaign_stats_metrics') }}
		               group by all)

, account as (select id, 
                        descriptive_name, 
                        currency_code
			  from {{ source('raw_mkt_historical_gads', 'account_history') }}
              qualify row_number() over (partition by id order by updated_at desc) = 1)

select 		            campaign_stats.date as  date,
						account.descriptive_name as account,
						campaign_id,
		                campaign_stats.name as campaign,
                        {{get_country_from_campaign('campaign')}} as country,
						account.currency_code  as currency,
		                sum(impressions) as impressions,
		                sum(cost) as cost,
		                sum(clicks) as clicks,
						sum(conversions) as conversions
from account  
left join campaign_stats
    on account.id = campaign_stats.customer_id 
group by all