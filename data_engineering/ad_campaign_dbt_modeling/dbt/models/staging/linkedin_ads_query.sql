with campaign as (select id, name, account_id
                  from {{ source('raw_mkt_historical_li', 'campaign_history') }}
                  qualify row_number() over (partition by id order by last_modified_time desc) = 1)

, account as (select id, name, currency
              from {{ source('raw_mkt_historical_li', 'account_history') }} 
              qualify row_number() over (partition by id order by last_modified_time desc) = 1)

, camp_stats as (select day::date as date, 
       campaign_id, 
       clicks, 
       impressions, 
       conversions,
       cost_in_local_currency as cost,
from {{ source('raw_mkt_historical_li', 'ad_analytics_by_campaign') }} )


select camp_stats.date as date,
       account.name as account,
       camp_stats.campaign_id,
       campaign.name  as campaign,
	account.currency as currency,
       {{get_country_from_campaign('campaign')}} as country,
       sum(camp_stats.clicks) as clicks,
       sum(camp_stats.impressions) as impressions,
       sum(camp_stats.cost) as cost,
       sum(camp_stats.conversions) as conversions
from account 
    left join campaign on account.id = campaign.account_id 
     left join camp_stats on campaign.id = camp_stats.campaign_id 
group by all






