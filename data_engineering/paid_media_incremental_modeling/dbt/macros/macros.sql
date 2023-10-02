{% macro get_country_from_campaign(campaign_name) %} -- typical ads platforms use case to get the targeted country for that campaign
    case 
		when lower({{ campaign_name }}) like '%_spain_%' then 'Spain'
		when lower({{ campaign_name }}) like '%_france_%'then 'France'
		when lower({{ campaign_name }}) like '%_brazil_%' then 'Brazil'
		when lower({{ campaign_name }}) like '%_portugal_%' then 'Portugal'
		when lower({{ campaign_name }}) like '%kingdom%' then 'United Kingdom'
		when lower({{ campaign_name }}) like '%_mexico_%' then 'Mexico'
		when lower({{ campaign_name }}) like '%states%' then 'United States'
		when lower({{ campaign_name }}) like '%_germany_%' then 'Germany'
		when lower({{ campaign_name }}) like '%_netherlands_%' then 'Netherlands'
		 else 'Other'
    end
{% endmacro %}